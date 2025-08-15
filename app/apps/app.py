# app.py — Streamlit: Googleフォーム→スプレッドシート→マトリクス可視化（完全版）
import os, re, json, unicodedata
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib import font_manager, rcParams

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

# ========================= 日本語フォント =========================
MEIRYO_PATH = r"/usr/share/fonts/truetype/msttcorefonts/Meiryo.ttf"  # Linuxの例
try:
    if os.path.exists(MEIRYO_PATH):
        font_manager.fontManager.addfont(MEIRYO_PATH)
        rcParams["font.family"] = font_manager.FontProperties(fname=MEIRYO_PATH).get_name()
    else:
        rcParams["font.family"] = "DejaVu Sans"
except Exception:
    rcParams["font.family"] = "DejaVu Sans"
rcParams["axes.unicode_minus"] = False

# ========================= 評価定義 =========================
DIVERSITY_COLS = [
    "多様性1_メニューの独自性","多様性2_内装の個性","多様性3_店主・スタッフのキャラ","多様性4_サービス独自性",
    "多様性5_地域性の反映","多様性6_イベント/季節","多様性7_SNSのユニークさ","多様性8_客層の多様性",
    "多様性9_提供方法の特異性","多様性10_店の物語性"
]
BRAND_COLS = [
    "防衛1_味の信頼感（初訪）","防衛2_衛生/清潔感","防衛3_接客態度","防衛4_価格の明確さ",
    "防衛5_提供スピード","防衛6_支払いの安全性","防衛7_入店しやすさ","防衛8_初見客への対応",
    "防衛9_常連/口コミ","防衛10_リスク対応力"
]
BASE_COLS = ["日付","店名","味フィルター（必要条件）"]
REQUIRED_COLS = BASE_COLS + DIVERSITY_COLS + BRAND_COLS

# 見出しゆらぎ吸収（エイリアス集）
NORMALIZE_RULES = {
    # ベース
    "タイムスタンプ": "タイムスタンプ",
    "訪問日": "日付", "お店名": "店名", "店名": "店名",
    "step0": "味フィルター（必要条件）", "step 0": "味フィルター（必要条件）",
    "味フィルター": "味フィルター（必要条件）",
    # 多様性
    "メニューの独自性": "多様性1_メニューの独自性",
    "内装の個性": "多様性2_内装の個性",
    "店主": "多様性3_店主・スタッフのキャラ", "スタッフ": "多様性3_店主・スタッフのキャラ",
    "サービス独自性": "多様性4_サービス独自性",
    "サービスの独自性": "多様性4_サービス独自性",
    "独自サービス": "多様性4_サービス独自性",
    "地域性": "多様性5_地域性の反映",
    "イベント": "多様性6_イベント/季節", "季節": "多様性6_イベント/季節",
    "sns": "多様性7_SNSのユニークさ", "ＳＮＳ": "多様性7_SNSのユニークさ",
    "客層": "多様性8_客層の多様性",
    "提供方法": "多様性9_提供方法の特異性",
    "物語性": "多様性10_店の物語性",
    # 防衛
    "味の信頼感": "防衛1_味の信頼感（初訪）",
    "衛生": "防衛2_衛生/清潔感", "清潔": "防衛2_衛生/清潔感",
    "接客": "防衛3_接客態度",
    "価格の明確さ": "防衛4_価格の明確さ",
    "提供スピード": "防衛5_提供スピード",
    "支払い": "防衛6_支払いの安全性",
    "入店しやすさ": "防衛7_入店しやすさ",
    "入店のしやすさ": "防衛7_入店しやすさ",
    "入りやすさ": "防衛7_入店しやすさ",
    "入り易さ": "防衛7_入店しやすさ",
    "入店し易さ": "防衛7_入店しやすさ",
    "初見客": "防衛8_初見客への対応",
    "口コミ": "防衛9_常連/口コミ",
    "リスク対応力": "防衛10_リスク対応力",
}

MIDLINE = 30
MIN_SCORE, MAX_SCORE = 1, 5

# ========================= 正規化ユーティリティ =========================
def _norm(s: str) -> str:
    """NFKC正規化→全空白除去→小文字化（マッチ用）"""
    s = unicodedata.normalize("NFKC", str(s))
    return s.replace(" ", "").replace("　", "").lower()

def extract_sheet_id(text: str) -> str:
    """URLでもIDでもOK。/d/…/ から抽出。/edit が無くても対応。"""
    t = (text or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)/?", t)
    return m.group(1) if m else t

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """列名をルールベースで正規化（包含判定は正規化後で頑丈に）"""
    new_cols = []
    for c in df.columns:
        cc_norm = _norm(c)
        mapped = None
        for key, dest in NORMALIZE_RULES.items():
            if _norm(key) in cc_norm:
                mapped = dest
                break
        new_cols.append(mapped or c)
    df.columns = new_cols
    return df

# ========================= 認証情報 =========================
def build_creds_from_secrets_or_text() -> dict | None:
    """
    Secretsを2方式対応:
      A) st.secrets['gcp']['service_account_json'] にJSON文字列
      B) st.secrets['gcp'] に個別キー（project_id 等）
    無ければ貼り付けUIを出す。
    """
    svc = st.secrets.get("gcp", {})
    # A: JSON丸ごと
    if "service_account_json" in svc:
        try:
            return json.loads(svc["service_account_json"])
        except Exception as e:
            st.error(f"Secretsの service_account_json が不正です: {e}")
            return None
    # B: 個別キー
    required = {"type","project_id","private_key_id","private_key","client_email","client_id"}
    if required.issubset(set(svc.keys())):
        return {
            "type": "service_account",
            "project_id": svc["project_id"],
            "private_key_id": svc["private_key_id"],
            "private_key": svc["private_key"],
            "client_email": svc["client_email"],
            "client_id": svc["client_id"],
            "auth_uri": svc.get("auth_uri","https://accounts.google.com/o/oauth2/auth"),
            "token_uri": svc.get("token_uri","https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": svc.get("auth_provider_x509_cert_url","https://www.googleapis.com/oauth2/v1/certs"),
            "client_x509_cert_url": svc.get("client_x509_cert_url",""),
            "universe_domain": svc.get("universe_domain","googleapis.com"),
        }
    # C: 未設定 → 貼り付け救済
    with st.expander("🔐 サービスアカウントJSONをここに貼り付け（Secretsが未設定のとき用）", expanded=True):
        pasted = st.text_area("Paste JSON", height=180, label_visibility="collapsed")
        if pasted.strip():
            try:
                return json.loads(pasted)
            except Exception as e:
                st.error(f"JSON解析に失敗: {e}")
    return None

# ========================= データ取得 =========================
@st.cache_data(show_spinner=False)
def load_sheet(creds_dict: dict, sheet_id: str, worksheet: str) -> pd.DataFrame:
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    # worksheet名が曖昧な可能性に少し寄り添う
    try:
        ws = sh.worksheet(worksheet)
    except WorksheetNotFound:
        # 類似候補を探す
        titles = [w.title for w in sh.worksheets()]
        norm_ws = _norm(worksheet)
        cand = [t for t in titles if _norm(t) == norm_ws or _norm(worksheet) in _norm(t)]
        if cand:
            ws = sh.worksheet(cand[0])
        else:
            raise
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    df = df.dropna(how="all")
    return normalize_columns(df)

# ========================= 前処理 =========================
def coerce_scores(df: pd.DataFrame) -> pd.DataFrame:
    for c in DIVERSITY_COLS + BRAND_COLS:
        s = pd.to_numeric(df[c], errors="coerce").clip(MIN_SCORE, MAX_SCORE).fillna(MIN_SCORE).astype(int)
        df[c] = s
    return df

def deduplicate(df: pd.DataFrame, keys=("店名","日付"), ts_col="タイムスタンプ"):
    if all(k in df.columns for k in keys):
        if ts_col in df.columns:
            d = df.copy()
            d["_ts"] = pd.to_datetime(d[ts_col], errors="coerce")
            d = d.sort_values("_ts").drop_duplicates(subset=list(keys), keep="last").drop(columns=["_ts"])
            return d
        return df.drop_duplicates(subset=list(keys), keep="last")
    return df

def compute_totals(df: pd.DataFrame) -> pd.DataFrame:
    df["多様性合計"] = df[DIVERSITY_COLS].sum(axis=1)
    df["防衛合計"] = df[BRAND_COLS].sum(axis=1)
    mask = df["味フィルター（必要条件）"].astype(str).str.strip().str.lower().isin(["yes","y","true","1","ok","○"])
    df["_plot_x"] = df["多様性合計"].where(mask)
    df["_plot_y"] = df["防衛合計"].where(mask)
    return df

# ========================= 描画 =========================
def draw_plot(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(8.8, 5.6), dpi=120)
    plot_df = df.dropna(subset=["_plot_x","_plot_y"])
    ax.scatter(plot_df["_plot_x"], plot_df["_plot_y"], s=42)
    ax.axvline(MIDLINE, lw=1); ax.axhline(MIDLINE, lw=1)
    ax.set_xlim(0,50); ax.set_ylim(0,50)
    ax.set_xlabel("多様性合計（1〜5×10＝10〜50）")
    ax.set_ylabel("ブランド防衛合計（1〜5×10＝10〜50）")
    ax.set_title("飲食店スコア・マトリクス（味OKのみプロット）")
    st.pyplot(fig, clear_figure=True)

# ========================= UI =========================
st.set_page_config(page_title="飲食店スコア・マトリクス", layout="wide")
st.title("飲食店スコア・マトリクス（Googleフォーム → スプレッドシート）")

# Secrets 既定値（あれば利用）
default_sheet_id = st.secrets.get("gcp", {}).get("sheet_id", "")
default_ws = st.secrets.get("gcp", {}).get("worksheet", "Form Responses")

with st.sidebar:
    st.header("設定")
    sheet_id_input = st.text_input("Spreadsheet ID / URL", value=default_sheet_id, placeholder="ID または URL を入力")
    ws_name = st.text_input("Worksheet名（タブ名）", value=default_ws, placeholder="例：Form Responses / フォームの回答 1")
    dedup_keys = st.text_input("重複除去キー（カンマ区切り）", value="店名,日付")
    ts_col = st.text_input("タイムスタンプ列（任意）", value="タイムスタンプ")

    # 共有漏れ・ID取り違えの即時確認用
    creds_preview = build_creds_from_secrets_or_text()
    sid_preview = extract_sheet_id(sheet_id_input or default_sheet_id)
    if creds_preview:
        st.caption(f"🔑 SA: {creds_preview.get('client_email','(unknown)')}")
    if sid_preview:
        st.caption(f"📄 Sheet ID: {sid_preview}")

    go = st.button("読み込み＆プロット", type="primary")

if go:
    creds = creds_preview or build_creds_from_secrets_or_text()
    if not creds:
        st.stop()

    try:
        sid = extract_sheet_id(sheet_id_input or default_sheet_id)
        if not sid:
            st.error("Spreadsheet ID / URL を入力してください。"); st.stop()

        df = load_sheet(creds, sid, ws_name or default_ws)

        # 必要列チェック（正規化後）
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            st.error(f"必要列が不足しています: {missing}")
            st.caption("💡 列名の表記ゆらぎが原因の場合は、1行目の見出しを正確に合わせるか、NORMALIZE_RULES にエイリアスを追加してください。")
            st.dataframe(df.head())
            st.stop()

        df = coerce_scores(df)

        keys = tuple([k.strip() for k in (dedup_keys or "店名,日付").split(",") if k.strip()])
        before = len(df)
        df = deduplicate(df, keys=keys if keys else ("店名","日付"), ts_col=ts_col or "タイムスタンプ")
        after = len(df)
        st.caption(f"重複除去: {before - after}件（キー: {keys if keys else ('店名','日付')} / タイムスタンプ最新を採用）")

        df = compute_totals(df)
        draw_plot(df)

        st.dataframe(
            df[BASE_COLS + ["多様性合計","防衛合計"]].sort_values(["日付","店名"]),
            use_container_width=True
        )
        # ダウンロード
        csv = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("CSVをダウンロード", data=csv, file_name="scores_cleaned.csv", mime="text/csv")

    except SpreadsheetNotFound as e:
        st.error("スプレッドシートにアクセスできません（404）。IDが誤っているか、サービスアカウントに共有が付いていません。"
                 " → 対策: 該当シートをサイドバーに表示された SA のメールアドレスに Viewer 共有してください。")
        st.exception(e)
    except WorksheetNotFound as e:
        st.error(f"指定のワークシート（タブ）が見つかりません: {ws_name}")
        try:
            # 候補を提示
            creds2 = Credentials.from_service_account_info(
                creds, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
            )
            gc2 = gspread.authorize(creds2)
            sh2 = gc2.open_by_key(sid)
            titles = [w.title for w in sh2.worksheets()]
            st.info(f"利用可能なタブ: {titles}")
        except Exception:
            pass
        st.exception(e)
    except Exception as e:
        st.exception(e)

st.markdown("---")
st.write("💡 *Yes* 系の味フィルターのみをプロット。境界は 30 点（10項目×3）。見出しのゆらぎは自動正規化＋エイリアスで吸収します。")
