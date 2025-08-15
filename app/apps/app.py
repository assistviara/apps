# app.py — Streamlit: Googleフォーム→スプレッドシート→マトリクス可視化（完全版・店名ラベル付き）
import os, re, json, unicodedata
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib import font_manager, rcParams
from matplotlib.patches import Rectangle

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from pathlib import Path

# ========================= 日本語フォントの有効化（同梱フォント優先） =========================
FONT_DIR = Path(__file__).parent / "fonts"
JP_FONT = FONT_DIR / "NotoSansJP-Regular.ttf"

try:
    if JP_FONT.exists():
        font_manager.fontManager.addfont(str(JP_FONT))
        try:
            font_manager._rebuild()
        except Exception:
            pass
        jp_name = font_manager.FontProperties(fname=str(JP_FONT)).get_name()
        rcParams["font.family"] = "sans-serif"
        rcParams["font.sans-serif"] = [jp_name, "DejaVu Sans", "Arial", "Liberation Sans"]
    else:
        rcParams["font.family"] = "DejaVu Sans"
    rcParams["axes.unicode_minus"] = False
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
    s = unicodedata.normalize("NFKC", str(s))
    return s.replace(" ", "").replace("　", "").lower()

def extract_sheet_id(text: str) -> str:
    t = (text or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)/?", t)
    return m.group(1) if m else t

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
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
    svc = st.secrets.get("gcp", {})
    if "service_account_json" in svc:
        try:
            return json.loads(svc["service_account_json"])
        except Exception as e:
            st.error(f"Secretsの service_account_json が不正です: {e}")
            return None
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
    try:
        ws = sh.worksheet(worksheet)
    except WorksheetNotFound:
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
    return df

# ========================= 描画 =========================
def draw_plot(df: pd.DataFrame, show_all: bool, show_labels: bool, max_labels: int):
    fig, ax = plt.subplots(figsize=(9, 6), dpi=120)

    # 味OKの定義（増やしたければここに追加）
    ok_vals = {"yes","y","true","1","ok","○","はい","可"}
    mask = df["味フィルター（必要条件）"].astype(str).str.strip().str.lower().isin(ok_vals)

    plot_df = df.copy() if show_all else df[mask].copy()

    # 背景（象限を薄く塗る）
    ax.add_patch(Rectangle((0, 0), 50, 50, facecolor=(0,0,0,0.02), edgecolor="none"))
    ax.add_patch(Rectangle((MIDLINE, 0), 50-MIDLINE, 50, facecolor=(0,0,0,0.04), edgecolor="none"))
    ax.add_patch(Rectangle((0, MIDLINE), 50, 50-MIDLINE, facecolor=(0,0,0,0.04), edgecolor="none"))

    # 散布図
    ax.scatter(plot_df["多様性合計"], plot_df["防衛合計"], s=64, alpha=0.9, linewidths=0.6, edgecolors="white")

    # 交差線
    ax.axvline(MIDLINE, lw=1)
    ax.axhline(MIDLINE, lw=1)

    # 軸とタイトル
    ax.set_xlim(0, 50); ax.set_ylim(0, 50)
    ax.set_xlabel("多様性合計（1〜5×10＝10〜50）")
    ax.set_ylabel("ブランド防衛合計（1〜5×10＝10〜50）")
    ax.set_title("飲食店スコア・マトリクス（味OKのみ）" if not show_all else "飲食店スコア・マトリクス（全件）")

    # ラベル（店名）
    if show_labels and not plot_df.empty:
        # 点数が高い順に最大 max_labels 件だけ注釈して、重なりを少し回避する
        label_df = plot_df.sort_values(["多様性合計","防衛合計"], ascending=False).head(max_labels)
        for _, r in label_df.iterrows():
            ax.annotate(
                str(r["店名"]),
                (r["多様性合計"], r["防衛合計"]),
                xytext=(4, 4), textcoords="offset points", fontsize=9
            )
        if len(plot_df) > max_labels:
            st.caption(f"※ ラベルは {max_labels} 件まで表示（全{len(plot_df)}件中）。サイドバーで変更できます。")

    st.caption(f"プロット数: {len(plot_df)} / 全体: {len(df)}（{'全件' if show_all else '味OKのみ'}）")
    st.pyplot(fig, clear_figure=True)

    # 下に「店名と座標」の表を出す（場所が分かるように）
    shown = plot_df.loc[:, ["店名","多様性合計","防衛合計"]].sort_values(["防衛合計","多様性合計"], ascending=False)
    st.dataframe(shown, use_container_width=True)

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

    # 表示オプション
    show_all = st.checkbox("味フィルター無視（全てプロット）", value=False)
    show_labels = st.checkbox("店名ラベルを表示", value=True)
    max_labels = st.slider("ラベル最大件数", min_value=0, max_value=200, value=50, step=5)

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
        draw_plot(df, show_all=show_all, show_labels=show_labels, max_labels=max_labels)

        # ダウンロード（整形済みデータを落とせるように）
        out = df.copy()
        out["味OK"] = out["味フィルター（必要条件）"].astype(str)
        csv = out.to_csv(index=False).encode("utf-8-sig")
        st.download_button("CSVをダウンロード", data=csv, file_name="scores_cleaned.csv", mime="text/csv")

    except SpreadsheetNotFound as e:
        st.error("スプレッドシートにアクセスできません（404）。IDが誤っているか、サービスアカウントに共有が付いていません。"
                 " → 対策: 該当シートをサイドバーに表示された SA のメールアドレスに Viewer 共有してください。")
        st.exception(e)
    except WorksheetNotFound as e:
        st.error(f"指定のワークシート（タブ）が見つかりません: {ws_name}")
        try:
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
st.write("💡 デフォルトは *Yes* 系の味フィルターのみをプロット。左のチェックで全件表示、ラベル件数も調整できます。境界は 30 点（10項目×3）。")
