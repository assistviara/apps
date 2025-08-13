import os, re, json
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib import font_manager, rcParams

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe

# ========= 日本語フォント（メイリオ優先、なければデフォルト） =========
MEIRYO_PATH = r"/usr/share/fonts/truetype/msttcorefonts/Meiryo.ttf"  # Linux環境用の例
try:
    if os.path.exists(MEIRYO_PATH):
        font_manager.fontManager.addfont(MEIRYO_PATH)
        rcParams["font.family"] = font_manager.FontProperties(fname=MEIRYO_PATH).get_name()
    else:
        rcParams["font.family"] = "DejaVu Sans"
except Exception:
    rcParams["font.family"] = "DejaVu Sans"
rcParams["axes.unicode_minus"] = False

# ========= 評価項目（正規名） =========
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

# 見出しのゆらぎを吸収（部分一致）
NORMALIZE_RULES = {
    "訪問日": "日付", "お店名": "店名", "Step 0": "味フィルター（必要条件）",
    "メニューの独自性": "多様性1_メニューの独自性", "内装の個性": "多様性2_内装の個性",
    "店主": "多様性3_店主・スタッフのキャラ", "サービス独自性": "多様性4_サービス独自性",
    "地域性": "多様性5_地域性の反映", "イベント": "多様性6_イベント/季節", "季節": "多様性6_イベント/季節",
    "SNS": "多様性7_SNSのユニークさ", "客層": "多様性8_客層の多様性",
    "提供方法": "多様性9_提供方法の特異性", "物語性": "多様性10_店の物語性",
    "味の信頼感": "防衛1_味の信頼感（初訪）", "衛生": "防衛2_衛生/清潔感", "清潔": "防衛2_衛生/清潔感",
    "接客": "防衛3_接客態度", "価格の明確さ": "防衛4_価格の明確さ", "提供スピード": "防衛5_提供スピード",
    "支払い": "防衛6_支払いの安全性", "入店しやすさ": "防衛7_入店しやすさ",
    "初見客": "防衛8_初見客への対応", "口コミ": "防衛9_常連/口コミ", "リスク対応力": "防衛10_リスク対応力",
}

MIDLINE = 30
MIN_SCORE, MAX_SCORE = 1, 5

def extract_sheet_id(text: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9-_]+)/", text.strip())
    return m.group(1) if m else text.strip()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    for c in df.columns:
        cc, mapped = str(c), None
        for key, dest in NORMALIZE_RULES.items():
            if key in cc:
                mapped = dest; break
        new_cols.append(mapped or cc)
    df.columns = new_cols
    return df

@st.cache_data(show_spinner=False)
def load_sheet(creds_dict: dict, sheet_id: str, worksheet: str) -> pd.DataFrame:
    creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    df = df.dropna(how="all")
    return normalize_columns(df)

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
    mask = df["味フィルター（必要条件）"].astype(str).str.strip().str.lower().eq("yes")
    df["_plot_x"] = df["多様性合計"].where(mask)
    df["_plot_y"] = df["防衛合計"].where(mask)
    return df

def draw_plot(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(8.8, 5.6), dpi=120)
    plot_df = df.dropna(subset=["_plot_x","_plot_y"])
    ax.scatter(plot_df["_plot_x"], plot_df["_plot_y"], s=42)
    ax.axvline(MIDLINE, color="grey", lw=1); ax.axhline(MIDLINE, color="grey", lw=1)
    ax.set_xlim(0,50); ax.set_ylim(0,50)
    ax.set_xlabel("多様性合計（1〜5×10＝10〜50）")
    ax.set_ylabel("ブランド防衛合計（1〜5×10＝10〜50）")
    ax.set_title("飲食店スコア・マトリクス（味OKのみプロット）")
    st.pyplot(fig, clear_figure=True)

# ===================== UI =====================
st.set_page_config(page_title="飲食店スコア・マトリクス", layout="wide")
st.title("飲食店スコア・マトリクス（Googleフォーム → スプレッドシート）")

# Secrets（Streamlit Cloud の「Settings > Secrets」へ設定）
# 例:
# [gcp]
# service_account_json = {...}  # サービスアカウントのJSON一式
# sheet_id = "xxxxxxxxxxxxxxxxxxxx"
# worksheet = "Form Responses"
svc_json = st.secrets["gcp"]["service_account_json"]
default_sheet_id = st.secrets["gcp"].get("sheet_id", "")
default_ws = st.secrets["gcp"].get("worksheet", "Form Responses")

col1, col2 = st.columns(2)
with col1:
    sheet_id_input = st.text_input("Spreadsheet ID / URL", value=default_sheet_id, placeholder="IDまたはURLを入力")
with col2:
    ws_name = st.text_input("Worksheet名（タブ名）", value=default_ws, placeholder="例：Form Responses / フォームの回答 1")

if st.button("読み込み＆プロット", type="primary"):
    try:
        sid = extract_sheet_id(sheet_id_input)
        df = load_sheet(json.loads(svc_json), sid, ws_name)
        # 必要列チェック
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            st.error(f"必要列が不足しています: {missing}")
        else:
            df = coerce_scores(df)
            before = len(df)
            df = deduplicate(df, keys=("店名","日付"), ts_col="タイムスタンプ")
            after = len(df)
            st.caption(f"重複除去: {before - after}件（キー: 店名×日付、タイムスタンプ最新を採用）")
            df = compute_totals(df)

            draw_plot(df)
            st.dataframe(df[BASE_COLS + ["多様性合計","防衛合計"]].sort_values(["日付","店名"]))

            csv = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button("CSVをダウンロード", data=csv, file_name="scores_cleaned.csv", mime="text/csv")
    except Exception as e:
        st.exception(e)

st.markdown("---")
st.write("💡 メモ：このアプリは *Yes* の味フィルターのみをプロットし、境界は 30 点（10項目×3）です。見出しが微妙に違っても自動で寄せます。")
