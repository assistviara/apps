# app.py — PCA対応版：Excel/Sheets入力 → 前処理 → 主成分分析（SVD） → 可視化
# Author: たけしゃん用（2025-08）

import os, re, json, unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib import font_manager, rcParams
from matplotlib.patches import Rectangle
from dotenv import load_dotenv

# --------- 最初の st.* は set_page_config！ ----------
st.set_page_config(
    page_title="飲食店評価：PCA & マトリクス",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ===== .env 読み込み =====
load_dotenv()
DEFAULT_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
DEFAULT_WS_NAME  = os.getenv("GSHEET_WORKSHEET", "Form Responses")

# ===== secrets.toml を優先 =====
try:
    if "gcp" in st.secrets:
        DEFAULT_SHEET_ID = st.secrets["gcp"].get("sheet_id", DEFAULT_SHEET_ID or "")
        DEFAULT_WS_NAME  = st.secrets["gcp"].get("worksheet", DEFAULT_WS_NAME or "Form Responses")
except Exception:
    pass

# ===== Service Account 取得 =====
def get_service_account_from_secrets() -> dict | None:
    try:
        if "gcp" in st.secrets:
            g = st.secrets["gcp"]
            required = [
                "type","project_id","private_key_id","private_key",
                "client_email","client_id","token_uri"
            ]
            if all(k in g for k in required):
                return {
                    "type": g["type"],
                    "project_id": g["project_id"],
                    "private_key_id": g["private_key_id"],
                    "private_key": g["private_key"],  # 改行そのまま
                    "client_email": g["client_email"],
                    "client_id": g["client_id"],
                    "auth_uri": g.get("auth_uri","https://accounts.google.com/o/oauth2/auth"),
                    "token_uri": g.get("token_uri","https://oauth2.googleapis.com/token"),
                    "auth_provider_x509_cert_url": g.get("auth_provider_x509_cert_url","https://www.googleapis.com/oauth2/v1/certs"),
                    "client_x509_cert_url": g.get("client_x509_cert_url",""),
                    "universe_domain": g.get("universe_domain","googleapis.com"),
                }
    except Exception:
        pass
    return None

# ===== 日本語フォント（任意） =====
FONT_DIR = Path(__file__).parent / "fonts"
JP_FONT = FONT_DIR / "NotoSansJP-Regular.ttf"
try:
    if JP_FONT.exists():
        font_manager.fontManager.addfont(str(JP_FONT))
        try: font_manager._rebuild()
        except Exception: pass
        jp_name = font_manager.FontProperties(fname=str(JP_FONT)).get_name()
        rcParams["font.family"] = "sans-serif"
        rcParams["font.sans-serif"] = [jp_name, "DejaVu Sans", "Arial", "Liberation Sans"]
    else:
        rcParams["font.family"] = "DejaVu Sans"
    rcParams["axes.unicode_minus"] = False
except Exception:
    rcParams["font.family"] = "DejaVu Sans"
    rcParams["axes.unicode_minus"] = False

# ===== 旧マトリクス用列 =====
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
MIDLINE = 30

# ===== 正規化ルール =====
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s))
    return s.replace(" ", "").replace("　", "").lower()

NORMALIZE_RULES = {
    "内装の個性": "多様性2_内装の個性",
    "サービス独自性": "多様性4_サービス独自性",
    "客層": "多様性8_客層の多様性",
    "価格の明確さ": "防衛4_価格の明確さ",
    "支払い": "防衛6_支払いの安全性",
    "入店しやすさ": "防衛7_入店しやすさ",
    "口コミ": "防衛9_常連/口コミ",
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    for c in df.columns:
        cn = _norm(c)
        mapped = None
        for key, dest in NORMALIZE_RULES.items():
            if _norm(key) == cn:
                mapped = dest
                break
        new_cols.append(mapped or c)
    df.columns = new_cols
    return df

def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    keep = [c for c in df.columns if not str(c).startswith("Unnamed:")]
    return df.loc[:, keep]

def collapse_duplicate_columns(df: pd.DataFrame, agg="mean") -> pd.DataFrame:
    if df.columns.has_duplicates:
        new_data = {}
        for name in df.columns.unique():
            block = df.loc[:, df.columns == name]
            if block.shape[1] == 1:
                new_data[name] = block.iloc[:, 0]
            else:
                block_num = block.apply(pd.to_numeric, errors="coerce")
                new_data[name] = block_num.mean(axis=1, skipna=True)
        df = pd.DataFrame(new_data)
    return df

def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_unnamed_columns(df)
    df = normalize_columns(df)
    df = collapse_duplicate_columns(df, agg="mean")
    return df

# ===== Google Sheets 読み込み =====
def extract_sheet_id(text: str) -> str:
    t = (text or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)/?", t)
    return m.group(1) if m else t

def read_from_sheets(creds_dict, sheet_id, worksheet) -> pd.DataFrame:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread_dataframe import get_as_dataframe
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    gc = gspread.authorize(creds)
    sid = extract_sheet_id(sheet_id)
    sh = gc.open_by_key(sid)
    try:
        ws = sh.worksheet(worksheet)
    except Exception:
        ws = sh.worksheets()[0]
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0).dropna(how="all")
    df = sanitize_columns(df)
    return df

# ===== PCA =====
def pca_svd(df_items: pd.DataFrame):
    X = df_items.copy()
    for c in X.columns:
        col = pd.to_numeric(X[c], errors="coerce")
        X[c] = col.fillna(col.mean())
    X = X.loc[:, X.var() > 1e-12]
    mu = X.mean(axis=0)
    sd = X.std(axis=0, ddof=1).replace(0, 1.0)
    Z = (X - mu) / sd
    U, S, VT = np.linalg.svd(Z, full_matrices=False)
    eigvals = (S**2) / (Z.shape[0]-1)
    ev_ratio = eigvals / eigvals.sum()
    scores = U * S
    loadings = VT.T
    scores_df = pd.DataFrame(scores, columns=[f"PC{i+1}" for i in range(scores.shape[1])])
    loadings_df = pd.DataFrame(loadings, index=X.columns, columns=[f"PC{i+1}" for i in range(loadings.shape[1])])
    return scores_df, loadings_df, eigvals, ev_ratio

# ===== UI =====
st.title("飲食店評価：主成分分析（PCA） & マトリクス")

with st.sidebar:
    source = st.radio("選択", ["Excelアップロード", "Googleスプレッドシート"], index=1)
    uploaded = None
    creds_dict = None
    sheet_id_input = ""
    ws_name_input = ""

    if source == "Googleスプレッドシート":
        sheet_id_input = st.text_input("Spreadsheet ID / URL", value=DEFAULT_SHEET_ID)
        ws_name_input  = st.text_input("Worksheet名（タブ名）", value=DEFAULT_WS_NAME)
        creds_dict = get_service_account_from_secrets()
        if creds_dict:
            st.success("Service Account: st.secrets[gcp] から自動読込")
        else:
            st.error("Service Account 情報が secrets.toml にありません")
    else:
        uploaded = st.file_uploader("Excelファイル（.xlsx）を選択", type=["xlsx"])

go = st.button("PCAを実行", type="primary")

if go:
    try:
        if source == "Googleスプレッドシート":
            df_raw = read_from_sheets(creds_dict, sheet_id_input, ws_name_input)
        else:
            df_raw = pd.read_excel(uploaded)
            df_raw = sanitize_columns(df_raw)

        st.dataframe(df_raw.head(), use_container_width=True)

        numeric_cols = [c for c in df_raw.columns if pd.api.types.is_numeric_dtype(df_raw[c])]
        df_items = df_raw[numeric_cols].copy()
        scores_df, loadings, ev, ev_ratio = pca_svd(df_items)

        st.subheader("PCA 結果")
        st.dataframe(scores_df)
    except Exception as e:
        st.exception(e)
