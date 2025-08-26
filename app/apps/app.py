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
DEFAULT_SVC_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
DEFAULT_SVC_JSON_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "")

# ===== 日本語フォント設定 =====
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

# ===== 必須列定義 =====
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

# ===== 列名正規化ルール =====
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s))
    return s.replace(" ", "").replace("　", "").lower()

ALIAS_COLS = {
    "店名": ["店名","お店名","店舗名","ショップ名","店舗"],
    "日付": ["日付","訪問日","来店日","日時"],
    "評価項目": ["評価項目","項目","質問","質問文"],
    "スコア": ["スコア","点数","評価","score","得点"],
    "コメント": ["コメント","自由記述","メモ","備考","自由回答"],
    "セクション": ["section","セクション","区分","カテゴリ","カテゴリー"]
}

NORMALIZE_RULES = {
    "タイムスタンプ": "タイムスタンプ",
    "訪問日": "日付", "お店名": "店名", "店名": "店名",
    "step0": "味フィルター（必要条件）", "step 0": "味フィルター（必要条件）",
    "味フィルター": "味フィルター（必要条件）",
    # 多様性
    "メニューの独自性": "多様性1_メニューの独自性",
    "内装の個性": "多様性2_内装の個性",
    "店主": "多様性3_店主・スタッフのキャラ", "スタッフ": "多様性3_店主・スタッフのキャラ",
    "サービス独自性": "多様性4_サービス独自性",
    "地域性": "多様性5_地域性の反映",
    "イベント": "多様性6_イベント/季節",
    "sns": "多様性7_SNSのユニークさ",
    "客層": "多様性8_客層の多様性",
    "提供方法": "多様性9_提供方法の特異性",
    "物語性": "多様性10_店の物語性",
    # 防衛
    "味の信頼感": "防衛1_味の信頼感（初訪）",
    "衛生": "防衛2_衛生/清潔感",
    "接客": "防衛3_接客態度",
    "価格の明確さ": "防衛4_価格の明確さ",
    "提供スピード": "防衛5_提供スピード",
    "支払い": "防衛6_支払いの安全性",
    "入店しやすさ": "防衛7_入店しやすさ",
    "初見客": "防衛8_初見客への対応",
    "口コミ": "防衛9_常連/口コミ",
    "リスク対応力": "防衛10_リスク対応力",
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

def find_col(df: pd.DataFrame, logical_name: str) -> str | None:
    cands = ALIAS_COLS.get(logical_name, [])
    cols_norm = { _norm(c): c for c in df.columns }
    for key in cands:
        k = _norm(key)
        for cn, orig in cols_norm.items():
            if k in cn:
                return orig
    return None

# ===== DataFrame前処理 =====
def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    keep = [c for c in df.columns if not str(c).startswith("Unnamed:")]
    return df.loc[:, keep]

def collapse_duplicate_columns(df: pd.DataFrame, agg: str = "mean") -> pd.DataFrame:
    if df.columns.has_duplicates:
        new_data = {}
        for name in df.columns.unique():
            block = df.loc[:, df.columns == name]
            if block.shape[1] == 1:
                new_data[name] = block.iloc[:, 0]
            else:
                block_num = block.apply(pd.to_numeric, errors="coerce")
                new_series = getattr(block_num, agg)(axis=1, skipna=True)
                new_data[name] = new_series
        df = pd.DataFrame(new_data)
    return df

def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_unnamed_columns(df)
    new_cols = [unicodedata.normalize("NFKC", str(c)).rstrip("：:").strip() for c in df.columns]
    df.columns = new_cols
    df = normalize_columns(df)
    df = collapse_duplicate_columns(df, agg="mean")
    if df.columns.duplicated().any():
        cols, seen = [], {}
        for c in df.columns:
            if c not in seen: seen[c] = 1; cols.append(c)
            else: seen[c]+=1; cols.append(f"{c}__dup{seen[c]}")
        df.columns = cols
    return df

# ===== secretsからサービスアカウントを取得 =====
def get_service_account_from_secrets() -> dict | None:
    try:
        if "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"])
        if "GOOGLE_SERVICE_ACCOUNT_JSON" in st.secrets:
            return json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"])
    except Exception:
        pass
    return None

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

# ===== UI =====
st.title("飲食店評価：主成分分析（PCA） & マトリクス")

with st.sidebar:
    st.header("データソース")
    source = st.radio("選択", ["Excelアップロード", "Googleスプレッドシート"], index=0, key="source_kind")

    uploaded = None
    creds_dict = None
    sheet_id_input = ""
    ws_name_input = ""

    if source == "Excelアップロード":
        uploaded = st.file_uploader("Excelファイル（.xlsx）を選択", type=["xlsx"], key="xlsx_uploader")
    else:
        sheet_id_input = st.text_input("Spreadsheet ID / URL", value=DEFAULT_SHEET_ID, key="sheet_id")
        ws_name_input  = st.text_input("Worksheet名（タブ名）", value=DEFAULT_WS_NAME, key="worksheet_name")

        svc_from_secrets = get_service_account_from_secrets()
        if svc_from_secrets:
            creds_dict = svc_from_secrets
            st.success("Service Account: st.secrets から自動読込")
            email = svc_from_secrets.get("client_email", "(no email)")
            st.caption(f"client_email: {email}")
        else:
            svc_default_text = DEFAULT_SVC_JSON or (Path(DEFAULT_SVC_JSON_PATH).read_text(encoding="utf-8")
                                if DEFAULT_SVC_JSON_PATH and Path(DEFAULT_SVC_JSON_PATH).exists() else "")
            svc_text = st.text_area("Service Account JSON（貼り付け）", value=svc_default_text, height=160, key="svc_json")
            if svc_text.strip():
                try:
                    creds_dict = json.loads(svc_text)
                    st.success("サービスアカウントJSONを読み込みました。")
                except Exception as e:
                    st.error(f"JSON解析に失敗: {e}")
