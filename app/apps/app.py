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

# ===== 旧マトリクス用列（参考タブ用） =====
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
MIN_SCORE, MAX_SCORE = 1, 5

# ===== 正規化/別名吸収 =====
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
    "サービス独自性": "多様性4_サービス独自性", "サービスの独自性": "多様性4_サービス独自性",
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
    "入店しやすさ": "防衛7_入店しやすさ", "入店のしやすさ": "防衛7_入店しやすさ",
    "初見客": "防衛8_初見客への対応",
    "口コミ": "防衛9_常連/口コミ",
    "リスク対応力": "防衛10_リスク対応力",
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """列名の完全一致のみで正規化。部分一致はしない"""
    new_cols = []
    for c in df.columns:
        cn = _norm(c)
        mapped = None
        for key, dest in NORMALIZE_RULES.items():
            if _norm(key) == cn:   # ★ ここを完全一致に
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

# ===== スコアの頑丈変換 =====
import re as _re
def _to_1to5(x):
    """セル単位で 1〜5 のスコアに正規化"""
    # SeriesやDataFrameが誤って渡ってきた場合に備えて
    if isinstance(x, (pd.Series, pd.DataFrame)):
        return x.applymap(_to_1to5) if isinstance(x, pd.DataFrame) else x.apply(_to_1to5)

    if pd.isna(x): 
        return np.nan
    s = str(x).strip()
    if s == "": 
        return np.nan
    s = unicodedata.normalize("NFKC", s)

    # Likertスケール対応
    likert_map = {
        "非常に低い":1, "とても低い":1, "低い":2, "やや低い":2,
        "ふつう":3, "普通":3, "やや高い":4, "高い":4, "非常に高い":5, "とても高い":5
    }
    if s in likert_map: 
        return float(likert_map[s])

    # 数字抽出
    import re
    m = re.search(r"([0-9]+)", s)
    if m:
        v = int(m.group(1))
        if 5 < v <= 100:  # 100点満点っぽいケース
            v = round(v/20)
        return float(max(1, min(5, v)))

    try:
        v = float(s)
        return float(max(1, min(5, v)))
    except:
        return np.nan


def coerce_1to5(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrame内のスコア列をすべて 1〜5 に変換"""
    for c in df.columns:
        if any(kw in str(c) for kw in ["コメント","自由記述","備考","メモ"]):
            continue
        if c in ("店名","日付","タイムスタンプ"):
            continue
        # ここで applymap / apply でセル単位処理を保証
        df[c] = df[c].apply(_to_1to5)
    return df

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
                if agg == "mean":
                    new_series = block_num.mean(axis=1, skipna=True)
                elif agg == "max":
                    new_series = block_num.max(axis=1, skipna=True)
                elif agg == "min":
                    new_series = block_num.min(axis=1, skipna=True)
                else:
                    new_series = block_num.mean(axis=1, skipna=True)
                new_data[name] = new_series
        df = pd.DataFrame(new_data)
    return df



# ===== 縦持ち→横持ち =====
def wide_from_long(df_long: pd.DataFrame) -> pd.DataFrame:
    col_store = find_col(df_long, "店名")
    col_date  = find_col(df_long, "日付")
    col_item  = find_col(df_long, "評価項目")
    col_score = find_col(df_long, "スコア")
    assert all([col_store, col_date, col_item, col_score]), "縦持ち→横持ち変換に必要な列が見つかりません"

    df_use = df_long[[col_store, col_date, col_item, col_score]].copy()
    # まず頑丈にスコアを数値化してからピボット
    df_use[col_score] = df_use[col_score].apply(_to_1to5)
    wide = df_use.pivot_table(index=[col_store, col_date], columns=col_item, values=col_score, aggfunc="mean")
    wide = wide.reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={col_store:"店名", col_date:"日付"})
    return coerce_1to5(wide)

# ===== データ読み込み =====
def read_from_excel(file) -> pd.DataFrame:
    # 1) 読み込み & ゴミ列（Unnamed）を先に落とす
    df = pd.read_excel(file).dropna(how="all")
    df = drop_unnamed_columns(df)

    # 2) 縦持ち？（Section/評価項目/スコア… があるなら縦）
    if find_col(df, "評価項目") and find_col(df, "スコア"):
        # 縦→横へ（内部で _to_1to5 → coerce_1to5 済み）
        df = wide_from_long(df)
    else:
        # 3) 横持ち（各項目が列）
        #    列名ゆらぎを正規化（完全一致ルール）→ 店名・日付の別名も拾う
        df = normalize_columns(df)
        alt = find_col(df, "店名")
        if alt and alt != "店名":
            df = df.rename(columns={alt: "店名"})
        alt = find_col(df, "日付")
        if alt and alt != "日付":
            df = df.rename(columns={alt: "日付"})
        # スコアを 1–5 に強制変換（自由記述はスキップ）
        df = coerce_1to5(df)

    # 4) 後処理：再度 Unnamed の混入を除去し、重複列名を集約
    df = drop_unnamed_columns(df)
    df = collapse_duplicate_columns(df, agg="mean")

    # 5) 返す
    return df


def extract_sheet_id(text: str) -> str:
    t = (text or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)/?", t)
    return m.group(1) if m else t

def read_from_sheets(creds_dict, sheet_id, worksheet) -> pd.DataFrame:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread_dataframe import get_as_dataframe

    # 1) 接続
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

    # 2) 読み込み & ゴミ列除去
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0).dropna(how="all")
    df = drop_unnamed_columns(df)

    # 3) 縦持ち？（Section/評価項目/スコア… があるなら縦）
    if find_col(df, "評価項目") and find_col(df, "スコア"):
        df = wide_from_long(df)  # 内部で _to_1to5 → coerce_1to5 済み
    else:
        # 4) 横持ち：列名を完全一致で正規化→ 店名/日付の別名吸収 → スコア強制変換
        df = normalize_columns(df)
        alt = find_col(df, "店名")
        if alt and alt != "店名":
            df = df.rename(columns={alt: "店名"})
        alt = find_col(df, "日付")
        if alt and alt != "日付":
            df = df.rename(columns={alt: "日付"})
        df = coerce_1to5(df)

    # 5) 後処理：再度 Unnamed 除去 & 重複列名の集約（平均）
    df = drop_unnamed_columns(df)
    df = collapse_duplicate_columns(df, agg="mean")

    # 6) 返却
    return df



# ===== PCA（SVDで安定化） =====
def pca_svd(df_items: pd.DataFrame):
    """
    行=店舗、列=評価項目（数値）
    SVDにより安定して主成分を算出
    戻り: scores_df, loadings_df, eigvals, ev_ratio
    """
    X = df_items.copy()

    # NaNを列平均で補完
    for c in X.columns:
        col = pd.to_numeric(X[c], errors="coerce")
        m = col.mean(skipna=True)
        X[c] = col.fillna(m)

    # 分散ゼロ列・重複列を除外
    X = X.loc[:, X.var() > 1e-12]
    X = X.loc[:, ~X.T.duplicated()]

    # 標準化
    mu = X.mean(axis=0)
    sd = X.std(axis=0, ddof=1).replace(0, 1.0)
    Z = (X - mu) / sd
    Z = Z.values

    # SVD
    U, S, VT = np.linalg.svd(Z, full_matrices=False)  # Z ≈ U @ diag(S) @ VT
    # 固有値/寄与率
    n_samples = Z.shape[0]
    eigvals = (S**2) / (n_samples - 1) if n_samples > 1 else (S**2)
    ev_ratio = eigvals / eigvals.sum() if eigvals.sum() > 0 else np.zeros_like(eigvals)

    # scores = U * S  （各行のPC座標）
    scores = U * S
    # loadings = VT.T （各項目の固有ベクトル）
    loadings = VT.T

    scores_df = pd.DataFrame(scores, columns=[f"PC{i+1}" for i in range(scores.shape[1])])
    loadings_df = pd.DataFrame(loadings, index=X.columns, columns=[f"PC{i+1}" for i in range(loadings.shape[1])])
    return scores_df, loadings_df, eigvals, ev_ratio

# ===== 旧マトリクス描画（参考） =====
def draw_matrix_plot(df: pd.DataFrame, show_all: bool, show_labels: bool, max_labels: int):
    fig, ax = plt.subplots(figsize=(9, 6), dpi=120)
    ok_vals = {"yes","y","true","1","ok","○","はい","可"}
    mask = df.get("味フィルター（必要条件）", pd.Series(["はい"]*len(df))).astype(str).str.strip().str.lower().isin(ok_vals)
    plot_df = df.copy() if show_all else df[mask].copy()

    ax.add_patch(Rectangle((0, 0), 50, 50, facecolor=(0,0,0,0.02), edgecolor="none"))
    ax.add_patch(Rectangle((MIDLINE, 0), 50-MIDLINE, 50, facecolor=(0,0,0,0.04), edgecolor="none"))
    ax.add_patch(Rectangle((0, MIDLINE), 50, 50-MIDLINE, facecolor=(0,0,0,0.04), edgecolor="none"))

    ax.scatter(plot_df["多様性合計"], plot_df["防衛合計"], s=64, alpha=0.9, linewidths=0.6, edgecolors="white")
    ax.axvline(MIDLINE, lw=1); ax.axhline(MIDLINE, lw=1)
    ax.set_xlim(0, 50); ax.set_ylim(0, 50)
    ax.set_xlabel("多様性合計（1〜5×10＝10〜50）")
    ax.set_ylabel("ブランド防衛合計（1〜5×10＝10〜50）")
    ax.set_title("飲食店スコア・マトリクス（参考）")

    if show_labels and not plot_df.empty:
        label_df = plot_df.sort_values(["多様性合計","防衛合計"], ascending=False).head(max_labels)
        for _, r in label_df.iterrows():
            ax.annotate(str(r["店名"]), (r["多様性合計"], r["防衛合計"]),
                        xytext=(4, 4), textcoords="offset points", fontsize=9)

    st.pyplot(fig, clear_figure=True)
    shown = plot_df.loc[:, ["店名","多様性合計","防衛合計"]].sort_values(["防衛合計","多様性合計"], ascending=False)
    st.dataframe(shown, use_container_width=True)

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
        st.caption("縦持ち（Section/評価項目/スコア…）でも横持ち（各項目が列）でもOK。")
    else:
        st.caption("※ サービスアカウントに対象スプレッドシートを閲覧共有してください。")
        sheet_id_input = st.text_input("Spreadsheet ID / URL", value=DEFAULT_SHEET_ID, key="sheet_id")
        ws_name_input = st.text_input("Worksheet名（タブ名）", value=DEFAULT_WS_NAME, key="worksheet_name")
        svc_default_text = DEFAULT_SVC_JSON or (Path(DEFAULT_SVC_JSON_PATH).read_text(encoding="utf-8") if DEFAULT_SVC_JSON_PATH and Path(DEFAULT_SVC_JSON_PATH).exists() else "")
        svc_text = st.text_area("Service Account JSON（貼り付け）", value=svc_default_text, height=160, key="svc_json")
        if svc_text.strip():
            try:
                creds_dict = json.loads(svc_text)
                st.success("サービスアカウントJSONを読み込みました。")
            except Exception as e:
                st.error(f"JSON解析に失敗: {e}")

    st.header("PCA 設定")
    show_vectors = st.checkbox("項目ベクトルを重ね描画（最大15）", value=True, key="show_vectors")
    max_vec = st.slider("ベクトルの最大表示本数", 0, 30, 15, 1, key="max_vec")

    st.header("参考：合計点マトリクス")
    show_matrix = st.checkbox("旧マトリクスも描く", value=False, key="show_matrix")
    show_all = st.checkbox("味フィルター無視（全件）", value=False, key="show_all")
    show_labels = st.checkbox("店名ラベル（マトリクス）", value=True, key="show_labels")
    max_labels = st.slider("ラベル最大件数（マトリクス）", 0, 200, 50, 5, key="max_labels")

go = st.button("PCAを実行", type="primary", key="run_pca")

# ===== 実行 =====
def extract_sheet_id(text: str) -> str:
    t = (text or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)/?", t)
    return m.group(1) if m else t

if go:
    try:
        if source == "Excelアップロード":
            if not uploaded:
                st.error("Excelファイルをアップロードしてください。"); st.stop()
            df_raw = read_from_excel(uploaded)
        else:
            if not creds_dict or not sheet_id_input:
                st.error("スプレッドシートの設定が不足しています。"); st.stop()
            df_raw = read_from_sheets(creds_dict, sheet_id_input, ws_name_input)

        # プレビュー
        st.subheader("データプレビュー（先頭10行）")
        st.dataframe(df_raw.head(10), use_container_width=True)
        st.caption(f"行数: {len(df_raw)} / 列数: {len(df_raw.columns)}")

        # 必須メタ
        if "店名" not in df_raw.columns:
            st.error("店名 列が見つかりません。フォームに 店名 を含めてください。"); st.stop()
        if "日付" not in df_raw.columns:
            df_raw["日付"] = pd.NaT

        # 数値列（自由記述・メタ除外）
        meta_cols = ["店名","日付","タイムスタンプ","味フィルター（必要条件）"]
        numeric_cols = [c for c in df_raw.columns
                        if c not in meta_cols
                        and not any(kw in str(c) for kw in ["コメント","自由記述","備考","メモ"])
                        and pd.api.types.is_numeric_dtype(df_raw[c])]

        if len(numeric_cols) < 3:
            st.error(f"数値の評価項目が少なすぎます（見つかった数: {len(numeric_cols)}、3列以上が望ましい）。"); st.stop()

        df_items = df_raw[numeric_cols].copy()
        scores_df, loadings, ev, ev_ratio = pca_svd(df_items)

        # 可視化（PC1×PC2）
        fig, ax = plt.subplots(figsize=(9, 7), dpi=120)
        xy = scores_df[["PC1","PC2"]].values
        ax.scatter(xy[:,0], xy[:,1], s=60, alpha=0.9)
        for i, name in enumerate(df_raw["店名"].astype(str).values):
            if i < len(xy):
                ax.annotate(name, (xy[i,0], xy[i,1]), xytext=(4,4), textcoords="offset points", fontsize=9)
        ax.axhline(0, lw=1, color="gray", alpha=0.6)
        ax.axvline(0, lw=1, color="gray", alpha=0.6)
        ax.set_xlabel(f"PC1 ({ev_ratio[0]*100:.1f}% var)")
        ax.set_ylabel(f"PC2 ({ev_ratio[1]*100:.1f}% var)")
        ax.set_title("PCA マップ（店舗の位置：PC1×PC2）")
        st.pyplot(fig, clear_figure=True)

        # ベクトル（負荷量）
        if show_vectors and "PC1" in loadings.columns and "PC2" in loadings.columns:
            fig2, ax2 = plt.subplots(figsize=(9, 7), dpi=120)
            ax2.axhline(0, lw=1, color="gray", alpha=0.6)
            ax2.axvline(0, lw=1, color="gray", alpha=0.6)
            ax2.set_xlim(-1.1, 1.1); ax2.set_ylim(-1.1, 1.1)
            ax2.set_xlabel("PC1 loading"); ax2.set_ylabel("PC2 loading")
            ax2.set_title("項目ベクトル（負荷量）")
            L = loadings[["PC1","PC2"]].copy()
            L["_mag"] = np.sqrt(L["PC1"]**2 + L["PC2"]**2)
            L = L.sort_values("_mag", ascending=False).head(max_vec)
            for item, row in L.iterrows():
                ax2.arrow(0,0, row["PC1"], row["PC2"], head_width=0.03, length_includes_head=True, alpha=0.85)
                ax2.text(row["PC1"]*1.05, row["PC2"]*1.05, str(item), fontsize=9)
            st.pyplot(fig2, clear_figure=True)

        # テーブル
        st.subheader("寄与率")
        var_df = pd.DataFrame({
            "PC": [f"PC{i+1}" for i in range(len(ev_ratio))],
            "固有値": ev,
            "寄与率": ev_ratio,
            "累積寄与率": ev_ratio.cumsum()
        })
        st.dataframe(var_df.style.format({"固有値":"{:.3f}","寄与率":"{:.3%}","累積寄与率":"{:.3%}"}), use_container_width=True)

        st.subheader("負荷量（項目×PC）")
        st.dataframe(loadings.style.format("{:.3f}"), use_container_width=True)

        st.subheader("店舗スコア（PC座標）")
        out_scores = pd.concat([df_raw[["店名","日付"]].reset_index(drop=True),
                                scores_df.reset_index(drop=True)], axis=1)
        st.dataframe(out_scores, use_container_width=True)

        # ダウンロード
        st.download_button("PCA_負荷量.csv をダウンロード",
                           loadings.to_csv().encode("utf-8-sig"),
                           file_name="pca_loadings.csv", mime="text/csv")
        st.download_button("PCA_店舗スコア.csv をダウンロード",
                           out_scores.to_csv(index=False).encode("utf-8-sig"),
                           file_name="pca_scores_by_store.csv", mime="text/csv")

        # 参考：旧マトリクス
        if show_matrix:
            df_old = df_raw.copy()
            if set(DIVERSITY_COLS).issubset(df_old.columns) and set(BRAND_COLS).issubset(df_old.columns):
                df_old["多様性合計"] = df_old[DIVERSITY_COLS].sum(axis=1)
                df_old["防衛合計"] = df_old[BRAND_COLS].sum(axis=1)
                draw_matrix_plot(df_old, show_all=show_all, show_labels=show_labels, max_labels=max_labels)
            else:
                st.info("旧マトリクス用の列がないため、参考図は割愛しました。")

    except Exception as e:
        st.exception(e)

def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    列名の空白/コロン/全角半角を整え、Unnamed除去→重複集約→まだ重複があれば強制ユニーク化
    """
    # 0) まず Unnamed を落とす
    df = drop_unnamed_columns(df)

    # 1) 列名トリミング（見た目同じなのに別物…を防ぐ）
    new_cols = []
    for c in df.columns:
        cc = str(c).strip()
        # ありがちな末尾コロンや全角空白を除去
        cc = unicodedata.normalize("NFKC", cc).rstrip("：:").strip()
        new_cols.append(cc)
    df.columns = new_cols

    # 2) 正規化（完全一致ルール）
    df = normalize_columns(df)

    # 3) 重複列を平均で集約
    df = collapse_duplicate_columns(df, agg="mean")

    # 4) それでも重複が残った場合、強制でユニーク化（安全弁）
    if df.columns.duplicated().any():
        cols = []
        seen = {}
        for c in df.columns:
            if c not in seen:
                seen[c] = 1
                cols.append(c)
            else:
                seen[c] += 1
                cols.append(f"{c}__dup{seen[c]}")
        df.columns = cols

    return df

