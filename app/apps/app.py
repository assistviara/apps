# app.py — PCA対応版：Excel/Sheets → 前処理 → PCA(SVD) → 可視化
# ラベル重なり解消：極座標リペル（角度間隔の最小化）＋外周配置

import os, re, json, unicodedata
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib import font_manager, rcParams
from matplotlib.patches import Rectangle

# ------------------------------------------------------------
# set_page_config は最初の1回だけ
# ------------------------------------------------------------
st.set_page_config(
    page_title="飲食店評価：PCA & マトリクス",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# 旧マトリクス用列（多様性/ブランド防衛スコアの集計用）
# ============================================================
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
MIDLINE = 30  # マトリクスの基準線

# ============================================================
# 日本語フォント（任意で fonts/NotoSansJP-Regular.ttf を同梱）
# ============================================================
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

# ============================================================
# 列名正規化まわり
# ============================================================
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
    mapped = []
    for c in df.columns:
        cn = _norm(c)
        dest = None
        for key, to in NORMALIZE_RULES.items():
            if _norm(key) == cn:
                dest = to
                break
        mapped.append(dest or c)
    df.columns = mapped
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

# ============================================================
# 値の 1〜5 変換（頑丈版）
# ============================================================
def _to_1to5(x):
    if isinstance(x, (pd.Series, pd.DataFrame)):
        return x.applymap(_to_1to5) if isinstance(x, pd.DataFrame) else x.apply(_to_1to5)
    if pd.isna(x): return np.nan
    s = unicodedata.normalize("NFKC", str(x)).strip()
    if s == "": return np.nan

    likert = {
        "非常に低い":1, "とても低い":1, "低い":2, "やや低い":2,
        "普通":3, "ふつう":3, "やや高い":4, "高い":4, "非常に高い":5, "とても高い":5
    }
    if s in likert: return float(likert[s])

    m = re.search(r"([0-9]+)", s)
    if m:
        v = int(m.group(1))
        if 5 < v <= 100:
            v = round(v/20)
        return float(max(1, min(5, v)))
    try:
        v = float(s)
        return float(max(1, min(5, v)))
    except:
        return np.nan

def coerce_1to5(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if any(k in str(c) for k in ["コメント","自由記述","備考","メモ"]):
            continue
        if c in ("店名","日付","タイムスタンプ"):
            continue
        df[c] = df[c].apply(_to_1to5)
    return df

def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed:")]]

def collapse_duplicate_columns(df: pd.DataFrame, agg: str = "mean") -> pd.DataFrame:
    if not df.columns.has_duplicates:
        return df
    new_data = {}
    for name in df.columns.unique():
        block = df.loc[:, df.columns == name]
        if block.shape[1] == 1:
            new_data[name] = block.iloc[:, 0]
        else:
            block_num = block.apply(pd.to_numeric, errors="coerce")
            if agg == "max":
                new_series = block_num.max(axis=1, skipna=True)
            elif agg == "min":
                new_series = block_num.min(axis=1, skipna=True)
            else:
                new_series = block_num.mean(axis=1, skipna=True)
            new_data[name] = new_series
    return pd.DataFrame(new_data)

def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_unnamed_columns(df)
    df.columns = [unicodedata.normalize("NFKC", str(c)).rstrip("：:").strip() for c in df.columns]
    df = normalize_columns(df)
    df = collapse_duplicate_columns(df, agg="mean")
    if df.columns.duplicated().any():
        seen, cols = {}, []
        for c in df.columns:
            if c not in seen:
                seen[c] = 1; cols.append(c)
            else:
                seen[c] += 1; cols.append(f"{c}__dup{seen[c]}")
        df.columns = cols
    return df

# ============================================================
# 縦持ち → 横持ち
# ============================================================
def wide_from_long(df_long: pd.DataFrame) -> pd.DataFrame:
    col_store = find_col(df_long, "店名")
    col_date  = find_col(df_long, "日付")
    col_item  = find_col(df_long, "評価項目")
    col_score = find_col(df_long, "スコア")
    assert all([col_store, col_date, col_item, col_score]), "縦持ち→横持ちに必要な列（店名/日付/評価項目/スコア）が見つかりません。"
    df_use = df_long[[col_store, col_date, col_item, col_score]].copy()
    df_use[col_score] = df_use[col_score].apply(_to_1to5)
    wide = df_use.pivot_table(index=[col_store, col_date], columns=col_item, values=col_score, aggfunc="mean")
    wide = wide.reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={col_store:"店名", col_date:"日付"})
    return coerce_1to5(wide)

# ============================================================
# Google 認証：secrets/貼付けの両対応
# ============================================================
def get_service_account_from_secrets() -> dict | None:
    try:
        g = st.secrets.get("gcp", None)
        if not g: return None
        pk = g.get("private_key", "")
        if "\\n" in pk and "\n" not in pk:
            pk = pk.replace("\\n", "\n")
        req = ["type","project_id","private_key_id","client_email","client_id","token_uri","private_key"]
        if not all(k in g for k in req):
            return None
        return {
            "type": g["type"],
            "project_id": g["project_id"],
            "private_key_id": g["private_key_id"],
            "private_key": pk,
            "client_email": g["client_email"],
            "client_id": g["client_id"],
            "auth_uri": g.get("auth_uri","https://accounts.google.com/o/oauth2/auth"),
            "token_uri": g.get("token_uri","https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": g.get("auth_provider_x509_cert_url","https://www.googleapis.com/oauth2/v1/certs"),
            "client_x509_cert_url": g.get("client_x509_cert_url",""),
            "universe_domain": g.get("universe_domain","googleapis.com"),
        }
    except Exception:
        return None

def parse_service_account_text(text: str) -> dict | None:
    if not text or not text.strip():
        return None
    raw = text.strip()
    # 1) JSON
    try:
        data = json.loads(raw)
    except Exception:
        data = None
    # 2) TOML/INI風 key="value"
    if data is None and "=" in raw and "{" not in raw:
        kv = {}
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("["):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                kv[k.strip()] = v.strip().strip('"').strip("'")
        need = ["type","project_id","private_key_id","private_key","client_email","client_id","token_uri"]
        if all(k in kv for k in need):
            data = {
                "type": kv["type"],
                "project_id": kv["project_id"],
                "private_key_id": kv["private_key_id"],
                "private_key": kv["private_key"],
                "client_email": kv["client_email"],
                "client_id": kv["client_id"],
                "auth_uri": kv.get("auth_uri","https://accounts.google.com/o/oauth2/auth"),
                "token_uri": kv.get("token_uri","https://oauth2.googleapis.com/token"),
                "auth_provider_x509_cert_url": kv.get("auth_provider_x509_cert_url","https://www.googleapis.com/oauth2/v1/certs"),
                "client_x509_cert_url": kv.get("client_x509_cert_url",""),
                "universe_domain": kv.get("universe_domain","googleapis.com"),
            }
    if data is None:
        return None
    # 改行補正
    pk = data.get("private_key","")
    if "\\n" in pk and "\n" not in pk:
        pk = pk.replace("\\n", "\n")
    data["private_key"] = pk
    return data

def get_service_account_any() -> dict | None:
    sc = get_service_account_from_secrets()
    if sc:
        return sc
    pasted = st.session_state.get("svc_json", "")
    return parse_service_account_text(pasted)

# ============================================================
# 入力データ読込
# ============================================================
def read_from_excel(file) -> pd.DataFrame:
    df = pd.read_excel(file).dropna(how="all")
    df = sanitize_columns(df)
    if find_col(df, "評価項目") and find_col(df, "スコア"):
        df = wide_from_long(df)
    else:
        alt = find_col(df, "店名")
        if alt and alt != "店名": df = df.rename(columns={alt:"店名"})
        alt = find_col(df, "日付")
        if alt and alt != "日付": df = df.rename(columns={alt:"日付"})
        df = coerce_1to5(df)
    df = sanitize_columns(df)
    return df

def extract_sheet_id(text: str) -> str:
    t = (text or "").strip()
    m = re.search(r"/d/([a-zA-Z0-9-_]+)/?", t)
    return m.group(1) if m else t

def read_from_sheets(creds_dict, sheet_id, worksheet) -> pd.DataFrame:
    # 依存ライブラリは関数内 import（未使用時は不要）
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
    if find_col(df, "評価項目") and find_col(df, "スコア"):
        df = wide_from_long(df)
    else:
        alt = find_col(df, "店名")
        if alt and alt != "店名": df = df.rename(columns={alt:"店名"})
        alt = find_col(df, "日付")
        if alt and alt != "日付": df = df.rename(columns={alt:"日付"})
        df = coerce_1to5(df)
    df = sanitize_columns(df)
    return df

# ============================================================
# PCA（SVD）
# ============================================================
def pca_svd(df_items: pd.DataFrame):
    X = df_items.copy()
    for c in X.columns:
        col = pd.to_numeric(X[c], errors="coerce")
        X[c] = col.fillna(col.mean(skipna=True))
    # 定数列/重複列の除去
    X = X.loc[:, X.var() > 1e-12]
    X = X.loc[:, ~X.T.duplicated()]
    mu = X.mean(axis=0)
    sd = X.std(axis=0, ddof=1).replace(0, 1.0)
    Z = ((X - mu) / sd).values
    U, S, VT = np.linalg.svd(Z, full_matrices=False)
    n = Z.shape[0]
    eigvals = (S**2)/(n-1) if n > 1 else (S**2)
    ev_ratio = eigvals/eigvals.sum() if eigvals.sum()>0 else np.zeros_like(eigvals)
    scores = U * S
    loadings = VT.T
    scores_df = pd.DataFrame(scores, columns=[f"PC{i+1}" for i in range(scores.shape[1])])
    loadings_df = pd.DataFrame(loadings, index=X.columns, columns=[f"PC{i+1}" for i in range(loadings.shape[1])])
    return scores_df, loadings_df, eigvals, ev_ratio

# ============================================================
# 参考：旧マトリクス描画
# ============================================================
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
                        xytext=(4,4), textcoords="offset points", fontsize=9)
    st.pyplot(fig, clear_figure=True)
    shown = plot_df.loc[:, ["店名","多様性合計","防衛合計"]].sort_values(["防衛合計","多様性合計"], ascending=False)
    st.dataframe(shown, use_container_width=True)

# ============================================================
# ベクトル図：極座標リペル（角度最小間隔）＋外周配置
# ============================================================
def draw_loading_vectors(loadings: pd.DataFrame,
                         max_vec: int = 15,
                         arrow_scale: float = 1.4,
                         radius_mode: str = "auto",    # "auto" or "fixed"
                         label_scale: float = 1.5,      # auto時：ベクトル先端×倍率
                         fixed_radius: float = 1.6,     # fixed時：外周半径（データ座標）
                         min_angle_deg: float = 10.0,   # ラベル間の最小角度
                         use_guides: bool = True):
    """
    - ベクトルは原点→(PC1,PC2)*arrow_scale
    - ラベルは外周に角度順で並べ、隣接ラベルの角度間隔を min_angle_deg 以上に調整
    - ラベル位置は radius_mode:
        * "auto": ベクトル先端からさらに label_scale 倍外側
        * "fixed": すべて fixed_radius の円周上に載せる
    """
    if not {"PC1","PC2"}.issubset(loadings.columns):
        fig, ax = plt.subplots(figsize=(9, 7), dpi=120)
        ax.text(0.5, 0.5, "PC2 が計算できなかったため\nベクトル図は省略します。", ha="center", va="center", fontsize=12)
        ax.axis("off")
        return fig

    fig, ax = plt.subplots(figsize=(9, 7), dpi=120)
    ax.axhline(0, lw=1, color="gray", alpha=0.6)
    ax.axvline(0, lw=1, color="gray", alpha=0.6)
    lim = 1.2 * max(1.0, arrow_scale, fixed_radius if radius_mode=="fixed" else 1.0)
    ax.set_xlim(-lim, lim); ax.set_ylim(-lim, lim)
    ax.set_xlabel("PC1 loading"); ax.set_ylabel("PC2 loading")
    ax.set_title("項目ベクトル（負荷量）")

    # 上位ベクトル抽出
    L = loadings[["PC1","PC2"]].copy()
    L["_mag"] = np.sqrt(L["PC1"]**2 + L["PC2"]**2)
    L = L.sort_values("_mag", ascending=False).head(max(1, int(max_vec)))

    # ベクトル（拡大）
    tips = []
    for item, row in L.iterrows():
        x, y = float(row["PC1"])*arrow_scale, float(row["PC2"])*arrow_scale
        ax.arrow(0, 0, x, y, head_width=0.03*arrow_scale, length_includes_head=True, alpha=0.9)
        tips.append((item, x, y))

    # 角度（-pi,pi] に正規化
    data = []
    for item, x, y in tips:
        theta = np.arctan2(y, x)  # -pi..pi
        r_tip = np.hypot(x, y)
        if radius_mode == "fixed":
            r_lbl = fixed_radius
        else:
            r_lbl = max(r_tip * label_scale, r_tip + 0.05)  # 先端より外側
        data.append([item, x, y, theta, r_tip, r_lbl])

    # 角度でソート → 最小角度間隔を確保（一周分で調整）
    data.sort(key=lambda z: z[3])  # theta
    min_d = np.deg2rad(min_angle_deg)

    thetas = np.array([d[3] for d in data], dtype=float)
    thetas_wrapped = thetas.copy()
    for i in range(1, len(thetas_wrapped)):
        if thetas_wrapped[i] - thetas_wrapped[i-1] < min_d:
            thetas_wrapped[i] = thetas_wrapped[i-1] + min_d
    # 端と端（循環）の間隔も調整
    total_span = thetas_wrapped[-1] - thetas_wrapped[0]
    if total_span < 2*np.pi - min_d:
        need = (2*np.pi - min_d) - total_span
        shift_each = need / 2.0
        thetas_wrapped[0] -= shift_each
        thetas_wrapped[-1] += shift_each

    # 角度を -pi..pi に戻す
    thetas_adj = ((thetas_wrapped + np.pi) % (2*np.pi)) - np.pi

    # ラベル配置と矢印（先端→ラベル）
    for theta, d in zip(thetas_adj, data):
        item, x_tip, y_tip, _, _, r_lbl = d
        x_lbl, y_lbl = r_lbl*np.cos(theta), r_lbl*np.sin(theta)
        ax.annotate(
            str(item),
            xy=(x_tip, y_tip),
            xytext=(x_lbl, y_lbl),
            arrowprops=dict(arrowstyle="->", lw=0.7, alpha=0.85),
            fontsize=9, ha="center", va="center"
        )

    if use_guides:
        circle = plt.Circle((0,0), radius=(fixed_radius if radius_mode=="fixed" else 1.05*arrow_scale),
                            fill=False, linestyle="--", linewidth=0.6, alpha=0.35)
        ax.add_artist(circle)

    return fig

# ============================================================
# UI
# ============================================================
st.title("飲食店評価：主成分分析（PCA） & マトリクス")

with st.sidebar:
    st.header("データソース")
    source = st.radio("選択", ["Excelアップロード", "Googleスプレッドシート"], index=1, key="source_kind")

    uploaded = None
    if source == "Excelアップロード":
        uploaded = st.file_uploader("Excelファイル（.xlsx）を選択", type=["xlsx"], key="xlsx_uploader")
        st.caption("縦持ち（Section/評価項目/スコア…）でも横持ち（各項目が列）でもOK。")
        sheet_id_input = ""; ws_name_input  = ""
    else:
        sheet_id_input = st.text_input("Spreadsheet ID / URL", value=st.secrets.get("gcp", {}).get("sheet_id", ""), key="sheet_id")
        ws_name_input  = st.text_input("Worksheet名（タブ名）", value=st.secrets.get("gcp", {}).get("worksheet", "Form Responses"), key="worksheet_name")
        st.text_area("Service Account JSON（貼り付け）", height=160, key="svc_json")

    st.header("PCA 設定（ベクトル図）")
    max_vec = st.slider("ベクトルの最大表示本数", 0, 30, 15, 1, key="max_vec")
    arrow_scale = st.slider("ベクトル拡大倍率（広がり）", 1.0, 2.5, 1.4, 0.1, key="arrow_scale")
    label_mode = st.radio("ラベル半径モード", ["自動（先端から外側）", "固定（外周円に揃える）"], index=0, key="label_mode")
    label_scale = st.slider("自動モード：外側倍率", 1.05, 2.0, 1.50, 0.05, key="label_scale")
    fixed_radius = st.slider("固定モード：外周半径", 1.1, 2.2, 1.6, 0.1, key="fixed_radius")
    min_angle_deg = st.slider("最小角度間隔（ラベル同士）", 4, 22, 10, 1, key="min_angle_deg")

    st.header("参考：合計点マトリクス")
    show_matrix = st.checkbox("旧マトリクスも描く", value=False, key="show_matrix")
    show_all = st.checkbox("味フィルター無視（全件）", value=False, key="show_all")
    show_labels = st.checkbox("店名ラベル（マトリクス）", value=True, key="show_labels")
    max_labels = st.slider("ラベル最大件数（マトリクス）", 0, 200, 50, 5, key="max_labels")

go = st.button("PCAを実行", type="primary", key="run_pca")

# ============================================================
# 実行
# ============================================================
if go:
    try:
        # データ読込
        if source == "Googleスプレッドシート":
            creds_dict = get_service_account_any()
            if not creds_dict:
                st.error("認証情報がありません。secrets.toml またはサイドバーへ Service Account JSON を貼り付けてください。"); st.stop()
            if not sheet_id_input:
                st.error("Spreadsheet ID / URL を入力してください。"); st.stop()
            df_raw = read_from_sheets(creds_dict, sheet_id_input, ws_name_input)
        else:
            if not uploaded:
                st.error("Excelファイルをアップロードしてください。"); st.stop()
            df_raw = read_from_excel(uploaded)

        st.subheader("データプレビュー（先頭10行）")
        st.dataframe(df_raw.head(10), use_container_width=True)
        st.caption(f"行数: {len(df_raw)} / 列数: {len(df_raw.columns)}")

        if "店名" not in df_raw.columns:
            st.error("店名 列が見つかりません。フォームに 店名 を含めてください。"); st.stop()
        if "日付" not in df_raw.columns:
            df_raw["日付"] = pd.NaT

        meta_cols = ["店名","日付","タイムスタンプ","味フィルター（必要条件）"]
        numeric_cols = [c for c in df_raw.columns
                        if c not in meta_cols
                        and not any(kw in str(c) for kw in ["コメント","自由記述","備考","メモ"])
                        and pd.api.types.is_numeric_dtype(df_raw[c])]
        if len(numeric_cols) < 3:
            st.error(f"数値の評価項目が少なすぎます（見つかった数: {len(numeric_cols)}、3列以上が望ましい）。"); st.stop()

        # PCA
        df_items = df_raw[numeric_cols].copy()
        scores_df, loadings, ev, ev_ratio = pca_svd(df_items)

        # 散布図（PC1×PC2）
        if {"PC1","PC2"}.issubset(scores_df.columns):
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
        else:
            st.info("サンプル数や項目の都合でPC2が得られませんでした。散布図は省略します。")

        # ベクトル図（重なり無し版）
        fig2 = draw_loading_vectors(
            loadings=loadings,
            max_vec=int(max_vec),
            arrow_scale=float(arrow_scale),
            radius_mode=("fixed" if label_mode.startswith("固定") else "auto"),
            label_scale=float(label_scale),
            fixed_radius=float(fixed_radius),
            min_angle_deg=float(min_angle_deg),
            use_guides=True,
        )
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
                draw_matrix_plot(df_old, show_all, show_labels, max_labels)
            else:
                st.info("旧マトリクス用の列がないため、参考図は割愛しました。")

    except Exception as e:
        st.exception(e)
