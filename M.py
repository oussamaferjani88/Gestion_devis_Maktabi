# pip install pandas openpyxl
import pandas as pd
import numpy as np
from pathlib import Path
import shutil
from datetime import datetime
import re

# ---------- dynamic paths (today) ----------
today_str = datetime.now().strftime("%d-%m-%Y")
BASE_DIR = r"C:\Users\NESSIM\Desktop\scrapping web"
SRC_FILE = fr"{BASE_DIR}\{today_str}.xlsx"
DST_FILE = fr"{BASE_DIR}\{today_str} cleaned.xlsx"
SHEET3   = "Sheet3"

# Target workbook (where Sheet1 must be replaced by the latest Sheet3)
TARGET_BOOK = r"C:\Users\NESSIM\Desktop\scrapping fournisseur\table_modifiee.xlsx"
TARGET_SHEET = "Sheet1"

# ---------- 0) clone the file (xlsx → xlsx) ----------
src = Path(SRC_FILE)
if not src.exists():
    raise FileNotFoundError(f"Source Excel not found: {SRC_FILE}")

# Copy source → destination (clone)
shutil.copy2(src, DST_FILE)
print(f"📄 Cloned: {SRC_FILE} → {DST_FILE}")

# ---------- 1) load Sheet3 from the cloned file ----------
df = pd.read_excel(DST_FILE, sheet_name=SHEET3)

# ---------- 2) (reserved for earlier steps if any) ----------

# ---------- 3) adjust prix_codis (>10000 -> //100) ----------
if "prix_codis" in df.columns:
    def _adjust_prix_codis(x):
        if pd.isna(x):
            return pd.NA
        try:
            # normalize separators and remove spaces / NBSP
            s = str(x).replace(" ", "").replace("\u00A0", "").replace(",", ".")
            v = float(s)
            v = int(v)  # drop decimals if any
            return (v // 100) if v > 10000 else v
        except Exception:
            return pd.NA

    df["prix_codis"] = df["prix_codis"].apply(_adjust_prix_codis)

# ---------- 4) CLEAN MYTEK PRICES (mytek_apres_remise) ----------
# Examples handled:
# "339,000 " -> 339 ; "1 244,000" -> 1244 ; "339" -> 339 ; "1.244,000" -> 1244
def _clean_price(x):
    if pd.isna(x):
        return pd.NA
    s = str(x)

    # Strip common currency/labels
    s = s.replace("DT", "").replace("TND", "").replace("\ufeff", "").strip()

    # Remove all kinds of spaces (regular & unicode)
    for sp in [" ", "\u00A0", "\u202F", "\u2009", "\u2007"]:
        s = s.replace(sp, "")

    # Normalize separators: remove thousands marks, unify decimal comma
    s = s.replace(".", "")                    # drop thousands dot
    s = s.replace("’", "").replace("'", "")   # drop apostrophes
    s = s.replace("\u066B", ",")              # Arabic decimal sep -> comma

    # If ends with decimal zeros (e.g., ",000", ",00"), drop them
    s = re.sub(r",0+$", "", s)

    # If still has a comma, drop decimal part (keep integer part only)
    if "," in s:
        s = s.split(",")[0]

    # Keep digits only
    s = re.sub(r"\D", "", s)

    if not s:
        return pd.NA
    try:
        return int(s)
    except Exception:
        return pd.NA

if "mytek_apres_remise" in df.columns:
    cleaned = df["mytek_apres_remise"].apply(_clean_price)
    # Use pandas nullable integer to keep NA support
    try:
        df["mytek_apres_remise"] = cleaned.astype("Int64")
    except Exception:
        df["mytek_apres_remise"] = cleaned

# ---------- 5) drop tunisianet_avant_remise (unchanged) ----------
if "tunisianet_avant_remise" in df.columns:
    df = df.drop(columns=["tunisianet_avant_remise"])

# ---------- 6) margins ----------
def _marge(p_codis, p_ttc):
    if pd.isna(p_codis) or pd.isna(p_ttc):
        return pd.NA
    try:
        return round(abs(1.0 - ((float(p_ttc) / 1.07) / float(p_codis))) * 100.0, 2)
    except Exception:
        return pd.NA

if {"prix_codis", "mytek_apres_remise"}.issubset(df.columns):
    df["marge_mytek%"] = [_marge(pc, mt) for pc, mt in zip(df["prix_codis"], df["mytek_apres_remise"])]

if {"prix_codis", "tunisianet_apres_remise"}.issubset(df.columns):
    df["marge_tunisianet%"] = [_marge(pc, tn) for pc, tn in zip(df["prix_codis"], df["tunisianet_apres_remise"])]

# ---------- 7) average margin ----------
if {"marge_mytek%", "marge_tunisianet%"}.issubset(df.columns):
    a = pd.to_numeric(df["marge_mytek%"], errors="coerce")
    b = pd.to_numeric(df["marge_tunisianet%"], errors="coerce")
    df["marge_moyenne"] = np.where(
        a.isna() | b.isna(),
        pd.NA,
        ((a.astype(float) + b.astype(float)) / 2.0).round(2)
    )

# (No sorting)
# ---------- 7.5) drop rows with too-high margins ----------
def drop_high_margin_rows(frame, cols=("marge_mytek%", "marge_tunisianet%"), threshold=50):
    """Remove rows where any margin in `cols` is > threshold.
       NAs are ignored (not dropped). Returns (filtered_df, removed_count)."""
    if not isinstance(cols, (list, tuple)):
        cols = [cols]

    # start with all-False mask
    to_drop = pd.Series(False, index=frame.index)

    for c in cols:
        if c in frame.columns:
            vals = pd.to_numeric(frame[c], errors="coerce")
            to_drop |= (vals > float(threshold))  # only true when value is numeric and > threshold

    removed = int(to_drop.sum())
    filtered = frame.loc[~to_drop].copy()
    return filtered, removed

df, removed_rows = drop_high_margin_rows(df, threshold=35)
print(f"🧹 Removed {removed_rows} rows with margin > 35% in any of ['marge_mytek%', 'marge_tunisianet%'].")


# ---------- 8) write back ONLY Sheet3 in the cloned file ----------
with pd.ExcelWriter(DST_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
    df.to_excel(w, index=False, sheet_name=SHEET3)

print(f"✅ Sheet3 updated in cloned file: {DST_FILE} (other sheets untouched).")

# ---------- 9) EXPORT: replace Sheet1 in target workbook with latest Sheet3 ----------
target_path = Path(TARGET_BOOK)
target_path.parent.mkdir(parents=True, exist_ok=True)

if target_path.exists():
    with pd.ExcelWriter(TARGET_BOOK, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, index=False, sheet_name=TARGET_SHEET)
    print(f"📦 Replaced '{TARGET_SHEET}' in existing workbook: {TARGET_BOOK}")
else:
    with pd.ExcelWriter(TARGET_BOOK, engine="openpyxl", mode="w") as w:
        df.to_excel(w, index=False, sheet_name=TARGET_SHEET)
    print(f"🆕 Created workbook and wrote '{TARGET_SHEET}': {TARGET_BOOK}")

print("🎯 Done.")
# ---------- 10) ALSO save a copy into cleaned folder ----------
CLEANED_DIR = Path(r"C:\Users\NESSIM\Desktop\cleaned")
CLEANED_DIR.mkdir(parents=True, exist_ok=True)

# Name file with today's date
today_str = datetime.now().strftime("%d-%m-%Y")
cleaned_copy = CLEANED_DIR / f"{today_str} cleaned.xlsx"

# Copy the cleaned DST_FILE into cleaned folder
shutil.copy2(DST_FILE, cleaned_copy)

print(f"📂 Extra copy created: {cleaned_copy}")