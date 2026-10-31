"""Étape 3 : nettoyage des prix et calcul des marges.

Copie le fichier du jour vers data/cleaned/JJ-MM-AAAA cleaned.xlsx, nettoie
les prix, calcule les marges Mytek / Tunisianet, puis exporte la Sheet3
vers la source Power BI (data/powerbi/table_modifiee.xlsx).
"""
import pandas as pd
import numpy as np
import shutil
import re

from maktabi import config

SHEET3 = "Sheet3"
# Feuille remplacée dans le classeur Power BI
TARGET_SHEET = "Sheet1"


# ---------- 3) adjust prix_codis (>10000 -> //100) ----------
def _adjust_prix_codis(x):
    if pd.isna(x):
        return pd.NA
    try:
        # normalize separators and remove spaces / NBSP
        s = str(x).replace(" ", "").replace(" ", "").replace(",", ".")
        v = float(s)
        v = int(v)  # drop decimals if any
        return (v // 100) if v > 10000 else v
    except Exception:
        return pd.NA


# ---------- 4) CLEAN MYTEK PRICES (mytek_apres_remise) ----------
# Examples handled:
# "339,000 " -> 339 ; "1 244,000" -> 1244 ; "339" -> 339 ; "1.244,000" -> 1244
def _clean_price(x):
    if pd.isna(x):
        return pd.NA
    s = str(x)

    # Strip common currency/labels
    s = s.replace("DT", "").replace("TND", "").replace("﻿", "").strip()

    # Remove all kinds of spaces (regular & unicode)
    for sp in [" ", " ", " ", " ", " "]:
        s = s.replace(sp, "")

    # Normalize separators: remove thousands marks, unify decimal comma
    s = s.replace(".", "")                    # drop thousands dot
    s = s.replace("’", "").replace("'", "")   # drop apostrophes
    s = s.replace("٫", ",")              # Arabic decimal sep -> comma

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


# ---------- 6) margins ----------
def _marge(p_codis, p_ttc):
    if pd.isna(p_codis) or pd.isna(p_ttc):
        return pd.NA
    try:
        return round(abs(1.0 - ((float(p_ttc) / 1.07) / float(p_codis))) * 100.0, 2)
    except Exception:
        return pd.NA


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


def main(date_str=None):
    src = config.daily_file(date_str)
    dst = config.cleaned_file(date_str)

    # ---------- 0) clone the file (xlsx → xlsx) ----------
    if not src.exists():
        raise FileNotFoundError(f"Source Excel not found: {src}")

    # Copy source → destination (clone)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    print(f"📄 Cloned: {src} → {dst}")

    # ---------- 1) load Sheet3 from the cloned file ----------
    df = pd.read_excel(dst, sheet_name=SHEET3)

    # ---------- 3) adjust prix_codis ----------
    if "prix_codis" in df.columns:
        df["prix_codis"] = df["prix_codis"].apply(_adjust_prix_codis)

    # ---------- 4) clean mytek prices ----------
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

    # ---------- 7.5) drop rows with too-high margins ----------
    df, removed_rows = drop_high_margin_rows(df, threshold=35)
    print(f"🧹 Removed {removed_rows} rows with margin > 35% in any of ['marge_mytek%', 'marge_tunisianet%'].")

    # ---------- 8) write back ONLY Sheet3 in the cloned file ----------
    with pd.ExcelWriter(dst, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, index=False, sheet_name=SHEET3)

    print(f"✅ Sheet3 updated in cloned file: {dst} (other sheets untouched).")

    # ---------- 9) EXPORT: replace Sheet1 in Power BI workbook with latest Sheet3 ----------
    target_path = config.POWERBI_TABLE
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if target_path.exists():
        with pd.ExcelWriter(target_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
            df.to_excel(w, index=False, sheet_name=TARGET_SHEET)
        print(f"📦 Replaced '{TARGET_SHEET}' in existing workbook: {target_path}")
    else:
        with pd.ExcelWriter(target_path, engine="openpyxl", mode="w") as w:
            df.to_excel(w, index=False, sheet_name=TARGET_SHEET)
        print(f"🆕 Created workbook and wrote '{TARGET_SHEET}': {target_path}")

    print("🎯 Done.")


if __name__ == "__main__":
    main()
