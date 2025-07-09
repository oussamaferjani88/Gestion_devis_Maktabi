"""
associer_ssc.py
---------------
Attribue automatiquement un sous_sous_categorie_id
à tous les produits de Sheet3 (test9.xlsx)
"""

import pandas as pd, re, unidecode, time
from rapidfuzz import fuzz

EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"
SHEET_PROD = "Sheet3"
SHEET_SSC  = "SSC"
SIM_THRESHOLD = 80          # % mini pour fuzzy-match

def norm(txt: str) -> str:
    """normalize string : minuscules, sans accents, sans ponctuation"""
    txt = unidecode.unidecode(str(txt or '').lower())
    txt = re.sub(r'[^a-z0-9 ]+', ' ', txt)
    return re.sub(r'\s+', ' ', txt).strip()

# 1. Charger le mapping SSC
ssc = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_SSC)
ssc["norm_nom"] = ssc["nom"].apply(norm)

# 2. Charger les produits
prod = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_PROD)

# 3. Préparer lookup par sous_categorie_id
ssc_by_sub = {}
for _, r in ssc.iterrows():
    sid = int(r.sous_categorie_id)
    ssc_by_sub.setdefault(sid, []).append(
        (int(r.id), r.nom, r.norm_nom)
    )

# 4. Parcourir produits et attribuer sous_sous_categorie_id
def guess_ssc(row):
    if pd.notna(row["sous_sous_categorie_id"]) and int(row["sous_sous_categorie_id"]) != 0:
        return row["sous_sous_categorie_id"]   # déjà rempli

    sub_id = int(row["sous_categorie_id"])
    if sub_id not in ssc_by_sub:
        return 0

    name_norm = norm(row["nom"])
    # 4.1 correspondance exacte
    for ssc_id, ssc_nom, ssc_norm in ssc_by_sub[sub_id]:
        if ssc_norm in name_norm:
            return ssc_id
    # 4.2 similarité
    best_id, best_score = 0, 0
    for ssc_id, ssc_nom, ssc_norm in ssc_by_sub[sub_id]:
        score = fuzz.partial_ratio(name_norm, ssc_norm)
        if score > best_score:
            best_id, best_score = ssc_id, score
    return best_id if best_score >= SIM_THRESHOLD else 0

t0 = time.time()
prod["sous_sous_categorie_id"] = prod.apply(guess_ssc, axis=1)
elapsed = time.time() - t0

# 5. Sauvegarde
with pd.ExcelWriter(EXCEL_PATH, mode="a", engine="openpyxl",
                    if_sheet_exists="replace") as writer:
    prod.to_excel(writer, index=False, sheet_name=SHEET_PROD)

print(f"✅ Attribution terminée en {elapsed:.1f}s.")
print("   Produits sans sous_sous_categorie_id :", (prod['sous_sous_categorie_id'] == 0).sum())
