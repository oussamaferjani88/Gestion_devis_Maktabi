"""
complete_onduleur_urls.py
—————————
Ajoute les URL manquantes pour les sous-sous-catégories Onduleurs
(id_categorie = 6, id_sous_categorie = 10) de la feuille SSC.
"""

import re, unidecode, pandas as pd, requests, time
from bs4 import BeautifulSoup

EXCEL      = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"
SSC_SHEET  = "SSC"

BASE_PATH  = "https://www.mytek.tn/reseaux-securite/onduleurs/"
UA         = {"User-Agent": "Mozilla/5.0"}
TIMEOUT    = 6

# ---------- helpers ----------
def slug(text: str) -> str:
    """onduleur Off-Line -> onduleur-off-line"""
    text = unidecode.unidecode(text.lower())
    return re.sub(r'[^a-z0-9]+', '-', text).strip('-')

# table d’alias pour Mytek
ALIASES = {
    "line-interactive": "onduleur-in-line",
    "in-line"         : "onduleur-in-line",
    "in line"         : "onduleur-in-line",
    "offline"         : "onduleur-off-line",
    "off line"        : "onduleur-off-line",
}

def to_mytek_slug(name: str) -> str:
    s = slug(name)
    return ALIASES.get(s, s)

def url_ok(url: str) -> bool:
    try:
        r = requests.head(url, allow_redirects=True,
                          headers=UA, timeout=TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False

# ---------- 1. Charger SSC ----------
ssc = pd.read_excel(EXCEL, sheet_name=SSC_SHEET)

mask = (
    (ssc["categorie_id"] == 6) &
    (ssc["sous_categorie_id"] == 10) &
    (ssc["url"].isna() | (ssc["url"]==""))
)

todo = ssc[mask].copy()
print(f"🔧 {len(todo)} lignes Onduleurs sans URL à traiter")

added, failed = 0, 0
for idx, row in todo.iterrows():
    my_slug = to_mytek_slug(row.nom)
    url     = f"{BASE_PATH}{my_slug}.html"
    if url_ok(url):
        ssc.at[idx, "url"] = url
        print(f"✅ {row.nom:<25} ➜ {url}")
        added += 1
    else:
        print(f"❌ {row.nom:<25} ➜ URL introuvable (slug : {my_slug})")
        failed += 1
    time.sleep(0.6)

# ---------- 2. Sauvegarder ----------
with pd.ExcelWriter(EXCEL, engine="openpyxl",
                    mode="a", if_sheet_exists="replace") as w:
    ssc.to_excel(w, index=False, sheet_name=SSC_SHEET)

print(f"\nRésultat : {added} URL ajoutées – {failed} échecs.")
