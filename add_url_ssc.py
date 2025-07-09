"""
add_url_to_ssc_check.py
—————————
• Construit l’URL Mytek pour chaque sous-sous-catégorie (SSC sheet)
• Vérifie que l’URL HTTP répond ‘200 OK’
• Signale dans la console les erreurs  (404 / 403 / timeout / …)
"""

import re, unidecode, pandas as pd, requests, time

EXCEL  = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"
SSC_SHEET   = "SSC"        # sous-sous-catégories  (id, categorie_id, sous_categorie_id, nom)
SUB_SHEET   = "Sheet2"  # sous-catégories       (id, categorie_id, nom)
CAT_SHEET   = "Sheet1"      # catégories            (id, nom)
BASE_URL    = "https://www.mytek.tn/"
UA          = {"User-Agent": "Mozilla/5.0"}
TIMEOUT     = 6           # seconds for HEAD

# ---------- helpers ----------
def slug(text: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', unidecode.unidecode(text.lower())).strip('-')

def check_url(url: str) -> bool:
    try:
        r = requests.head(url, headers=UA, allow_redirects=True, timeout=TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False

# ---------- load sheets ----------
ssc = pd.read_excel(EXCEL, sheet_name=SSC_SHEET)
sub = pd.read_excel(EXCEL, sheet_name=SUB_SHEET)
cat = pd.read_excel(EXCEL, sheet_name=CAT_SHEET)

sub_slug = {int(r.id): slug(r.nom) for _, r in sub.iterrows()}
cat_slug = {int(r.id): slug(r.nom) for _, r in cat.iterrows()}

# ---------- build + validate ----------
bad, missing = 0, 0
urls = []
t0 = time.time()
for _, row in ssc.iterrows():
    cid, sid = int(row.categorie_id), int(row.sous_categorie_id)

    if cid not in cat_slug or sid not in sub_slug:
        urls.append(None)
        print(f"⚠️  Missing name for cat_id={cid} or sub_id={sid} ➜ URL skipped.")
        missing += 1
        continue

    url = f"{BASE_URL}{cat_slug[cid]}/{sub_slug[sid]}/{slug(row.nom)}.html"
    valid = check_url(url)
    if not valid:
        print(f"❌  {url}  -> NOT 200")
        bad += 1
    urls.append(url)

ssc["url"] = urls
elapsed = time.time() - t0

# ---------- save ----------
with pd.ExcelWriter(EXCEL, engine="openpyxl", mode="a",
                    if_sheet_exists="replace") as w:
    ssc.to_excel(w, index=False, sheet_name=SSC_SHEET)

print(f"\n✅ Terminé en {elapsed:.1f}s  –  lignes : {len(ssc)}")
print(f"   URLs invalides : {bad}   |   noms manquants : {missing}")
