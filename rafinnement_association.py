"""
map_ssc_to_sheet3.py
————————
Associe le bon sous_sous_categorie_id aux produits de Sheet3
en explorant les URLs stockées dans la feuille SSC.

• Retry automatique (3 tentatives) + timeout 20 s
• Pagination ?p=2,3… jusqu’à page vide
• Match par url_mytek ou reference_mytek (SKU)
"""

import re, time, unidecode, requests, pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter            # <-- import manquant
from urllib3.util.retry import Retry                 # <-- import manquant

# ---------- CONFIG ----------
EXCEL      = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"
SHEET3     = "Sheet3"
SHEET_SSC  = "SSC"
TIMEOUT    = 20            # secondes
PAUSE      = 0.8           # pause entre pages
MAX_PAGES  = 30

# ---------- Session HTTP avec Retry ----------
retry = Retry(total=3, backoff_factor=1,
              status_forcelist=[500, 502, 503, 504],
              allowed_methods=["HEAD", "GET"])
sess = requests.Session()
sess.headers.update({"User-Agent": "Mozilla/5.0"})
sess.mount("https://", HTTPAdapter(max_retries=retry))
sess.mount("http://",  HTTPAdapter(max_retries=retry))

# ---------- Scraper catalogue ----------
def scrape_catalog(url_base: str):
    """Yield dicts {url, ref, name} pour chaque produit d’un catalogue."""
    for page in range(1, MAX_PAGES + 1):
        url = url_base if page == 1 else f"{url_base}?p={page}"
        try:
            r = sess.get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                break
        except requests.RequestException as e:
            print(f"   ⚠️  {e}  (skip page)")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("li.item.product.product-item")
        if not items:
            break

        for it in items:
            name = it.select_one("h2.product.name.product-item-name")
            ref  = it.select_one("div.skuDesktop")
            link = it.select_one("a.product.photo.product-item-photo")
            yield {
                "name": name.text.strip() if name else None,
                "ref" : re.sub(r'[\[\]]','', ref.text).strip().upper() if ref else '',
                "url" : link["href"] if link else url,
            }
        time.sleep(PAUSE)

def norm_ref(r):          # normalise les SKU pour matching
    return re.sub(r'[^A-Z0-9]', '', str(r or '').upper())

# ---------- 1. Charger Excel ----------
ssc = pd.read_excel(EXCEL, sheet_name=SHEET_SSC)
p3  = pd.read_excel(EXCEL, sheet_name=SHEET3)

# index Sheet3
p3["ref_norm"] = p3["reference_mytek"].apply(norm_ref)
idx_url = {u: i for i, u in p3["url_mytek"].items() if isinstance(u, str)}
idx_ref = {}
for i, r in p3["ref_norm"].items():
    if r:
        idx_ref.setdefault(r, []).append(i)

added = already = missing = 0

# ---------- 2. Boucle SSC ----------
for _, row in ssc.iterrows():
    ssc_id  = int(row.id)
    leaf_url = row.get("url")
    if not isinstance(leaf_url, str) or not leaf_url.startswith("http"):
        continue

    print(f"\n🔎 {row.nom}  ➜  {leaf_url}")
    for prod in scrape_catalog(leaf_url):
        # Match par URL
        idx = idx_url.get(prod["url"])
        # Sinon par référence
        if idx is None and prod["ref"]:
            for i in idx_ref.get(norm_ref(prod["ref"]), []):
                idx = i; break

        if idx is not None:
            if int(p3.at[idx, "sous_sous_categorie_id"] or 0) == ssc_id:
                already += 1
            else:
                p3.at[idx, "sous_sous_categorie_id"] = ssc_id
                added += 1
        else:
            missing += 1

print(f"\n✅ Terminé : {added} lignes mises à jour, "
      f"{already} déjà correctes, {missing} produits introuvables.")

# ---------- 3. Sauvegarde ----------
p3.drop(columns="ref_norm", inplace=True, errors="ignore")
with pd.ExcelWriter(EXCEL, mode="a", engine="openpyxl",
                    if_sheet_exists="replace") as w:
    p3.to_excel(w, index=False, sheet_name=SHEET3)
print("📁 Sheet3 sauvegardée.")
