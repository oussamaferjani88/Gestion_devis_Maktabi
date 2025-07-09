"""
auto_fill_url_tunisianet.py
—————————————
• Pour chaque sous_sous_categorie du fichier test9.xlsx (feuille SSC) :
    – construit un slug du nom ;
    – lance une recherche Tunisianet (param ?search_query=slug) ;
    – récupère dans le premier résultat la breadcrumb « <a href="/ID-slug_base"> » ;
    – en déduit l’URL catalogue : https://www.tunisianet.com.tn/ID-slug_base
    – vérifie HEAD 200 → écrit url_tunisianet ; sinon laisse vide.
• Rien d’autre n’est modifié.
"""

import re, unidecode, requests, pandas as pd, time
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EXCEL      = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"
SSC_SHEET  = "SSC"
BASE_TN    = "https://www.tunisianet.com.tn/"
UA         = {"User-Agent": "Mozilla/5.0"}
TIMEOUT    = 10
SLEEP      = 0.6

def slug(txt: str) -> str:
    return re.sub(r'[^a-z0-9]+','-',unidecode.unidecode(txt.lower())).strip('-')

# Session HTTP robuste
sess = requests.Session()
sess.headers.update(UA)
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3,backoff_factor=1)))

# 1. Charger SSC
ssc = pd.read_excel(EXCEL, sheet_name=SSC_SHEET)
if "url_tunisianet" not in ssc.columns:
    ssc["url_tunisianet"] = ""

added = 0
for idx, row in ssc.iterrows():
    if pd.notna(row.url_tunisianet) and row.url_tunisianet:
        continue                     # déjà rempli

    q = slug(row.nom)
    search_url = f"{BASE_TN}recherche?search_query={q}"
    try:
        resp = sess.get(search_url, timeout=TIMEOUT)
    except requests.RequestException:
        continue

    soup = BeautifulSoup(resp.text, "html.parser")
    first_prod = soup.select_one("article.product-miniature a.product-thumbnail")
    if not first_prod:
        continue

    # Suivre le lien produit puis lire son breadcrumb
    prod_url = first_prod["href"]
    try:
        prod_html = sess.get(prod_url, timeout=TIMEOUT).text
    except requests.RequestException:
        continue
    prod_soup = BeautifulSoup(prod_html, "html.parser")
    crumb = prod_soup.select_one("nav.breadcrumb a[href*='-']")
    if not crumb:
        continue

    catalogue = crumb["href"]
    if not catalogue.startswith("http"):
        catalogue = BASE_TN.rstrip('/') + catalogue
    # Vérifier HEAD
    try:
        if sess.head(catalogue, timeout=TIMEOUT).status_code == 200:
            ssc.at[idx, "url_tunisianet"] = catalogue
            added += 1
            print("✅", row.nom, "→", catalogue)
    except requests.RequestException:
        pass

    time.sleep(SLEEP)

# 2. Sauvegarder
with pd.ExcelWriter(EXCEL, engine="openpyxl", mode="a",
                    if_sheet_exists="replace") as w:
    ssc.to_excel(w, index=False, sheet_name=SSC_SHEET)

print(f"\n✅ Colonne 'url_tunisianet' complétée pour {added} lignes.")
