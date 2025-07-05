import requests, re, time, math
from bs4 import BeautifulSoup
import pandas as pd
from rapidfuzz import fuzz
import unidecode

# ============= PARAMÈTRES =====================
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test4.xlsx"
SHEET_NAME   = "Sheet3"
TUNISIANAET_BASE = "https://www.tunisianet.com.tn/316-imprimante-en-tunisie"
MAX_PAGES_TUN   = 2              # <-- on ne scrape que 2 pages (test)
SOUS_CAT_ID_IMPR = 1             # id de la sous-catégorie Imprimantes (Mytek)
HEADERS = {'User-Agent': 'Mozilla/5.0'}
PRICE_TOLERANCE = 0.03           # ±3 % pour comparer les prix
# ==============================================

# ----------  UTILS ----------------------------
def clean_ref(ref: str) -> str:
    """Minuscule, accents retirés, tirets/espaces supprimés, alphanum uniquement."""
    if not ref or pd.isnull(ref): 
        return ""
    ref = unidecode.unidecode(ref).lower()
    return re.sub(r'[^a-z0-9]', '', ref)

def price_float(price_txt: str | None) -> float | None:
    if not price_txt: 
        return None
    p = re.sub(r'[^\d,.]', '', price_txt).replace(',', '.')
    try:   return float(p)
    except: return None

def is_price_close(p1, p2, tol=PRICE_TOLERANCE) -> bool:
    if p1 is None or p2 is None: 
        return False
    return abs(p1 - p2) / max(p1, p2) <= tol

def is_ref_match(ref1, ref2) -> bool:
    if not ref1 or not ref2: 
        return False
    if ref1 in ref2 or ref2 in ref1: 
        return True
    return fuzz.partial_ratio(ref1, ref2) >= 85
# ---------------------------------------------

# ---------- 1. SCRAPE TUNISIANET -------------
def scrape_tunisianet(max_pages=2):
    records = []
    for page in range(1, max_pages + 1):
        url = f"{TUNISIANAET_BASE}?page={page}&order=product.price.asc"
        print(f"Scraping Tunisianet page {page}: {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            print("  ❌  erreur", e); break

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("article.product-miniature")
        if not items: 
            print("  ⚠️  aucun item trouvé"); break

        for it in items:
            name  = it.select_one("h2.h3.product-title")
            ref   = it.select_one("span.product-reference")
            price = it.select_one(".product-price-and-shipping .price")
            link  = it.select_one("a.product-thumbnail") or it.select_one("a.product-title")
            records.append({
                "nom"                 : name.text.strip()  if name  else None,
                "reference_tun"       : ref.text.strip()   if ref   else None,
                "prix_tun"            : price.text.strip() if price else None,
                "url_tunisianet"      : link["href"]       if link  else None
            })
        time.sleep(1)
    return pd.DataFrame(records)
# ---------------------------------------------

# ---------- 2. CHARGER SHEET3 MYTEK ----------
mytek_df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
# Renommages clairs
mytek_df = mytek_df.rename(columns={
    "reference"        : "reference mytek",
    "prix_avant_remise": "mytek_avant_remise",
    "prix_apres_remise": "mytek_apres_remise",
    "url"              : "url mytek"
})
# Ajout colonnes vides si manquantes
for col in ["reference_tunisianet","tunisianet_avant_remise","tunisianet_apres_remise","url_tunisianet"]:
    if col not in mytek_df.columns: mytek_df[col] = None

# Nettoyage référence Mytek
mytek_df["ref_clean"] = mytek_df["reference mytek"].apply(clean_ref)
# Index rapide par ref_clean pour matcher
ref_to_idx = {r:i for i,r in mytek_df[["ref_clean"]].itertuples() if r}

# ---------- 3. SCRAPER TUNISIANET + MATCH ---
tun_df = scrape_tunisianet(MAX_PAGES_TUN)
tun_df["ref_clean"] = tun_df["reference_tun"].apply(clean_ref)
tun_df["prix_float"] = tun_df["prix_tun"].apply(price_float)

used_idx = set()
next_id  = int(mytek_df["id"].max() or 0) + 1
new_rows = []

for _, row in tun_df.iterrows():
    ref_c   = row["ref_clean"]
    price_c = row["prix_float"]
    match_i = None

    # 1) Match exact/dedans/déjà propre
    if ref_c in ref_to_idx:
        match_i = ref_to_idx[ref_c]
    else:
        # 2) Fuzzy / substring loop
        for idx, ref_m in ref_to_idx.items():
            pass  # placeholder

    # 2) boucle manuelle si non trouvé, plus permissive
    if match_i is None:
        for idx, ref_m in mytek_df["ref_clean"].items():
            if is_ref_match(ref_c, ref_m):
                # Vérifier prix si dispo
                if is_price_close(price_c, price_float(mytek_df.at[idx,"mytek_apres_remise"])):
                    match_i = idx
                    break

    # ---------   (a) Trouvé -> enrichir   ----------
    if match_i is not None:
        if match_i in used_idx:  # déjà enrichi
            continue
        used_idx.add(match_i)
        mytek_df.at[match_i,"reference_tunisianet"]          = row["reference_tun"]
        mytek_df.at[match_i,"tunisianet_avant_remise"]       = row["prix_tun"]
        mytek_df.at[match_i,"tunisianet_apres_remise"]       = row["prix_tun"]
        mytek_df.at[match_i,"url_tunisianet"]                = row["url_tunisianet"]
    # ---------   (b) Non trouvé -> nouveau produit ----
    else:
        new_rows.append({
            "id"                           : next_id,
            "reference_mytek"              : None,
            "reference_tunisianet"         : row["reference_tun"],
            "nom"                          : row["nom"],
            "sous_categorie_id"            : SOUS_CAT_ID_IMPR,
            "mytek_avant_remise"           : None,
            "mytek_apres_remise"           : None,
            "tunisianet_avant_remise"      : row["prix_tun"],
            "tunisianet_apres_remise"      : row["prix_tun"],
            "url_mytek"                    : None,
            "url_tunisianet"               : row["url_tunisianet"],
            "ref_clean"                    : ref_c
        })
        next_id += 1

# ---------- 4. CONCAT & SAVE -----------------
if new_rows:
    mytek_df = pd.concat([mytek_df, pd.DataFrame(new_rows)], ignore_index=True)

# Supprimer la colonne technique ref_clean
mytek_df = mytek_df.drop(columns=["ref_clean"], errors="ignore")

with pd.ExcelWriter(EXCEL_PATH, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
    mytek_df.to_excel(writer, index=False, sheet_name=SHEET_NAME)

print("✅ Sheet3 mise à jour avec Mytek + Tunisianet (2 pages test).")
