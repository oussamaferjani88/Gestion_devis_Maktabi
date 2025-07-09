"""
map_tunisianet_ssc_to_sheet3.py
————————
Associe sous_sous_categorie_id aux produits Tunisianet dans Sheet3 :

✔️ 1) Pour chaque SSC avec url_tunisianet -> scrape tous les produits
    - Associe directement ssc.id à tous les produits trouvés (par URL ou ref)
✔️ 2) Pour les produits Tunisianet non associés -> fuzzy match du nom
    - Cherche meilleure sous_sous_categorie_id dans SSC (même sous_categorie_id)
✔️ Enregistre Sheet3 mis à jour
"""

import re, time, unidecode, requests, pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- CONFIG ----------
EXCEL      = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"
SHEET3     = "Sheet3"
SHEET_SSC  = "SSC"
TIMEOUT    = 20
PAUSE      = 0.8
MAX_PAGES  = 30
FUZZY_THR  = 80

# ---------- Session HTTP avec Retry ----------
retry = Retry(total=3, backoff_factor=1,
              status_forcelist=[500, 502, 503, 504],
              allowed_methods=["HEAD", "GET"])
sess = requests.Session()
sess.headers.update({"User-Agent": "Mozilla/5.0"})
sess.mount("https://", HTTPAdapter(max_retries=retry))
sess.mount("http://",  HTTPAdapter(max_retries=retry))

def slug(text):
    return re.sub(r'[^a-z0-9]+','-',unidecode.unidecode(text.lower())).strip('-')

def norm_ref(r):
    return re.sub(r'[^A-Z0-9]', '', str(r or '').upper())

def scrape_catalog(url_base):
    """Yield dict(url, ref, name) pour tous les produits sur une page catalogue."""
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
        items = soup.select("article.product-miniature")
        if not items:
            break

        for it in items:
            name = it.select_one("h2.h3.product-title")
            ref  = it.select_one("span.product-reference")
            link = it.select_one("a.product-thumbnail") or it.select_one("a.product-title")
            yield {
                "name": name.text.strip() if name else None,
                "ref" : norm_ref(ref.text) if ref else '',
                "url" : link["href"] if link else url,
            }
        time.sleep(PAUSE)

# ---------- 1. Charger Excel ----------
ssc = pd.read_excel(EXCEL, sheet_name=SHEET_SSC)
p3  = pd.read_excel(EXCEL, sheet_name=SHEET3)

# Index Sheet3
p3["ref_norm"] = p3["reference_tunisianet"].apply(norm_ref)
idx_url = {u: i for i, u in p3["url_tunisianet"].items() if isinstance(u, str)}
idx_ref = {}
for i, r in p3["ref_norm"].items():
    if r:
        idx_ref.setdefault(r, []).append(i)

# Préparer SSC pour fuzzy
ssc["slug"] = ssc["nom"].apply(slug)
ssc_by_sub = {}
for _, r in ssc.iterrows():
    ssc_by_sub.setdefault(int(r.sous_categorie_id), []).append((int(r.id), r.slug, r.nom))

added, already, fuzzy_matched, missing = 0, 0, 0, 0

# ---------- 2. Boucle SSC avec URL Tunisianet connue ----------
for _, row in ssc.iterrows():
    ssc_id  = int(row.id)
    leaf_url = row.get("url_tunisianet")
    if not isinstance(leaf_url, str) or not leaf_url.startswith("http"):
        continue

    print(f"\n🔎 {row.nom}  ➜  {leaf_url}")
    for prod in scrape_catalog(leaf_url):
        idx = idx_url.get(prod["url"])
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
            # Créer une nouvelle ligne si produit pas encore en Sheet3
            p3 = pd.concat([p3, pd.DataFrame([{
                "id": p3["id"].max()+1,
                "reference_mytek": None,
                "nom": prod["name"],
                "sous_categorie_id": int(row.sous_categorie_id),
                "sous_sous_categorie_id": ssc_id,
                "mytek_avant_remise": None,
                "mytek_apres_remise": None,
                "url_mytek": None,
                "reference_tunisianet": prod["ref"],
                "tunisianet_avant_remise": None,
                "tunisianet_apres_remise": None,
                "url_tunisianet": prod["url"],
                "ref_norm": prod["ref"]
            }])], ignore_index=True)
            added +=1

# ---------- 3. Fallback fuzzy : produits non associés ----------
print("\n✨ Fallback : Fuzzy matching pour les produits sans SSC.")
for idx, row in p3[p3["sous_sous_categorie_id"].isna() | (p3["sous_sous_categorie_id"]==0)].iterrows():
    nom_prod = row.nom
    sous_cat = row.sous_categorie_id
    if not isinstance(nom_prod, str) or sous_cat not in ssc_by_sub:
        missing += 1
        continue

    # Matching SSC dans même sous_categorie_id
    best_id, best_score = 0, 0
    for sid, sslug, snom in ssc_by_sub[sous_cat]:
        sc = fuzz.partial_ratio(nom_prod.lower(), snom.lower())
        if sc > best_score:
            best_score = sc
            best_id = sid

    if best_score >= FUZZY_THR:
        p3.at[idx, "sous_sous_categorie_id"] = best_id
        fuzzy_matched += 1
        print(f"✅ Fuzzy: {nom_prod} -> {best_id} ({best_score}%)")
    else:
        missing +=1

# ---------- 4. Sauvegarde ----------
p3.drop(columns="ref_norm", inplace=True, errors="ignore")
with pd.ExcelWriter(EXCEL, mode="a", engine="openpyxl",
                    if_sheet_exists="replace") as w:
    p3.to_excel(w, index=False, sheet_name=SHEET3)

print(f"\n✅ Terminé : {added} directs, {already} déjà corrects, "
      f"{fuzzy_matched} fuzzy, {missing} toujours sans match.")
