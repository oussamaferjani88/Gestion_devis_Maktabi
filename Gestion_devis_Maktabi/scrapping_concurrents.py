import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import unidecode
from pathlib import Path

# ============== CONFIG ==============
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\concurrents_final.xlsx"
SHEET3 = "Sheet3"
SHEET4 = "Sheet4"
SHEET5 = "Sheet5"
SSC    = "SSC"

HEADERS = {"User-Agent": "Mozilla/5.0"}
REQ_TIMEOUT = 12
PAGE_SLEEP = 0.8
PRICE_TOLERANCE = 0.03

# ============== UTILS ==============
def slug(txt: str) -> str:
    txt = unidecode.unidecode(str(txt or "")).lower()
    txt = re.sub(r'[^a-z0-9 ]+', ' ', txt)
    return re.sub(r'\s+', ' ', txt).strip()

def price_float(txt):
    if not txt or pd.isna(txt):
        return None
    try:
        return float(re.sub(r'[^\d,.]', '', txt).replace(',', '.'))
    except:
        return None

def safe_get(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code == 200:
            return r
    except Exception:
        pass
    return None

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    needed = [
        "id", "nom", "sous_categorie_id",
        "reference_mytek", "mytek_avant_remise", "mytek_apres_remise", "url_mytek",
        "reference_tunisianet", "tunisianet_avant_remise", "tunisianet_apres_remise", "url_tunisianet"
    ]
    for col in needed:
        if col not in df.columns:
            df[col] = None

    if "disponibilite_mytek" not in df.columns:
        pos = df.columns.get_loc("mytek_apres_remise") + 1
        df.insert(pos, "disponibilite_mytek", None)

    if "disponibilite_tunisianet" not in df.columns:
        pos = df.columns.get_loc("tunisianet_apres_remise") + 1
        df.insert(pos, "disponibilite_tunisianet", None)

    return df

def parse_mytek_product(url):
    """
    Retourne: (prix_apres_remise, prix_avant_remise, dispo, ref)
    """
    r = safe_get(url)
    if r is None:
        return (None, None, "rupture de stock", None)
    soup = BeautifulSoup(r.text, "html.parser")

    # 1️⃣ Prix après remise (prix principal)
    price = None
    # Priorité: meta[itemprop="price"]
    price_meta = soup.select_one('meta[itemprop="price"]')
    if price_meta and price_meta.has_attr("content"):
        price = price_meta["content"]
    # Si pas, cibler le span id qui commence par "product-price-"
    if not price:
        price_span = soup.select_one('span[id^="product-price-"]')
        if price_span:
            # Essaye d'abord data-price-amount
            price = price_span.get("data-price-amount")
            # Si pas, prends le texte
            if not price:
                price_inner = price_span.select_one("span.price")
                price = price_inner.get_text(strip=True) if price_inner else price_span.get_text(strip=True)
    # En dernier recours: la méthode ancienne (moins fiable)
    if not price:
        price_el = soup.select_one(".price-box.price-final_price .special-price .price")
        price = price_el.get_text(strip=True) if price_el else None

    # 2️⃣ Prix avant remise (ancien prix, s'il existe)
    old_price = None
    old_price_span = soup.select_one(".price-box.price-final_price .old-price .price")
    if old_price_span:
        old_price = old_price_span.get_text(strip=True)
    # Sinon, old_price = price (pas de promo)
    if not old_price:
        old_price = price

    # 3️⃣ Référence produit (ne change pas)
    ref_el = soup.select_one("div.skuDesktop")
    ref = ref_el.get_text(strip=True).replace("[", "").replace("]", "") if ref_el else None

    # 4️⃣ Disponibilité (fiche technique)
    dispo = "non spécifiée"
    table = soup.find("table", id="product-attribute-specs-table")
    if table:
        for row in table.select("tbody tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue
            if "disponibilité" in th.text.strip().lower():
                dispo = td.text.strip()
                break

    return (price, old_price, dispo or "non spécifiée", ref)


def parse_tunisianet_product(url):
    """
    Retourne: (prix, dispo, ref)
    - Prix: span[itemprop="price"] (prioritaire), sinon .product-price-and-shipping .price
    - Disponibilité: #stock_availability span
    - Référence: span.product-reference
    """
    r = safe_get(url)
    if r is None:
        return (None, "rupture de stock", None)
    soup = BeautifulSoup(r.text, "html.parser")

    # Prix : priorité à <span itemprop="price">
    price_el = soup.select_one('span[itemprop="price"]')
    if price_el:
        price = price_el.get("content") or price_el.get_text(strip=True)
    else:
        price_el2 = soup.select_one(".product-price-and-shipping .price")
        price = price_el2.get_text(strip=True) if price_el2 else None

    stock_span = soup.select_one("#stock_availability span")
    dispo = stock_span.get_text(strip=True) if stock_span else "non spécifiée"

    ref_el = soup.select_one("span.product-reference")
    ref = ref_el.get_text(strip=True) if ref_el else None

    return (price, dispo, ref)

# ============== SCRAPE CATALOGUES (depuis SSC) ==============
def scrape_catalog_mytek(base_url, cat_name):
    out = []
    page = 1
    while True:
        url = base_url if page == 1 else f"{base_url}?p={page}"
        print(f"  🔎 Mytek: {cat_name} – page {page} → {url}")
        r = safe_get(url)
        if r is None:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("li.item.product.product-item")
        if not items:
            break

        for it in items:
            name_el = it.select_one("h2.product.name.product-item-name")
            name = name_el.get_text(strip=True) if name_el else None

            ref_el = it.select_one("div.skuDesktop")
            ref = ref_el.get_text(strip=True).replace("[", "").replace("]", "") if ref_el else None

            price_el = it.select_one('meta[itemprop="price"]')
            if price_el and price_el.has_attr("content"):
                price_txt = price_el["content"]
            else:
                price_eltag = it.select_one(".special-price .price") or it.select_one(".price")
                price_txt = price_eltag.get_text(strip=True) if price_eltag else None

            link_el = it.select_one("a.product.photo.product-item-photo")
            url_prod = link_el["href"] if link_el else None

            out.append({
                "source": "mytek",
                "nom": name,
                "url": url_prod,
                "prix_txt": price_txt,
                "ref": ref
            })
        page += 1
        time.sleep(PAGE_SLEEP)
    return out

def scrape_catalog_tunisianet(base_url, cat_name):
    out = []
    for page in range(1, 50):
        url = f"{base_url}?page={page}"
        print(f"  🔎 Tunisianet: {cat_name} – page {page} → {url}")
        r = safe_get(url)
        if r is None:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("article.product-miniature")
        if not items:
            break

        for it in items:
            name_el = it.select_one("h2.h3.product-title")
            ref_el = it.select_one("span.product-reference")

            # Prix : priorité à <span itemprop="price">
            price_el = it.select_one('span[itemprop="price"]')
            if price_el:
                price_txt = price_el.get("content") or price_el.get_text(strip=True)
            else:
                price_eltag = it.select_one(".product-price-and-shipping .price")
                price_txt = price_eltag.get_text(strip=True) if price_eltag else None

            link_el = it.select_one("a.product-thumbnail") or it.select_one("a.product-title")

            name = name_el.get_text(strip=True) if name_el else None
            ref = ref_el.get_text(strip=True) if ref_el else None
            url_prod = link_el["href"] if link_el else None

            out.append({
                "source": "tunisianet",
                "nom": name,
                "url": url_prod,
                "prix_txt": price_txt,
                "ref": ref
            })
        time.sleep(PAGE_SLEEP)
    return out

# ============== EAV HELPERS (inchangés) ==============
def build_attr_lookup(sheet4: pd.DataFrame):
    if sheet4.empty:
        return {}, 1
    lookup = {}
    for _, r in sheet4.iterrows():
        key = (int(r["sous_categorie_id"]), slug(r["nom"]))
        lookup[key] = (int(r["id"]), r["nom"])
    next_id = int(sheet4["id"].max()) + 1
    return lookup, next_id

def add_eav_for_product(sheet4, sheet5, sous_categorie_id, produit_id, source, product_url, attr_lookup, next_attr_id):
    r = safe_get(product_url)
    if r is None:
        return sheet4, sheet5, attr_lookup, next_attr_id

    soup = BeautifulSoup(r.text, "html.parser")
    new_attr_rows = []
    new_value_rows = []

    if source == "mytek":
        table = soup.find("table", id="product-attribute-specs-table")
        if table:
            for row in table.select("tbody tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                key_raw = th.text.strip()
                value = td.text.strip()
                if not value:
                    continue

                k = (int(sous_categorie_id), slug(key_raw))
                if k in attr_lookup:
                    attr_id = attr_lookup[k][0]
                else:
                    attr_id = next_attr_id
                    attr_lookup[k] = (attr_id, key_raw)
                    new_attr_rows.append({
                        "id": attr_id,
                        "nom": key_raw,
                        "sous_categorie_id": int(sous_categorie_id)
                    })
                    next_attr_id += 1

                new_value_rows.append({
                    "produit_id": int(produit_id),
                    "attribut_id": int(attr_id),
                    "valeur": value
                })

    else:  # tunisianet
        for dt in soup.select("section.product-features dl.data-sheet dt.name"):
            key_raw = dt.get_text(strip=True)
            dd = dt.find_next("dd", class_="value")
            value = dd.get_text(strip=True) if dd else None
            if not value:
                continue

            k = (int(sous_categorie_id), slug(key_raw))
            if k in attr_lookup:
                attr_id = attr_lookup[k][0]
            else:
                attr_id = next_attr_id
                attr_lookup[k] = (attr_id, key_raw)
                new_attr_rows.append({
                    "id": attr_id,
                    "nom": key_raw,
                    "sous_categorie_id": int(sous_categorie_id)
                })
                next_attr_id += 1

            new_value_rows.append({
                "produit_id": int(produit_id),
                "attribut_id": int(attr_id),
                "valeur": value
            })

        # Disponibilité magasin dn EAV (si tu veux aussi l'inscrire là)
        stock_div = soup.select_one("#stock_availability")
        if stock_div:
            stock_txt = stock_div.get_text(strip=True)
            key_raw = "Disponibilité magasin"
            k = (int(sous_categorie_id), slug(key_raw))
            if k in attr_lookup:
                attr_id = attr_lookup[k][0]
            else:
                attr_id = next_attr_id
                attr_lookup[k] = (attr_id, key_raw)
                new_attr_rows.append({
                    "id": attr_id,
                    "nom": key_raw,
                    "sous_categorie_id": int(sous_categorie_id)
                })
                next_attr_id += 1
            new_value_rows.append({
                "produit_id": int(produit_id),
                "attribut_id": int(attr_id),
                "valeur": stock_txt
            })

    if new_attr_rows:
        sheet4 = pd.concat([sheet4, pd.DataFrame(new_attr_rows)], ignore_index=True)
    if new_value_rows:
        sheet5 = pd.concat([sheet5, pd.DataFrame(new_value_rows)], ignore_index=True)

    return sheet4, sheet5, attr_lookup, next_attr_id

# ============== 1) MàJ des produits EXISTANTS (prix + dispo) ==============
def update_existing_products_prices_and_dispo(p3: pd.DataFrame) -> pd.DataFrame:
    for i, row in p3.iterrows():
        pid = row["id"]

        # Mytek
        url_m = row.get("url_mytek")
        if isinstance(url_m, str) and url_m.startswith("http"):
            print(f"🔄 Update (Mytek) pid={pid}")
            price, old_price, dispo, ref = parse_mytek_product(url_m)
            if price:
                p3.at[i, "mytek_apres_remise"] = price
            if old_price:
                p3.at[i, "mytek_avant_remise"] = old_price
            p3.at[i, "disponibilite_mytek"] = dispo
            if ref and not pd.notna(row.get("reference_mytek")):
                p3.at[i, "reference_mytek"] = ref

        # Tunisianet
        url_t = row.get("url_tunisianet")
        if isinstance(url_t, str) and url_t.startswith("http"):
            print(f"🔄 Update (Tunisianet) pid={pid}")
            price, dispo, ref = parse_tunisianet_product(url_t)
            if price:
                p3.at[i, "tunisianet_apres_remise"] = price
                p3.at[i, "tunisianet_avant_remise"] = price  # pas de distinction sur TN
            p3.at[i, "disponibilite_tunisianet"] = dispo
            if ref and not pd.notna(row.get("reference_tunisianet")):
                p3.at[i, "reference_tunisianet"] = ref

    return p3

# ============== 2) Découverte de NOUVEAUX produits via SSC ==============
def discover_new_products_from_ssc(p3: pd.DataFrame, p4: pd.DataFrame, p5: pd.DataFrame, ssc: pd.DataFrame):
    urls_mytek_exist = set(u for u in p3["url_mytek"].dropna().astype(str) if u.startswith("http"))
    urls_tun_exist   = set(u for u in p3["url_tunisianet"].dropna().astype(str) if u.startswith("http"))

    next_prod_id = int(p3["id"].max() or 0) + 1
    attr_lookup, next_attr_id = build_attr_lookup(p4)
    ssc_valid = ssc[ssc["url"].notna() & ssc["sous_categorie_id"].notna()]
    has_ssc_col = "sous_sous_categorie_id" in p3.columns

    for _, row in ssc_valid.iterrows():
        cat_name = row["nom"]
        sid = int(row["sous_categorie_id"])
        ssc_id = int(row["id"]) if "id" in row else None
        base_url = str(row["url"]).strip().lower()

        print(f"\n📚 SSC -> {cat_name} | sous_categorie_id={sid} | url={base_url}")

        if "tunisianet" in base_url:
            items = scrape_catalog_tunisianet(row["url"], cat_name)
        elif "mytek.tn" in base_url:
            items = scrape_catalog_mytek(row["url"], cat_name)
        else:
            print("  ⚠️ Domaine non géré, skip.")
            continue

        for it in items:
            prod_url = it["url"]
            if not prod_url or not prod_url.startswith("http"):
                continue

            is_mytek = (it["source"] == "mytek")

            if (is_mytek and prod_url in urls_mytek_exist) or ((not is_mytek) and prod_url in urls_tun_exist):
                continue  # déjà existant

            source_txt = "Mytek" if is_mytek else "Tunisianet"
            print(f"➕ Nouveau produit détecté ({source_txt}) : {prod_url}")

            # --- Scraping détaillé
            if is_mytek:
                price, old_price, dispo, ref = parse_mytek_product(prod_url)
            else:
                price, dispo, ref = parse_tunisianet_product(prod_url)
                old_price = price

            row_data = {
                "id": next_prod_id,
                "nom": it.get("nom"),
                "sous_categorie_id": sid,
                "reference_mytek": None,
                "mytek_avant_remise": None,
                "mytek_apres_remise": None,
                "url_mytek": None,
                "reference_tunisianet": None,
                "tunisianet_avant_remise": None,
                "tunisianet_apres_remise": None,
                "url_tunisianet": None,
                "disponibilite_mytek": None,
                "disponibilite_tunisianet": None
            }

            if has_ssc_col:
                row_data["sous_sous_categorie_id"] = ssc_id

            if is_mytek:
                row_data["reference_mytek"] = ref
                row_data["mytek_apres_remise"] = price or it.get("prix_txt")
                row_data["mytek_avant_remise"] = old_price or row_data["mytek_apres_remise"]
                row_data["url_mytek"] = prod_url
                row_data["disponibilite_mytek"] = dispo
            else:
                row_data["reference_tunisianet"] = ref
                row_data["tunisianet_apres_remise"] = price or it.get("prix_txt")
                row_data["tunisianet_avant_remise"] = old_price or row_data["tunisianet_apres_remise"]
                row_data["url_tunisianet"] = prod_url
                row_data["disponibilite_tunisianet"] = dispo

            p3 = pd.concat([p3, pd.DataFrame([row_data])], ignore_index=True)

            if is_mytek:
                urls_mytek_exist.add(prod_url)
            else:
                urls_tun_exist.add(prod_url)

            # EAV
            p4, p5, attr_lookup, next_attr_id = add_eav_for_product(
                p4, p5, sid, next_prod_id, "mytek" if is_mytek else "tunisianet",
                prod_url, attr_lookup, next_attr_id
            )

            next_prod_id += 1

    return p3, p4, p5

# ============== MAIN ==============
def main():
    xls_path = Path(EXCEL_PATH)
    if not xls_path.exists():
        raise FileNotFoundError(f"Excel introuvable: {EXCEL_PATH}")

    p3 = pd.read_excel(EXCEL_PATH, sheet_name=SHEET3)
    p4 = pd.read_excel(EXCEL_PATH, sheet_name=SHEET4)
    try:
        p5 = pd.read_excel(EXCEL_PATH, sheet_name=SHEET5)
    except ValueError:
        p5 = pd.DataFrame(columns=["produit_id", "attribut_id", "valeur"])
    ssc = pd.read_excel(EXCEL_PATH, sheet_name=SSC)

    p3 = ensure_columns(p3)

    print("\n=== Étape 1 : Mise à jour prix + disponibilités des produits existants ===")
    p3 = update_existing_products_prices_and_dispo(p3)

    print("\n=== Étape 2 : Découverte & insertion des nouveaux produits depuis SSC ===")
    p3, p4, p5 = discover_new_products_from_ssc(p3, p4, p5, ssc)

    print("\n💾 Sauvegarde dans l'Excel ...")
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        p3.to_excel(w, index=False, sheet_name=SHEET3)
        p4.to_excel(w, index=False, sheet_name=SHEET4)
        p5.to_excel(w, index=False, sheet_name=SHEET5)

    print("\n✅ Terminé !")

if __name__ == "__main__":
    main()
