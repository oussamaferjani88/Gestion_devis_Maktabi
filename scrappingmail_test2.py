#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script d'extraction flexible d'emails "Codis" et fusion avec votre catalogue Excel.
Corrections :
 - Utilisation de raw string pour les regex (plus de invalid escape)
 - Remplacement de applymap (déconseillé) par strip sur colonnes object
 - Reindexation pour garantir exactement les colonnes std_cols et éviter InvalidIndexError
"""

import imaplib
import email
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import re
import unidecode

# ========================
# 1️⃣ Connexion IMAP
# ========================
IMAP_SERVER    = 'imap.gmail.com'
EMAIL_ACCOUNT  = 'hjaiejnessim@gmail.com'
EMAIL_PASSWORD = 'chpr cjbp uuyr kbic'

mail = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
mail.select('INBOX')

# ========================
# 2️⃣ Recherche d'emails 
# ========================

def normalize_text(text):
    if not isinstance(text, str):
        return ""
    return unidecode.unidecode(text).lower().replace(" ", "").replace("_", "").strip()


senders_raw = ["maktabi2013@gmail.com", "Hanene.BOUAZZA@codis.com.tn"]
subjects_raw = ["Codis", "Lenovo"]

senders_normalized = [normalize_text(s) for s in senders_raw]
subjects_normalized = [normalize_text(s) for s in subjects_raw]

all_ids = set()

# Recherche par FROM
for sender in senders_raw:
    result, data = mail.search(None, f'FROM "{sender}"')
    ids = set(data[0].split())
    all_ids.update(ids)

# Recherche par SUBJECT
for subj in subjects_raw:
    result, data = mail.search(None, f'SUBJECT "{subj}"')
    ids = set(data[0].split())
    all_ids.update(ids)

email_ids = list(all_ids)

if not email_ids:
    print("❌ Aucun email trouvé avec FROM ou SUBJECT parmi vos critères.")
    exit()

print(f"✅ {len(email_ids)} emails trouvés (critères multiples FROM/SUBJECT).")


# ========================
# 3️⃣ Utils : normalization & mapping
# ========================
def normalize_col(col):
    return unidecode.unidecode(str(col)).lower().replace(" ", "").replace("_", "").strip()

COLUMN_MAP = {
    'REF': [
        'ref', 'référence', 'reference', 'réference', 'code', 'sku', 'codearticle',
        'refprod', 'refproduit', 'productcode', 'product_ref', 'ref produit'
    ],

    'FAMILLE': [
        'famille', 'categorie', 'catégorie', 'family', 'productfamily', 'categorieproduit',
        'familleproduit', 'fam', 'cat', 'groupe' ,'designation' , 'nom' ,'nom_prod' ,'nom_produit' 
    ],

    'DESCRIPTION': [
        'description', 'designation', 'désignation', 'descr', 'libelle', 'nom', 'productname',
        'desc', 'titre', 'designationproduit', 'libellé'
    ],

    'Prix HT': [
        'prix', 'prixht', 'prix_unitaire', 'prixunitaire', 'prixunitaireht', 'price',
        'unitprice', 'unit_price', 'prix unitaire ht', 'puht', 'prixhtva'
    ],

    'Disponibilité': [
        'disponibilite', 'disponibilité', 'stock', 'dispo', 'availability', 'etat',
        'etatstock', 'stockstatus', 'status', 'availabilitystatus', 'disponible' ,'QTE LIMITEE' , 'quantité limitée' , 'qte limitée',
        'rupture', 'enstock', 'en stock', 'enrupture', 'en rupture', 'enrupturede stock', 'en rupture de stock', 'enrupturedestock', 'en rupture de stock'
    ]
}


def guess_column_roles(df_piece):
    mapping = {}
    norm_cols = {normalize_col(c): c for c in df_piece.columns}
    # map by name variants
    for standard, variants in COLUMN_MAP.items():
        for v in variants:
            nv = normalize_col(v)
            if nv in norm_cols:
                mapping[norm_cols[nv]] = standard
    for col in df_piece.columns:
    # Si déjà mappée à partir des noms connus
     if col in mapping:
        continue

    # Préparation des données : dropna + conversion en str
    vals = df_piece[col].dropna().astype(str).str.strip()

    if vals.empty:
        mapping[col] = 'DESCRIPTION'
        return mapping

    # Heuristique : proportion de numériques purs ou décimaux
    num_pattern = r'^\d+(\.\d+)?$'
    num_ratio = vals.str.match(num_pattern, na=False).mean()

    # Heuristique : présence de mots-clés stock/dispo
    stock_keywords = r'\bstock\b|\bdispo\b|\brupture\b|qte\s*limitee|non\s*dispo'

    stock_ratio = vals.str.contains(stock_keywords, case=False, na=False).mean()

    # Heuristique : proportion de descriptions longues
    long_ratio = (vals.str.len() > 30).mean()

    # Décision basée sur les patterns
    if stock_ratio > 0.3:
        mapping[col] = 'Disponibilité'

    elif num_ratio > 0.7:
        # Raffinons la distinction entre PRIX et REF
        prix_pattern = r'^\d+(\.\d+)?(\s*(HT|DT|TND|EUR|USD))?$'
        ref_pattern = r'^(?=.*[A-Za-z])(?=.*\d)'

        prix_matches = vals.str.match(prix_pattern, na=False).mean()
        ref_matches = vals.str.match(ref_pattern, na=False).mean()

        if prix_matches > 0.5:
            mapping[col] = 'Prix HT'
        elif ref_matches > 0.5:
            mapping[col] = 'REF'
        else:
            # Fallback : majorité numérique mais pas clair
            mapping[col] = 'Prix HT'

    elif long_ratio > 0.4:
        mapping[col] = 'DESCRIPTION'

    else:
        # Par défaut → description
        mapping[col] = 'DESCRIPTION'
    return mapping



def standardize_price_string(raw):
    if pd.isna(raw):
        return pd.NA
    raw = str(raw).strip().replace(' ', '')
    if raw == '':
        return pd.NA
    raw = raw.replace(',', '.')
    try:
        val = float(raw)
        return "{:,.3f}".format(val).replace('.', ',')
    except:
        return pd.NA


# ========================
# 4️⃣ Extraction & parsing
# ========================
df_list = []
for email_id in email_ids:
    _, msg_data = mail.fetch(email_id, '(RFC822)')
    msg = email.message_from_bytes(msg_data[0][1])
    # extract HTML body
    html_content = None
    for part in msg.walk():
        if part.get_content_type() == 'text/html':
            html_content = part.get_payload(decode=True).decode(errors='ignore')
            break
    if not html_content:
        continue

    soup = BeautifulSoup(html_content, 'html.parser')
    for table in soup.find_all('table'):
        html_str = str(table)
        try:
            df_piece = pd.read_html(StringIO(html_str), header=0)[0]
        except:
            df_piece = pd.read_html(StringIO(html_str), header=None)[0]
            df_piece.columns = [f'COL{i+1}' for i in range(df_piece.shape[1])]
        # detect real header row
        first = df_piece.iloc[0].astype(str)
        if all(~first.str.match(r'^\d', na=False)):
            df_piece.columns = first
            df_piece = df_piece[1:].reset_index(drop=True)

        # map & standardize
        col_map = guess_column_roles(df_piece)
        df_piece.rename(columns=col_map, inplace=True)

        # ------------------------------------------------------------------
        #  Standardiser les colonnes et éliminer les doublons ↴
        # ------------------------------------------------------------------
        std_cols = ['REF', 'FAMILLE', 'DESCRIPTION', 'Prix HT', 'Disponibilité']

        # 1️⃣  Supprimer les doublons de nom de colonne en gardant la 1ʳᵉ occurrence
        df_piece = df_piece.loc[:, ~df_piece.columns.duplicated()]

        # 2️⃣  Re-indexer pour obtenir exactement std_cols (ajoute les manquantes, drop les autres)
        df_piece = df_piece.reindex(columns=std_cols, fill_value=pd.NA)
        # ------------------------------------------------------------------


        # strip strings on object cols
        for c in df_piece.select_dtypes(include=['object','string']).columns:
            df_piece[c] = df_piece[c].str.strip()

        # clean price with raw string regex
        df_piece['Prix HT'] = (
            df_piece['Prix HT']
            .astype(str)
            .str.replace(r'[^\d\.]', '', regex=True)
        )
        df_piece['Prix HT'] = pd.to_numeric(df_piece['Prix HT'], errors='coerce')

        

        # unify availability
        df_piece['Disponibilité'] = df_piece['Disponibilité'].fillna('').str.upper().str.strip()

        df_list.append(df_piece)

if not df_list:
    print("❌ Aucun tableau extrait.")
    exit()

df_codis = pd.concat(df_list, ignore_index=True)


print(f"✅ {len(df_codis)} lignes extraites de tous les emails.")

df_codis['Prix HT'] = df_codis['Prix HT'].apply(standardize_price_string)



# Nettoyer les valeurs incorrectes de disponibilité
def clean_dispo(val):
    val = str(val).upper().strip()
    if val not in ['EN STOCK', 'RUPTURE', 'NON DISPO', 'QTE LIMITEE']:
        return pd.NA
    return val

df_codis['Disponibilité'] = df_codis['Disponibilité'].apply(clean_dispo)

# ========================
# 5️⃣ Préparation fusion
# ========================
df_codis.rename(columns={
    'REF': 'reference_codis',
    'Prix HT': 'prix_codis',
    'Disponibilité': 'disponibilite_codis',
    'FAMILLE': 'famille_codis'
}, inplace=True)

# ✅ On enlève les doublons sur la bonne colonne
if 'reference_codis' in df_codis.columns:
    df_codis = df_codis.drop_duplicates(subset='reference_codis')
else:
    print("⚠️ Attention : la colonne 'reference_codis' est absente après le renommage.")


# ========================
# 6️⃣ Charger catalogue
# ========================
df_table = pd.read_excel('test10.xlsx', sheet_name='Sheet3')

# ========================
# 7️⃣ Nettoyer refs
# ========================
def clean_ref(ref):
    if isinstance(ref, str):
        return re.sub(r'[^A-Za-z0-9]', '', ref).upper()
    return ref

for col in ['reference_mytek', 'reference_tunisianet']:
    if col in df_table.columns:
        df_table[col] = df_table[col].apply(clean_ref)
df_codis['reference_codis'] = df_codis['reference_codis'].apply(clean_ref)
# ------------------------------------------------------------------
# 8️⃣ Fusion enrichissante : TOUS les produits conservés
# ------------------------------------------------------------------

# 1) DataFrame unique avec les infos Codis
codis_unique = (
    df_codis[['reference_codis', 'prix_codis', 'disponibilite_codis']]
    .drop_duplicates('reference_codis')
    .copy()
)

# 2) Nettoyer les références dans df_table
for col in ['reference_mytek', 'reference_tunisianet']:
    if col in df_table.columns:
        df_table[col] = df_table[col].apply(clean_ref)

# 3) S’assurer que les colonnes cible existent
for col in ['prix_codis', 'disponibilite_codis']:
    if col not in df_table.columns:
        df_table[col] = pd.NA

# 4) Copie de travail
merged = df_table.copy()

# ---------- Enrichissement via reference_mytek ----------
tmp = codis_unique.rename(columns={'reference_codis': 'reference_mytek'})
merged = merged.merge(tmp, on='reference_mytek', how='left', suffixes=('', '_new'))
for col in ['prix_codis', 'disponibilite_codis']:
    merged[col] = merged[col].combine_first(merged[col + '_new'])
    merged.drop(columns=[col + '_new'], inplace=True)

# ---------- Enrichissement via reference_tunisianet ----------
tmp = codis_unique.rename(columns={'reference_codis': 'reference_tunisianet'})
merged = merged.merge(tmp, on='reference_tunisianet', how='left', suffixes=('', '_new'))
for col in ['prix_codis', 'disponibilite_codis']:
    merged[col] = merged[col].combine_first(merged[col + '_new'])
    merged.drop(columns=[col + '_new'], inplace=True)

# ------------------------------------------------------------------
# 9️⃣ Préserver l’historique – ne changer que si valeur différente
# ------------------------------------------------------------------
def keep_updated(new_val, old_val):
    if pd.notna(new_val):
        if pd.isna(old_val) or new_val != old_val:
            return new_val
    return old_val

for col in ['prix_codis', 'disponibilite_codis']:
    if col in df_table.columns:
        merged[col] = merged.apply(
            lambda r: keep_updated(r[col], df_table.at[r.name, col]),
            axis=1
        )

# ------------------------------------------------------------------
# 🔟 Ajouter les nouveaux produits Codis absents du catalogue
# ------------------------------------------------------------------
known_refs = set(df_table['reference_mytek'].dropna()) | set(df_table['reference_tunisianet'].dropna())
codis_refs = set(df_codis['reference_codis'].dropna())
new_refs   = codis_refs - known_refs

print(f"✅ Nouveaux produits Codis non présents dans le catalogue initial : {len(new_refs)}")

if new_refs:
    newp = df_codis[df_codis['reference_codis'].isin(new_refs)].copy()

    # --- nom du produit ---
    def choose_nom(row):
        if pd.notna(row.get('famille_codis')) and str(row['famille_codis']).strip():
            return row['famille_codis']
        if pd.notna(row.get('DESCRIPTION')) and str(row['DESCRIPTION']).strip():
            return row['DESCRIPTION']
        return pd.NA
    newp['nom'] = newp.apply(choose_nom, axis=1)

    newp['reference_officielle'] = newp['reference_codis']

    # --- sous_categorie_id ---
    def find_cat(name):
        if not isinstance(name, str) or not name.strip():
            return -1
        escaped = re.escape(name.strip())
        matches = df_table.loc[
            df_table['nom'].dropna().str.contains(escaped, case=False, na=False)
        ]
        if not matches.empty:
            return matches['sous_categorie_id'].mode().iloc[0]
        return -1
    newp['sous_categorie_id'] = newp['nom'].apply(find_cat)

    # --- id unique ---
    max_id = df_table['id'].max() if 'id' in df_table.columns else 0
    max_id = 0 if pd.isna(max_id) else int(max_id)
    new_ids = range(max_id + 1, max_id + 1 + len(newp))
    newp['id'] = list(new_ids)
    print(f"✅ IDs attribués aux nouveaux produits : {list(new_ids)}")

    # --- aligner les colonnes ---
    for col in merged.columns:
        if col not in newp.columns:
            newp[col] = pd.NA
    newp = newp[merged.columns]

    merged = pd.concat([merged, newp], ignore_index=True)

# ------------------------------------------------------------------
# 1️⃣1️⃣ Réorganisation & export Excel
# ------------------------------------------------------------------
cols = merged.columns.tolist()
if {'nom', 'prix_codis', 'disponibilite_codis'}.issubset(cols):
    idx_nom = cols.index('nom')
    for c in ['prix_codis', 'disponibilite_codis']:
        cols.remove(c)
    cols = cols[:idx_nom + 1] + ['prix_codis', 'disponibilite_codis'] + cols[idx_nom + 1:]
    merged = merged[cols]

# Retirer colonnes techniques inutiles
for col in ['famille_codis', 'DESCRIPTION']:
    if col in merged.columns:
        merged.drop(columns=col, inplace=True)

merged.to_excel('table_modifiee.xlsx', index=False)
print("✅ Export final généré : table_modifiee.xlsx")
