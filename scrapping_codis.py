import imaplib
import email
import unicodedata
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import re
import unidecode    
# ========================
# 0 Utilitaires
# ========================
COLUMN_MAP = {
    'REF': [
        'ref', 'référence', 'reference', 'réference', 'code', 'sku',
        'codearticle', 'refprod', 'refproduit', 'productcode', 'product_ref',
        'refproduit', 'referenceproduit', 'ref produit'
    ],

    'FAMILLE': [
        'famille', 'categorie', 'catégorie', 'category', 'family',
        'productfamily', 'categorieproduit', 'familleproduit', 'fam',
        'cat', 'groupe', 'designation', 'nom', 'nomproduit', 'nom_prod',
        'designationproduit', 'libelle', 'libellé'
    ],

    'DESCRIPTION': [
        'description', 'designation', 'désignation', 'descr', 'desc',
        'titre', 'productname', 'designationproduit', 'libelle', 'libellé'
    ],

    'Prix HT': [
        'prix', 'prixht', 'prix_unitaire', 'prixunitaire', 'prixunitaireht',
        'price', 'unitprice', 'unit_price', 'prixunitairehtva', 'puht',
        'prixhtva', 'prixht'
    ],

    'Disponibilité': [
        'disponibilite', 'disponibilité', 'stock',  'availability',
        'etat', 'etatstock', 'stockstatus', 'status', 'availabilitystatus'
    ]
}
def normalize_text(text):
    """
    Normalize text to compare header candidates.
    - Lowercase
    - Remove accents
    - Remove spaces
    - Remove punctuation
    """
    if not isinstance(text, str):
        return ""
    text = unidecode.unidecode(text).lower()
    text = re.sub(r'\s+', '', text)
    text = re.sub(r'[^a-z0-9]', '', text)
    return text.strip()
# Create a set of all normalized header variants from COLUMN_MAP
COLUMN_KEYWORDS = set()
for variants in COLUMN_MAP.values():
    COLUMN_KEYWORDS.update([normalize_text(v) for v in variants])

def format_codis_price_clean(df):
    def transform(val):
        if pd.isna(val):
            return pd.NA
        try:
            # Supprimer tous les caractères sauf chiffres, points ou virgules
            val = re.sub(r'[^0-9,\.]', '', str(val))

            # Normaliser virgule en point
            val = val.replace(',', '.')

            # Convertir en float, puis prendre partie entière
            num = float(val)
            return str(int(num)) + ',000'

        except:
            return pd.NA

    df['Prix HT'] = df['Prix HT'].apply(transform)
    return df


# ========================
# 1️⃣ Connexion IMAP
# ========================
IMAP_SERVER = 'imap.gmail.com'
EMAIL_ACCOUNT = 'hjaiejnessim@gmail.com'
EMAIL_PASSWORD = 'chpr cjbp uuyr kbic'

mail = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
mail.select('INBOX')

# ========================
# 2️⃣ Recherche d'emails spécifiques
# ========================
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
    print("❌ Aucun email trouvé avec ce critère.")
    exit()


print("✅ Email trouvé et chargé.")

# ========================
# 3️⃣ Extraire le corps HTML
# ========================
df_list = []

for email_idx, email_id in enumerate(email_ids, start=1):
    print(f"\n📥 Processing email {email_idx}/{len(email_ids)} ID: {email_id.decode()}")

    # ========================
    # Fetch this email
    # ========================
    result, msg_data = mail.fetch(email_id, '(RFC822)')
    msg = email.message_from_bytes(msg_data[0][1])

    # ========================
    # Extract HTML content
    # ========================
    html_content = None
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            html_content = part.get_payload(decode=True).decode(errors='ignore')
            break

    if not html_content:
        print("❌ Aucun contenu HTML trouvé dans cet email.")
        continue

    print("✅ Contenu HTML extrait.")

    # ========================
    # Parse all tables in this email
    # ========================
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')

    if not tables:
        print("❌ Aucun tableau trouvé dans cet email.")
        continue

    print(f"✅ {len(tables)} tableau(x) trouvé(s) dans cet email.")

    for idx, table in enumerate(tables):
        try:
            html_str = str(table)
            # Always read WITHOUT header
            df_raw = pd.read_html(StringIO(html_str), header=None)[0]

            # Check first row
            first_row = df_raw.iloc[0].astype(str).apply(normalize_text).tolist()
            has_header = any(cell in COLUMN_KEYWORDS for cell in first_row)

            if has_header:
                # ✅ First row is header
                df_raw.columns = df_raw.iloc[0].astype(str).str.strip()
                df_piece = df_raw[1:].reset_index(drop=True)
                print(f"✅ Header detected in first row for table {idx + 1}.")
            else:
                # ❌ No header → generic columns
                df_raw.columns = [f'COL{i+1}' for i in range(df_raw.shape[1])]
                df_piece = df_raw
                print(f"⚠️ No header detected in first row for table {idx + 1}. Using generic columns.")

            # Clean column names
            df_piece.columns = [str(c).strip() for c in df_piece.columns]
            # Nettoyage des caractères invisibles dans les cellules
            # Nettoyage des espaces invisibles unicode (non-breaking spaces, etc.)
            df_piece = df_piece.applymap(
                 lambda x: ''.join(
                    c for c in unicodedata.normalize('NFKC', str(x))
                        if not unicodedata.category(c).startswith('Z')
                 ).strip() if isinstance(x, str) else x
                )
            # Standardize or split wide tables
            if len(df_piece.columns) == 5:
                df_piece.columns = ["REF", "FAMILLE", "DESCRIPTION", "Prix HT", "Disponibilité"]
                df_list.append(df_piece)
            elif len(df_piece.columns) == 3:
                df_piece.columns = ["REF", "FAMILLE", "Prix HT"]
                df_list.append(df_piece)
            elif len(df_piece.columns) == 4:
                df_piece.columns = ["REF", "FAMILLE", "DESCRIPTION", "Prix HT"]
                df_list.append(df_piece)
            elif len(df_piece.columns) == 2:
                df_piece.columns = ["REF", "Prix HT"]
                df_list.append(df_piece)
            elif len(df_piece.columns) > 5:
                df_piece = df_piece.dropna(axis=1, how='all')
                num_all_columns = df_piece.shape[1]
                split_index = num_all_columns // 2
                df_left = df_piece.iloc[:, :split_index]
                df_right = df_piece.iloc[:, split_index:]
                df_left.columns = ["REF", "FAMILLE",  "Prix HT"]
                df_right.columns = ["REF", "FAMILLE", "Prix HT"]
                
                df_list.append(df_left)
                df_list.append(df_right)

            print(f"✅ Tableau {idx + 1} extrait et standardisé ({df_piece.shape[0]} lignes).")

        except Exception as e:
            print(f"⚠️ Erreur sur le tableau {idx + 1} de l'email {email_idx}: {e}")


if not df_list:
    print("❌ Aucun tableau valide trouvé .")
    exit()

df_codis = pd.concat(df_list, ignore_index=True)
df_codis = format_codis_price_clean(df_codis)
df_codis.to_excel('codis_extracted_data.xlsx', index=False)
print(f"✅ Total lignes combinées : {len(df_codis)}")

# ========================
# 5️⃣ Nettoyage des données fournisseur
# ========================

df_codis.columns = [col.strip() for col in df_codis.columns]
df_codis = df_codis.applymap(lambda x: x.strip() if isinstance(x, str) else x)


df_codis['Disponibilité'] = df_codis['Disponibilité'].str.strip().str.upper()

# ✅ Conserver aussi la colonne FAMILLE
df_codis = df_codis[['REF', 'FAMILLE', 'Prix HT', 'Disponibilité']].dropna(subset=['REF'])
df_codis = df_codis.rename(columns={
    'REF': 'reference_mytek',
    'Prix HT': 'prix_codis',
    'Disponibilité': 'disponibilite_codis'
})

print("✅ Données du fournisseur prêtes :")
print(df_codis.head())

# ========================
# 6️⃣ Charger le catalogue table.xlsx
# ========================
df_table = pd.read_excel('table.xlsx', sheet_name='Sheet3')
print(f"✅ table.xlsx chargé ({df_table.shape[0]} lignes).")

# ========================
# 7️⃣ Nettoyer les références dans table et codis
# ========================
def clean_ref(ref):
    if isinstance(ref, str):
        return re.sub(r'[^A-Za-z0-9]', '', ref).upper()
    return ref

if 'reference_mytek' in df_table.columns:
    df_table['reference_mytek'] = df_table['reference_mytek'].apply(clean_ref)
if 'reference_tunisianet' in df_table.columns:
    df_table['reference_tunisianet'] = df_table['reference_tunisianet'].apply(clean_ref)
df_codis['reference_mytek'] = df_codis['reference_mytek'].apply(clean_ref)

# ========================
# 8️⃣ Fusion en deux étapes : mytek puis fallback tunisianet
# ========================
merged_mytek = df_table.merge(
    df_codis,
    left_on='reference_mytek',
    right_on='reference_mytek',
    how='left',
    suffixes=('', '_codis')
)

print(f"✅ Merge sur reference_mytek terminé ({merged_mytek.shape[0]} lignes).")
needs_fallback = merged_mytek[merged_mytek['prix_codis'].isna()]
print(f"✅ Lignes sans prix_codis après 1er merge : {needs_fallback.shape[0]}")

if not needs_fallback.empty:
    fallback_merge = needs_fallback.drop(columns=['prix_codis', 'disponibilite_codis']).merge(
        df_codis,
        left_on='reference_tunisianet',
        right_on='reference_mytek',
        how='left'
    )
    merged_mytek.update(fallback_merge[['prix_codis', 'disponibilite_codis']])
    print("✅ Fallback merge sur reference_tunisianet terminé et valeurs complétées.")
else:
    print("✅ Pas besoin de fallback, tous les prix trouvés sur reference_mytek.")

# ========================
# 9️⃣ Comparer avec anciens prix et éviter écrasement inutile
# ========================
merged_mytek = merged_mytek.reset_index(drop=True)
df_table = df_table.reset_index(drop=True)

for col in ['prix_codis', 'disponibilite_codis']:
    if col in df_table.columns:
        merged_mytek[col] = merged_mytek.apply(
            lambda row: row[col] if pd.notnull(row[col]) and (row[col] != df_table.loc[row.name, col]) else df_table.loc[row.name, col],
            axis=1
        )
        changes = (merged_mytek[col] != df_table[col]) & merged_mytek[col].notnull()
        print(f"✅ {changes.sum()} changement(s) détecté(s) pour la colonne {col}")

# ========================
# 🔟 Ajouter les nouveaux produits Codis non présents dans le catalogue
# ========================
refs_catalogue_mytek = df_table['reference_mytek'].dropna().unique()
refs_catalogue_tunisianet = df_table['reference_tunisianet'].dropna().unique()
refs_connues = set(refs_catalogue_mytek).union(set(refs_catalogue_tunisianet))
refs_codis = set(df_codis['reference_mytek'].dropna().unique())

nouveaux_refs = refs_codis - refs_connues
print(f"✅ Nouveaux produits détectés : {len(nouveaux_refs)}")

if nouveaux_refs:
    nouveaux_produits = df_codis[df_codis['reference_mytek'].isin(nouveaux_refs)].copy()
    nouveaux_produits['nom'] = nouveaux_produits['FAMILLE']
    nouveaux_produits['reference_officielle'] = nouveaux_produits['reference_mytek']
        # ✅ Assigner automatiquement une sous_categorie_id
    def trouver_sous_categorie(famille):
        if not isinstance(famille, str) or famille.strip() == "":
            return -1
        matches = df_table[df_table['nom'].str.contains(famille, case=False, na=False)]
        if not matches.empty:
            return matches['sous_categorie_id'].mode().iloc[0]
        else:
            return -1

    nouveaux_produits['sous_categorie_id'] = nouveaux_produits['FAMILLE'].apply(trouver_sous_categorie)


    # ✅ Générer des IDs uniques pour les nouveaux produits
    if 'id' in df_table.columns:
        max_id = df_table['id'].max()
        if pd.isna(max_id):
            max_id = 0
        nouveaux_ids = range(int(max_id) + 1, int(max_id) + 1 + len(nouveaux_produits))
        nouveaux_produits['id'] = list(nouveaux_ids)
        print(f"✅ IDs attribués aux nouveaux produits : {list(nouveaux_ids)}")

    # ✅ Ajouter colonnes manquantes avec valeurs par défaut
    colonnes_base = list(merged_mytek.columns)
    for col in colonnes_base:
        if col not in nouveaux_produits.columns:
            if col == 'sous_categorie_id':
                nouveaux_produits[col] = -1
            else:
                nouveaux_produits[col] = pd.NA

    # ✅ Remettre les colonnes dans le même ordre
    nouveaux_produits = nouveaux_produits[colonnes_base]

    # ✅ Ajouter à la table
    merged_mytek = pd.concat([merged_mytek, nouveaux_produits], ignore_index=True)
    print(f"✅ {len(nouveaux_refs)} nouveaux produits ajoutés à la table avec nom depuis FAMILLE.")


# ✅ Assurer que 'reference_officielle' est placé juste après 'id'
colonnes = list(merged_mytek.columns)
if 'id' in colonnes and 'reference_officielle' in colonnes:
    idx_id = colonnes.index('id')
    colonnes.remove('reference_officielle')
    colonnes = colonnes[:idx_id + 1] + ['reference_officielle'] + colonnes[idx_id + 1:]
    merged_mytek = merged_mytek[colonnes]
    print("✅ Colonne 'reference_officielle' insérée après 'id'.")

# ========================
# 11️⃣ Réorganisation des colonnes pour Excel
# ========================
# === Nettoyage final avant export ===
if 'FAMILLE' in merged_mytek.columns:
    merged_mytek.drop(columns=['FAMILLE'], inplace=True)

colonnes_prix = ['prix_codis', 'mytek_avant_remise', 'mytek_apres_remise', 'tunisianet_avant_remise', 'tunisianet_apres_remise']

def ligne_sans_prix(row):
    return all(pd.isna(row.get(c)) or str(row[c]).strip() == '' for c in colonnes_prix)

merged_mytek = merged_mytek[~merged_mytek.apply(ligne_sans_prix, axis=1)].reset_index(drop=True)

colonnes = list(merged_mytek.columns)

if 'nom' not in colonnes:
    print("❌ La colonne 'nom' est absente de table.xlsx !")
    exit()

idx_nom_produit = colonnes.index('nom')
colonnes_sans_new = [c for c in colonnes if c not in ['prix_codis', 'disponibilite_codis']]
nouvel_ordre = (
    colonnes_sans_new[:idx_nom_produit + 1]
    + ['prix_codis', 'disponibilite_codis']
    + colonnes_sans_new[idx_nom_produit + 1:]
)


df_final = merged_mytek[nouvel_ordre]

# ========================
# 12️⃣ Sauvegarde Excel
# ========================
df_final.to_excel('table_modifiee.xlsx', index=False)
print("✅ Fichier final généré : table_modifiee.xlsx")
