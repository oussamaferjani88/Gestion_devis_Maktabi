import imaplib
import email
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import re

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
result, data = mail.search(None, '(SUBJECT "Codis")')
email_ids = data[0].split()

if not email_ids:
    print("❌ Aucun email trouvé avec ce critère.")
    exit()

latest_email_id = email_ids[-1]
result, data = mail.fetch(latest_email_id, '(RFC822)')
raw_email = data[0][1]
msg = email.message_from_bytes(raw_email)

print("✅ Email trouvé et chargé.")

# ========================
# 3️⃣ Extraire le corps HTML
# ========================
html_content = None
for part in msg.walk():
    if part.get_content_type() == "text/html":
        html_content = part.get_payload(decode=True).decode()
        break

if html_content is None:
    print("❌ Aucun contenu HTML trouvé dans l'email.")
    exit()

print("✅ Contenu HTML extrait.")

# ========================
# 4️⃣ Parser tous les tableaux HTML
# ========================
soup = BeautifulSoup(html_content, 'html.parser')
tables = soup.find_all('table')

if not tables:
    print("❌ Aucun tableau trouvé dans l'email.")
    exit()

print(f"✅ {len(tables)} tableau(x) trouvé(s).")

df_list = []
for idx, table in enumerate(tables):
    try:
        html_str = str(table)
        df_piece = pd.read_html(StringIO(html_str), header=0)[0]
        df_piece.columns = [str(c).strip() for c in df_piece.columns]

        if len(df_piece.columns) >= 5:
            df_piece = df_piece.iloc[:, :5]
            df_piece.columns = ["REF", "FAMILLE", "DESCRIPTION", "Prix HT", "Disponibilité"]
            df_list.append(df_piece)
            print(f"✅ Tableau {idx + 1} ACCEPTÉ ({df_piece.shape[0]} lignes). Colonnes standardisées.")
        else:
            print(f"⚠️ Tableau {idx + 1} ignoré : pas assez de colonnes ({df_piece.columns})")

    except Exception as e:
        print(f"⚠️ Erreur sur le tableau {idx + 1}: {e}")

if not df_list:
    print("❌ Aucun tableau valide trouvé après filtrage.")
    exit()

df_codis = pd.concat(df_list, ignore_index=True)
print(f"✅ Total lignes combinées : {len(df_codis)}")

# ========================
# 5️⃣ Nettoyage des données fournisseur
# ========================
df_codis.columns = [col.strip() for col in df_codis.columns]
df_codis = df_codis.applymap(lambda x: x.strip() if isinstance(x, str) else x)

expected_cols = ['REF', 'FAMILLE', 'Prix HT', 'Disponibilité']
for col in expected_cols:
    if col not in df_codis.columns:
        print(f"❌ La colonne attendue '{col}' est manquante dans le tableau Codis.")
        exit()

df_codis['Prix HT'] = df_codis['Prix HT'].str.replace('HT', '', regex=False).str.strip()
df_codis['Prix HT'] = pd.to_numeric(df_codis['Prix HT'], errors='coerce')
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
