"""Étape 2 : prix fournisseur Codis.

Lit les tableaux de prix envoyés par email par Codis, puis met à jour
prix_codis / disponibilite_codis dans la Sheet3 du fichier du jour.
"""
import imaplib
import email
import os
import unicodedata
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import re
import unidecode
from dotenv import load_dotenv

from maktabi import config

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


def clean_ref(ref):
    if isinstance(ref, str):
        return re.sub(r'[^A-Za-z0-9]', '', ref).upper()
    return ref


IMAP_SERVER = 'imap.gmail.com'
SENDERS = ["maktabi2013@gmail.com", "Hanene.BOUAZZA@codis.com.tn"]
SUBJECTS = ["Codis", "Lenovo"]


def main(date_str=None):
    # ========================
    # 1️⃣ Connexion IMAP
    # ========================
    # Identifiants lus depuis le fichier .env du projet (jamais dans le code)
    load_dotenv(config.ENV_FILE)
    email_account = os.environ["MAKTABI_EMAIL"]
    email_password = os.environ["MAKTABI_EMAIL_PASSWORD"]

    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(email_account, email_password)
    mail.select('INBOX')

    # ========================
    # 2️⃣ Recherche d'emails spécifiques
    # ========================
    all_ids = set()

    # Recherche par FROM
    for sender in SENDERS:
        result, data = mail.search(None, f'FROM "{sender}"')
        ids = set(data[0].split())
        all_ids.update(ids)

    # Recherche par SUBJECT
    for subj in SUBJECTS:
        result, data = mail.search(None, f'SUBJECT "{subj}"')
        ids = set(data[0].split())
        all_ids.update(ids)

    # Trier par ordre d'arrivée : les emails les plus récents sont traités en dernier
    email_ids = sorted(all_ids, key=int)


    if not email_ids:
        print("❌ Aucun email trouvé avec ce critère.")
        mail.logout()
        return


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
                # Nettoyage des espaces invisibles unicode (non-breaking spaces, etc.)
                df_piece = df_piece.map(
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

    mail.logout()

    if not df_list:
        print("❌ Aucun tableau valide trouvé .")
        return

    update_catalogue(df_list, date_str)


def update_catalogue(df_list, date_str=None):
    """Fusionne les tableaux Codis extraits dans la Sheet3 du fichier du jour."""
    df_codis = pd.concat(df_list, ignore_index=True)
    # Les tableaux à 2/3/4 colonnes n'ont pas toutes les colonnes
    for col in ['FAMILLE', 'DESCRIPTION', 'Disponibilité']:
        if col not in df_codis.columns:
            df_codis[col] = pd.NA
    df_codis = format_codis_price_clean(df_codis)
    config.CODIS_DIR.mkdir(parents=True, exist_ok=True)
    df_codis.to_excel(config.CODIS_DIR / 'codis_extracted_data.xlsx', index=False)
    print(f"✅ Total lignes combinées : {len(df_codis)}")

    # ========================
    # 5️⃣ Nettoyage des données fournisseur
    # ========================

    df_codis.columns = [col.strip() for col in df_codis.columns]
    df_codis = df_codis.map(lambda x: x.strip() if isinstance(x, str) else x)


    df_codis['Disponibilité'] = df_codis['Disponibilité'].map(
        lambda x: x.strip().upper() if isinstance(x, str) else x
    )

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
    # 6️⃣ Charger le catalogue du jour
    # ========================
    file_path = config.daily_file(date_str)

    if not file_path.exists():
        raise FileNotFoundError(f"❌ Fichier introuvable : {file_path} (lancer d'abord l'étape concurrents)")

    df_table = pd.read_excel(file_path, sheet_name='Sheet3')
    print(f"✅ {file_path.name} chargé ({df_table.shape[0]} lignes).")
    # ========================
    # 7️⃣ Nettoyer les références dans table et codis
    # ========================
    if 'reference_mytek' in df_table.columns:
        df_table['reference_mytek'] = df_table['reference_mytek'].apply(clean_ref)
    if 'reference_tunisianet' in df_table.columns:
        df_table['reference_tunisianet'] = df_table['reference_tunisianet'].apply(clean_ref)
    df_codis['reference_mytek'] = df_codis['reference_mytek'].apply(clean_ref)

    # Une même référence peut apparaître dans plusieurs emails : garder la plus récente
    nb_avant = len(df_codis)
    df_codis = df_codis.drop_duplicates(subset='reference_mytek', keep='last').reset_index(drop=True)
    print(f"✅ {nb_avant - len(df_codis)} doublon(s) de référence supprimé(s) dans les données Codis.")

    # ========================
    # 8️⃣ Correspondance : reference_mytek puis fallback reference_tunisianet
    # ========================
    # map() garde exactement une ligne par produit du catalogue (pas de lignes dupliquées
    # ni de valeurs écrites sur la mauvaise ligne)
    codis_par_ref = df_codis.set_index('reference_mytek')
    merged_mytek = df_table.copy()

    for col in ['prix_codis', 'disponibilite_codis']:
        nouveau = merged_mytek['reference_mytek'].map(codis_par_ref[col])
        if 'reference_tunisianet' in merged_mytek.columns:
            nouveau = nouveau.fillna(merged_mytek['reference_tunisianet'].map(codis_par_ref[col]))

        # ========================
        # 9️⃣ Comparer avec anciens prix : garder l'ancienne valeur si Codis n'en donne pas
        # ========================
        if col in merged_mytek.columns:
            changes = nouveau.notna() & (nouveau != merged_mytek[col])
            print(f"✅ {changes.sum()} changement(s) détecté(s) pour la colonne {col}")
            merged_mytek[col] = nouveau.fillna(merged_mytek[col])
        else:
            merged_mytek[col] = nouveau

    print(f"✅ {merged_mytek['prix_codis'].notna().sum()} produit(s) du catalogue avec un prix Codis.")

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
            matches = df_table[df_table['nom'].str.contains(famille, case=False, na=False, regex=False)]
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
        print("❌ La colonne 'nom' est absente de la Sheet3 !")
        return

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
    # Remplacement en une seule écriture : si le script plante, l'ancienne Sheet3 reste intacte
    with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_final.to_excel(writer, sheet_name="Sheet3", index=False)

    print(f"✅ Sheet3 replaced in {file_path.name} (other sheets untouched)")


if __name__ == "__main__":
    main()
