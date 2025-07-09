import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------------
# CONFIGURATION
# -------------------------------
DB_USER = 'postgres'
DB_PASS = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'gestion de devis maktabi'
EXCEL_PATH = 'concurrents.xlsx'

# Connexion
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# -------------------------------
# Charger les données Excel
# -------------------------------
sheets = pd.read_excel(EXCEL_PATH, sheet_name=None)

df_category = sheets['Sheet1']
df_sous_categorie = sheets['Sheet2']
df_produit = sheets['Sheet3']
df_attributs = sheets['Sheet4']
df_valeurs = sheets['Sheet5']

# -------------------------------
# Créer les tables dans la DB
# (DROP + CREATE pour être sûr)
# -------------------------------
with engine.connect() as conn:
    conn.execute(text("""
        DROP TABLE IF EXISTS valeurs;
        DROP TABLE IF EXISTS attributs;
        DROP TABLE IF EXISTS produit;
        DROP TABLE IF EXISTS sous_categorie;
        DROP TABLE IF EXISTS category;
        
        CREATE TABLE category (
            id INTEGER PRIMARY KEY,
            nom TEXT
        );

        CREATE TABLE sous_categorie (
            id INTEGER PRIMARY KEY,
            nom TEXT,
            categorie_id INTEGER REFERENCES category(id)
        );

        CREATE TABLE produit (
            id INTEGER PRIMARY KEY,
            nom TEXT,
            sous_categorie_id INTEGER REFERENCES sous_categorie(id),
            prix_avant_remise TEXT,
            prix_apres_remise TEXT
        );

        CREATE TABLE attributs (
            id INTEGER PRIMARY KEY,
            nom TEXT,
            sous_categorie_id INTEGER REFERENCES sous_categorie(id)
        );

        CREATE TABLE valeurs (
            produit_id INTEGER REFERENCES produit(id),
            attribut_id INTEGER REFERENCES attributs(id),
            valeur TEXT
        );
    """))
    print("✅ Tables créées.")

# -------------------------------
# Insérer les données
# -------------------------------
df_category.to_sql('category', engine, if_exists='append', index=False)
df_sous_categorie.to_sql('sous_categorie', engine, if_exists='append', index=False)
df_produit.to_sql('produit', engine, if_exists='append', index=False)
df_attributs.to_sql('attributs', engine, if_exists='append', index=False)
df_valeurs.to_sql('valeurs', engine, if_exists='append', index=False)

print("✅ Données importées dans PostgreSQL.")
