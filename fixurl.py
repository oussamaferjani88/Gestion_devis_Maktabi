import pandas as pd

# -----------------------------
# Chemin vers ton Excel
# -----------------------------
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test3_updated.xlsx"

# -----------------------------
# Charger Sheet3
# -----------------------------
df = pd.read_excel(EXCEL_PATH, sheet_name='Sheet3')
print(f"✅ Sheet3 chargé : {len(df)} lignes.")

# -----------------------------
# Nettoyer la colonne 'url'
# -----------------------------
def clean_url(url):
    if pd.isnull(url):
        return url
    cleaned = url.replace("'", "-").replace("é", "e")
    return cleaned

df['url'] = df['url'].apply(clean_url)

print("✅ Colonnes 'url' nettoyées :")
print(df[['url']].head())

# -----------------------------
# Sauvegarder dans le même fichier (remplacer Sheet3)
# -----------------------------
with pd.ExcelWriter(EXCEL_PATH, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
    df.to_excel(writer, index=False, sheet_name='Sheet3')

print(f"✅ Sheet3 mis à jour et sauvegardé dans : {EXCEL_PATH}")
