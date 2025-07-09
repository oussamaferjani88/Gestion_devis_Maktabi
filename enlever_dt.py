import pandas as pd

# Fichier à modifier
FILENAME = 'concurrents_final.xlsx'

# Charger la feuille par défaut
df = pd.read_excel(FILENAME)

# Colonnes cibles à nettoyer
colonnes_cibles = [
    'my_tek_avant_remise',
    'my_tek_apres_remise',
    'tunisianet_avant_remise',
    'tunisianet_apres_remise'
]

# Nettoyer chaque colonne si elle existe
for col in colonnes_cibles:
    if col in df.columns:
        print(f"Nettoyage de la colonne : {col}")
        df[col] = (
            df[col]
            .astype(str)
            .str.replace('DT', '', regex=False)
            .str.strip()
        )
        # Convertir en nombre
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Sauvegarder par-dessus le même fichier
df.to_excel(FILENAME, index=False)
print(f"✅ Nettoyage terminé. Fichier écrasé : {FILENAME}")
