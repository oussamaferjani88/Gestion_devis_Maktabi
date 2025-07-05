from rapidfuzz import fuzz
import unidecode
import re

# -----------------------------
# Function to clean product names
# -----------------------------
def clean_name(name):
    name = name.lower()
    name = unidecode.unidecode(name)
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    stopwords = ['multifonction', 'couleur', 'noir', 'jet', 'd', 'encre', '3', 'en', '1']
    words = [w for w in name.split() if w not in stopwords]
    return ' '.join(words)

# -----------------------------
# Example products
# -----------------------------
mytek_product = "Imprimante multifonction CANON PIXMA TS3340 WIFI Couleur"
tunisianet_product = "Imprimante multifonction CANON PIXMA L3230 WIFI Couleur"

# -----------------------------
# Cleaned versions
# -----------------------------
clean_mytek = clean_name(mytek_product)
clean_tunisianet = clean_name(tunisianet_product)

print(f"Mytek (clean): {clean_mytek}")
print(f"Tunisianet (clean): {clean_tunisianet}")

# -----------------------------
# Fuzzy similarity
# -----------------------------
score = fuzz.ratio(clean_mytek, clean_tunisianet)
print(f"Fuzzy match score: {score}")
