# Gestion devis Maktabi

Projet de gestion de devis développé pour Maktabi. Le pipeline récupère chaque jour
les prix des concurrents et du fournisseur Codis, calcule les
marges et alimente un rapport Power BI.
## Architecture
<img width="6836" height="2174" alt="mermaid-diagram" src="https://github.com/user-attachments/assets/a979416f-0e50-47d0-8ce0-7220a331ff3e" />
-> tout est automatisé en une seule commande, donc les rapports se mettent à jour sans saisie manuelle. Cela garantit des données fraîches et un gain de temps considérable pour le suivi quotidien des prix.

## Structure

```
Gestion_devis_Maktabi/
├── run.py                  # point d'entrée unique du pipeline
├── maktabi/
│   ├── config.py           # tous les chemins et noms de fichiers
│   ├── concurrents.py      # étape 1 : scraping Mytek / Tunisianet
│   ├── codis.py            # étape 2 : prix Codis depuis les emails (IMAP)
│   └── marges.py           # étape 3 : nettoyage des prix, marges, export Power BI
├── data/
│   ├── catalogue_base.xlsx # catalogue de référence (suivi par git)
│   ├── daily/              # JJ-MM-AAAA.xlsx         — sortie des étapes 1 et 2
│   ├── cleaned/            # JJ-MM-AAAA cleaned.xlsx — sortie de l'étape 3
│   ├── codis/              # extraction brute des emails Codis
│   └── powerbi/            # table_modifiee.xlsx — source du rapport Power BI
├── reports/powerbi.pbix    # rapport Power BI
├── docs/                   # notes : catégories à scraper, TODO
├── requirements.txt
├── .env.example            # modèle du fichier d'identifiants
└── .env                    # identifiants Gmail (jamais commité)
```

Les dossiers `data/daily`, `data/cleaned`, `data/codis` et `data/powerbi` sont
régénérés à chaque exécution et ne sont pas suivis par git.

### Feuilles de `catalogue_base.xlsx`

| Feuille | Contenu |
|---------|---------|
| Sheet1  | catégories |
| Sheet2  | sous-catégories |
| SSC     | sous-sous-catégories + URL des pages catalogue à scraper |
| Sheet3  | produits (références, prix, disponibilités, URL Mytek / Tunisianet, prix Codis) |
| Sheet4  | attributs techniques par sous-catégorie |
| Sheet5  | valeurs des attributs par produit |

## Installation

Python 3.12 :

```powershell
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env    # puis remplir .env
```

`.env` contient l'adresse Gmail et un **mot de passe d'application** Gmail
(https://myaccount.google.com/apppasswords), pas le mot de passe du compte.

## Utilisation

Depuis le dossier du projet, avec le `.venv` activé :

```powershell
python run.py all            # les 3 étapes dans l'ordre
python run.py concurrents    # 1. prix Mytek / Tunisianet  -> data/daily/JJ-MM-AAAA.xlsx
python run.py codis          # 2. prix Codis               -> Sheet3 du fichier du jour
python run.py marges         # 3. marges                   -> data/cleaned + data/powerbi
python run.py codis --date 28-08-2025   # relancer une étape sur un autre jour
```

Les étapes 2 et 3 lisent le fichier produit par l'étape 1 pour la même date.

## Power BI

Le rapport `reports/powerbi.pbix` lit `data/powerbi/table_modifiee.xlsx`.
Si le rapport pointe encore vers un ancien chemin : Power BI Desktop →
Transformer les données → Paramètres de la source de données → Changer de source.
## Sorties 
<img width="1280" height="713" alt="capture1" src="https://github.com/user-attachments/assets/60327f78-fc0e-4eb8-86d9-0430b111736f" />
on repère immédiatement les produits vendus presque à perte, comme les EcoTank, pour réajuster leur prix. On identifie aussi les produits à forte marge, comme les imprimantes de ticket et les scanners, que l’on peut mettre en avant.
<img width="1280" height="727" alt="capture2" src="https://github.com/user-attachments/assets/fb37b834-bd8d-4124-911c-84342ea625de" />
on visualise l’écart entre prix de vente et prix d’achat, donc la marge brute en valeur. On peut aussi renégocier avec Codis sur les produits où l’écart est faible ou nul, par exemple en fin de graphique.
<img width="1280" height="717" alt="capture3" src="https://github.com/user-attachments/assets/8bd0511b-e52d-41bb-bc7f-0e2497f38b68" />
on voit sur quels produits Tunisianet réalise de meilleures marges que nous, et on découvre des catégories absentes de notre catalogue (onduleurs, PC gamers). Ce sont des opportunités d’élargissement de gamme.



