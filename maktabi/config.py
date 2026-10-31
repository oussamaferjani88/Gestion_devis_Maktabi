"""Chemins et paramètres partagés par toutes les étapes du pipeline.

Tous les chemins sont relatifs au dossier du projet : le projet fonctionne
quel que soit l'endroit où il est copié.
"""
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT_DIR / ".env"

DATA_DIR = ROOT_DIR / "data"
# Catalogue de référence : Sheet1, Sheet2, SSC (URLs des catégories), Sheet3/4/5
CATALOGUE_BASE = DATA_DIR / "catalogue_base.xlsx"
DAILY_DIR = DATA_DIR / "daily"        # JJ-MM-AAAA.xlsx         (concurrents puis codis)
CLEANED_DIR = DATA_DIR / "cleaned"    # JJ-MM-AAAA cleaned.xlsx (marges)
CODIS_DIR = DATA_DIR / "codis"        # extraction brute des emails Codis
POWERBI_DIR = DATA_DIR / "powerbi"    # source de données du rapport Power BI
POWERBI_TABLE = POWERBI_DIR / "table_modifiee.xlsx"


def today_str() -> str:
    return datetime.today().strftime("%d-%m-%Y")


def daily_file(date_str: str | None = None) -> Path:
    return DAILY_DIR / f"{date_str or today_str()}.xlsx"


def cleaned_file(date_str: str | None = None) -> Path:
    return CLEANED_DIR / f"{date_str or today_str()} cleaned.xlsx"
