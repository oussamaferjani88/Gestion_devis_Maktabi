"""Point d'entrée du pipeline Maktabi.

Usage :
    python run.py all                        # les 3 étapes, dans l'ordre
    python run.py concurrents                # 1. prix Mytek / Tunisianet
    python run.py codis                      # 2. prix fournisseur Codis (emails)
    python run.py marges                     # 3. nettoyage + marges + export Power BI
    python run.py codis --date 28-08-2025    # relancer une étape sur un autre jour
"""
import argparse
import sys

from maktabi import codis, concurrents, config, marges

STEPS = {
    "concurrents": concurrents.main,
    "codis": codis.main,
    "marges": marges.main,
}


def main():
    # Les scripts affichent des emojis : éviter les erreurs d'encodage de la console Windows
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Pipeline de veille prix Maktabi")
    parser.add_argument("step", choices=[*STEPS, "all"], help="étape à lancer")
    parser.add_argument("--date", help="date du fichier à traiter, JJ-MM-AAAA (défaut : aujourd'hui)")
    args = parser.parse_args()

    # Même date pour toutes les étapes, même si le pipeline passe minuit
    date_str = args.date or config.today_str()
    steps = STEPS if args.step == "all" else [args.step]
    for name in steps:
        print(f"\n========== {name} ({date_str}) ==========")
        STEPS[name](date_str)


if __name__ == "__main__":
    main()
