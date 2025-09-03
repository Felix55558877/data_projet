import pandas as pd
from dotenv import load_dotenv
import psycopg2
import os
import datetime


# Chargement des variables d’environnement
load_dotenv()
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

def insert_match_stats(df, connection):
    """
    Insère les fixtures avec stats dans match_stats.
    Chaque ligne est insérée indépendamment.
    """
    inserted = 0
    ignored = 0
    cursor = connection.cursor()

    for _, row in df.iterrows():
        try:
            fixture_id = int(row.get("fixture_id"))
            season_id = str(row.get("season_id")).strip()
            date_match = row.get("date_match")

            home_team_name = str(row.get("home_team")).strip()
            away_team_name = str(row.get("away_team")).strip()

            # Récupérer team_id
            cursor.execute("SELECT team_id FROM teams WHERE name = %s;", (home_team_name,))
            home_team = cursor.fetchone()
            if not home_team:
                print(f"⚠️ Équipe domicile '{home_team_name}' introuvable -> ligne ignorée")
                ignored += 1
                continue
            home_team_id = home_team[0]

            cursor.execute("SELECT team_id FROM teams WHERE name = %s;", (away_team_name,))
            away_team = cursor.fetchone()
            if not away_team:
                print(f"⚠️ Équipe extérieur '{away_team_name}' introuvable -> ligne ignorée")
                ignored += 1
                continue
            away_team_id = away_team[0]

            # Conversion de la date*
            if not date_match or pd.isna(date_match):
                print(f"⚠️ Date manquante pour le match {home_team_name} vs {away_team_name} -> ligne ignorée")
                ignored += 1
                continue

            # Supprimer espaces éventuels
            date_match = str(date_match).strip()

            # Convertir en date
            try:
                date_match_obj = pd.to_datetime(date_match, errors="coerce").date()
                if date_match_obj is pd.NaT:
                    raise ValueError("Conversion impossible")
            except Exception as e:
                print(f"⚠️ Date invalide pour le match {home_team_name} vs {away_team_name} -> ligne ignorée")
                ignored += 1
                continue            
            

            # Insérer dans match_stats
            cursor.execute("""
                INSERT INTO match_stats (
                    fixture_id, season_id, date_match,
                    home_team_id, away_team_id,
                    home_goals, away_goals,
                    home_possession, away_possession,
                    home_shots_on_target, away_shots_on_target,
                    home_fouls, away_fouls,
                    home_passes, away_passes,
                    home_corners, away_corners,
                    home_attacks, away_attacks,
                    home_dangerous_attacks, away_dangerous_attacks,
                    adv_home
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, TRUE
                )
                ON CONFLICT (fixture_id) DO NOTHING;
            """, (
                fixture_id, season_id, date_match_obj,
                home_team_id, away_team_id,
                row.get("home_goals"), row.get("away_goals"),
                row.get("home_possession"), row.get("away_possession"),
                row.get("home_shots_on_target"), row.get("away_shots_on_target"),
                row.get("home_fouls"), row.get("away_fouls"),
                row.get("home_passes"), row.get("away_passes"),
                row.get("home_corners"), row.get("away_corners"),
                row.get("home_attacks"), row.get("away_attacks"),
                row.get("home_dangerous_attacks"), row.get("away_dangerous_attacks")
            ))
            inserted += 1
            print(f"✅ Fixture {row.get('fixture_id')} insérée")
        except Exception as e:
            ignored += 1
            print(f"❌ Erreur fixture {row.get('fixture_id')} : {e}")

    cursor.close()
    print(f"\n✅ Insertion terminée : {inserted} lignes insérées, {ignored} ignorées.")


def main():
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,  
            dbname=DBNAME
        )
        connection.autocommit = True  # exécuter ligne par ligne

        # Charger le CSV généré précédemment
        df = pd.read_csv("data/processed/fixtures_stats.csv")

        insert_match_stats(df, connection)

        connection.close()

    except Exception as e:
        print(f"❌ Connexion DB échouée ou erreur SQL : {e}")


if __name__ == "__main__":
    main()
