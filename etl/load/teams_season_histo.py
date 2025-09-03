import pandas as pd
from dotenv import load_dotenv
import psycopg2
import os

# Chargement des variables d’environnement
load_dotenv()
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

def insert_team_seasons(df, cursor):
    """
    Insère les stats des équipes dans team_season
    en utilisant directement la saison comme texte.
    """
    inserted = 0
    ignored = 0

    for _, row in df.iterrows():
        try:
            team_name = row["Team"]
            season_value = row["Season"]   # ex "2019-2020"
            points = row["Points"]
            goal_diff = row["GoalDifference"]
            rank = row["Position"]

            #  Récupérer le team_id
            cursor.execute("SELECT team_id FROM teams WHERE name = %s;", (team_name,))
            team = cursor.fetchone()
            if not team:
                print(f"⚠️ Équipe {team_name} introuvable -> ligne ignorée")
                ignored += 1
                continue
            team_id = team[0]

            #  Insérer dans team_season
            cursor.execute("""
                INSERT INTO team_season (team_id, season, league, rank, points, goal_diff)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (team_id, season_value, "Premier League", rank, points, goal_diff))
            inserted += 1

        except Exception as e:
            print(f" Erreur pour {team_name} saison {season_value}: {e}")
            ignored += 1

    print(f"\n Insertion terminée : {inserted} lignes insérées, {ignored} ignorées.")

def main():
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        cursor = connection.cursor()

        # Charger ton CSV
        df = pd.read_csv("data/processed/teams_seasons_stats.csv")

        insert_team_seasons(df, cursor)

        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"❌ Connexion DB échouée ou erreur SQL : {e}")

if __name__ == "__main__":
    main()
