#Charger le fichier csv généré par extract_match_histo_api_sportsmonk dans la base de données
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

def insert_matches(df, connection):
    """
    Insère les matchs historiques dans la table match.
    Chaque ligne est insérée indépendamment pour éviter
    que toute la transaction échoue si une ligne est incorrecte.
    """
    inserted = 0
    ignored = 0
    cursor = connection.cursor()

    for _, row in df.iterrows():
        try:
            # Normalisation des noms et champs obligatoires
            fixture_id = str(row.get("fixture_id")).strip()
            season_id = str(row.get("season_id")).strip()
            season_label = str(row.get("season_label")).strip()
            date_match = row.get("Date_Match")

            home_team = str(row.get("home_team")).strip()
            away_team = str(row.get("away_team")).strip()
            home_goals = row.get("home_goals")
            away_goals = row.get("away_goals")
            home_possession = row.get("home_possession")
            away_possession = row.get("away_possession")
            home_shots_on_target = row.get("home_shots_on_target")
            away_shots_on_target = row.get("away_shots_on_target")
            home_fouls = row.get("home_fouls")
            away_fouls = row.get("away_fouls")
            home_passes = row.get("home_passes")
            away_passes = row.get("away_passes")
            home_corners = row.get("home_corners")
            away_corners = row.get("away_corners")
            home_attacks = row.get("home_attacks")
            away_attacks = row.get("away_attacks")
            home_dangerous_attacks = row.get("home_dangerous_attacks")
            away_dangerous_attacks = row.get("away_dangerous_attacks")
            home_adv = row.get("adv_home")

            # Vérification champs obligatoires
            if None in (fixture_id, home_team_name, away_team_name, date_match, season_name, home_score, away_score):
                ignored += 1
                print(f"⚠️ Ligne ignorée : champs obligatoires manquants")
                continue

            # Conversion de la date
            date_match_obj = pd.to_datetime(date_match, dayfirst=True).date()

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

            # Insérer le match avec season texte
            cursor.execute("""
            INSERT INTO match (season, date_match, home_team_id, away_team_id, home_score, away_score)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (season_name, date_match_obj, home_team_id, away_team_id, int(home_score), int(away_score)))
            inserted += 1

        except Exception as e:
            ignored += 1
            print(f" Erreur traitement ligne {home_team_name} vs {away_team_name} le {date_match}: {e}")

    cursor.close()
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
        connection.autocommit = True  # Chaque commande est indépendante

        df = pd.read_csv("data/processed/matchs_historiques.csv")

        insert_matches(df, connection)

        connection.close()

    except Exception as e:
        print(f" Connexion DB échouée ou erreur SQL : {e}")

if __name__ == "__main__":
    main()