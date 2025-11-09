import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

# Charger variables d'environnement
load_dotenv()
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

def get_connection():
    """Retourne une connexion PostgreSQL avec autocommit."""
    conn = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    conn.autocommit = True
    return conn

def upsert_teams_season(df, table="team_season1"):
    """
    Insert ou update les stats des équipes pour une saison.
    df doit contenir exactement les colonnes suivantes :
    ['team','season','home_points','points','home_goal_difference','away_goal_difference',
     'played','home_wins','away_wins',
     'home_possession','away_possession','home_shots_on_target','away_shots_on_target',
     'home_fouls','away_fouls','home_passes','away_passes',
     'home_corners','away_corners','home_attacks','away_attacks',
     'home_dangerous_attacks','away_dangerous_attacks']
    """
    
    query = f"""
        INSERT INTO {table} (
            team, season, home_points, points, home_goal_difference, away_goal_difference,
            played, home_wins, away_wins,
            home_possession, away_possession, home_shots_on_target, away_shots_on_target,
            home_fouls, away_fouls, home_passes, away_passes,
            home_corners, away_corners, home_attacks, away_attacks,
            home_dangerous_attacks, away_dangerous_attacks
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (team, season) DO UPDATE SET
            home_points = EXCLUDED.home_points,
            points = EXCLUDED.points,
            home_goal_difference = EXCLUDED.home_goal_difference,
            away_goal_difference = EXCLUDED.away_goal_difference,
            played = EXCLUDED.played,
            home_wins = EXCLUDED.home_wins,
            away_wins = EXCLUDED.away_wins,
            home_possession = EXCLUDED.home_possession,
            away_possession = EXCLUDED.away_possession,
            home_shots_on_target = EXCLUDED.home_shots_on_target,
            away_shots_on_target = EXCLUDED.away_shots_on_target,
            home_fouls = EXCLUDED.home_fouls,
            away_fouls = EXCLUDED.away_fouls,
            home_passes = EXCLUDED.home_passes,
            away_passes = EXCLUDED.away_passes,
            home_corners = EXCLUDED.home_corners,
            away_corners = EXCLUDED.away_corners,
            home_attacks = EXCLUDED.home_attacks,
            away_attacks = EXCLUDED.away_attacks,
            home_dangerous_attacks = EXCLUDED.home_dangerous_attacks,
            away_dangerous_attacks = EXCLUDED.away_dangerous_attacks;
    """

    conn = get_connection()
    cur = conn.cursor()
    
    # Transformer le DataFrame en liste de tuples
    rows = [tuple(row) for row in df.to_numpy()]
    
    # Exécution batch pour optimiser l'insertion
    execute_batch(cur, query, rows, page_size=50)
    
    cur.close()
    conn.close()
    print(f"✅ {len(rows)} lignes insérées ou mises à jour dans {table}.")
