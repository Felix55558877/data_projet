import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

load_dotenv()
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

def get_connection():
    conn = psycopg2.connect(
        user=USER, password=PASSWORD, host=HOST, port=PORT, dbname=DBNAME
    )
    return conn

def fetch_matches(season_id):
    query = """
        SELECT match_id, date_match, home_team_id, away_team_id,
               home_goals, away_goals
        FROM match_stats
        WHERE season_id = %s
        ORDER BY date_match ASC
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(season_id,))
    conn.close()
    return df

def fetch_team_stats_before_season(season_id):
    query = """
        SELECT * FROM team_season1
        WHERE season_id = %s -1
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(season_id,))
    conn.close()
    return df

def fetch_h2h(home_team_id, away_team_id, season_id):
    query = """
        SELECT home_team_id, away_team_id, home_goals, away_goals
        FROM match_stats
        WHERE ((home_team_id = %s AND away_team_id = %s)
           OR (home_team_id = %s AND away_team_id = %s))
           AND season_id <= %s
        ORDER BY date_match DESC
        LIMIT 5
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(home_team_id, away_team_id, away_team_id, home_team_id, season_id))
    conn.close()
    return df

def generate_dataset(season_id):
    matches = fetch_matches(season_id)
    stats_df = fetch_team_stats_before_season(season_id)
    stats_map = stats_df.set_index('team_season_id').to_dict(orient='index')

    rows = []

    for _, match in matches.iterrows():
        home_id = match['home_team_id']
        away_id = match['away_team_id']

        home_stats = stats_map.get(home_id, {})
        away_stats = stats_map.get(away_id, {})

        # Head-to-head
        h2h = fetch_h2h(home_id, away_id, season_id)
        h2h_home_wins = ((h2h['home_team_id'] == home_id) & (h2h['home_goals'] > h2h['away_goals'])).sum()
        h2h_away_wins = ((h2h['away_team_id'] == away_id) & (h2h['away_goals'] > h2h['home_goals'])).sum()
        h2h_draws = (h2h['home_goals'] == h2h['away_goals']).sum()
        h2h_avg_goal_diff_home = ((h2h['home_goals'] - h2h['away_goals']).mean() if not h2h.empty else 0)
        h2h_avg_goals_home_scored = (h2h['home_goals'].mean() if not h2h.empty else 0)
        h2h_avg_goals_away_scored = (h2h['away_goals'].mean() if not h2h.empty else 0)

        # Label
        if match['home_goals'] > match['away_goals']:
            result = 'home_win'
        elif match['home_goals'] < match['away_goals']:
            result = 'away_win'
        else:
            result = 'draw'

        row = {
            'match_id': match['match_id'],
            'season_id': season_id,
            'date_match': match['date_match'],
            'home_team_id': home_id,
            'away_team_id': away_id,
            
            # Stats historiques
            'points_home': home_stats.get('points', 0),
            'points_away': away_stats.get('points', 0),
            'goal_diff_home': home_stats.get('goal_diff', 0),
            'goal_diff_away': away_stats.get('goal_diff', 0),
            'goals_scored_home': home_stats.get('goals_scored', 0),
            'goals_scored_away': away_stats.get('goals_scored', 0),
            'goals_conceded_home': home_stats.get('goals_conceded', 0),
            'goals_conceded_away': away_stats.get('goals_conceded', 0),
            'possession_home': home_stats.get('possession_avg', 0),
            'possession_away': away_stats.get('possession_avg', 0),
            'shots_on_target_home': home_stats.get('shots_on_target_avg', 0),
            'shots_on_target_away': away_stats.get('shots_on_target_avg', 0),
            
            # Head-to-head
            'h2h_home_wins': h2h_home_wins,
            'h2h_away_wins': h2h_away_wins,
            'h2h_draws': h2h_draws,
            'h2h_avg_goal_diff_home': h2h_avg_goal_diff_home,
            'h2h_avg_goals_home_scored': h2h_avg_goals_home_scored,
            'h2h_avg_goals_away_scored': h2h_avg_goals_away_scored,

            'result': result
        }
        rows.append(row)

    return pd.DataFrame(rows)

def insert_training_dataset(df, table="training_modele_season"):
    """Insère le DataFrame dans la table Supabase"""
    conn = get_connection()
    try:
        cur = conn.cursor()
        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        columns_sql = ", ".join(columns)
        query = f"""
            INSERT INTO {table} ({columns_sql})
            VALUES ({placeholders})
            ON CONFLICT (match_id) DO NOTHING;
        """
        rows = [tuple(r) for r in df.to_numpy()]
        execute_batch(cur, query, rows, page_size=50)
        conn.commit()  
        print(f" {len(df)} lignes insérées dans {table}")
    except Exception as e:
        conn.rollback() 
        print(f"Erreur lors de l'insertion: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    for season_id in range(2, 7):  # On insere pas la premiere et la derniere saison pour ce modele
        print(f" Génération dataset pour la saison_id {season_id}")
        df = generate_dataset(season_id)
        insert_training_dataset(df)