import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
from psycopg2.extras import execute_batch

load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

def get_connection():
    conn = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    return conn

def fetch_season_calendar(season_label):
    query = """
        SELECT 
            match_id, date_match, home_team_id, away_team_id
        FROM match_stats
        WHERE season_label = %s
        ORDER BY date_match ASC
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(season_label,))
    conn.close()
    return df

def fetch_team_stats(before_season_label):
    """
    Récupère toutes les stats cumulées des équipes avant la saison donnée.
    """
    query = """
        SELECT *
        FROM team_season
        WHERE season < %s
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(before_season_label,))
    conn.close()
    return df

def fetch_h2h(home_team_id, away_team_id):
    query = """
        SELECT *
            FROM match_stats
        WHERE 
            (home_team_id = %s AND away_team_id = %s)
            OR (home_team_id = %s AND away_team_id = %s)
        ORDER BY date_match DESC
        LIMIT 5
    """
    conn = get_connection()
    df = pd.read_sql(query, conn, params=(home_team_id, away_team_id, away_team_id, home_team_id))
    conn.close()
    return df

def generate_training_dataset(season_label):
    calendar_df = fetch_season_calendar(season_label)
    team_stats_df = fetch_team_stats(season_label)
    team_stats_map = team_stats_df.set_index('team_id').to_dict(orient='index')
    rows = []

    for _, match in calendar_df.iterrows():
        home_id = match['home_team_id']
        away_id = match['away_team_id']

        home_stats = team_stats_map.get(home_id, {})
        away_stats = team_stats_map.get(away_id, {})

        h2h_df = fetch_h2h(home_id, away_id)
        h2h_home_wins = ((h2h_df['home_team_id'] == home_id) & (h2h_df['home_goals'] > h2h_df['away_goals'])).sum()
        h2h_away_wins = ((h2h_df['away_team_id'] == away_id) & (h2h_df['away_goals'] > h2h_df['home_goals'])).sum()
        h2h_draws = ((h2h_df['home_goals'] == h2h_df['away_goals'])).sum()
        h2h_avg_goal_diff_home = ((h2h_df['home_goals'] - h2h_df['away_goals']).mean() if not h2h_df.empty else 0)
        h2h_avg_goals_home_scored = h2h_df['home_goals'].mean() if not h2h_df.empty else 0
        h2h_avg_goals_away_scored = h2h_df['away_goals'].mean() if not h2h_df.empty else 0

        row = {
            'match_id': match['match_id'],
            'season_label': season_label,
            'date_match': match['date_match'],
            'home_team_id': home_id,
            'away_team_id': away_id,

            'points_so_far_home': home_stats.get('points', 0),
            'points_so_far_away': away_stats.get('points', 0),
            'wins_so_far_home': home_stats.get('wins', 0),
            'wins_so_far_away': away_stats.get('wins', 0),
            'goal_diff_so_far_home': home_stats.get('goal_diff', 0),
            'goal_diff_so_far_away': away_stats.get('goal_diff', 0),
            'goals_scored_so_far_home': home_stats.get('goals_scored', 0),
            'goals_scored_so_far_away': away_stats.get('goals_scored', 0),
            'goals_conceded_so_far_home': home_stats.get('goals_conceded', 0),
            'goals_conceded_so_far_away': away_stats.get('goals_conceded', 0),
            'possession_avg_so_far_home': home_stats.get('possession_avg', 0),
            'possession_avg_so_far_away': away_stats.get('possession_avg', 0),
            'shots_on_target_avg_so_far_home': home_stats.get('shots_on_target_avg', 0),
            'shots_on_target_avg_so_far_away': away_stats.get('shots_on_target_avg', 0),
            'fouls_avg_so_far_home': home_stats.get('fouls_avg', 0),
            'fouls_avg_so_far_away': away_stats.get('fouls_avg', 0),
            'passes_avg_so_far_home': home_stats.get('passes_avg', 0),
            'passes_avg_so_far_away': away_stats.get('passes_avg', 0),
            'corners_avg_so_far_home': home_stats.get('corners_avg', 0),
            'corners_avg_so_far_away': away_stats.get('corners_avg', 0),
            'attacks_avg_so_far_home': home_stats.get('attacks_avg', 0),
            'attacks_avg_so_far_away': away_stats.get('attacks_avg', 0),
            'dangerous_attacks_avg_so_far_home': home_stats.get('dangerous_attacks_avg', 0),
            'dangerous_attacks_avg_so_far_away': away_stats.get('dangerous_attacks_avg', 0),

            'h2h_home_wins': h2h_home_wins,
            'h2h_away_wins': h2h_away_wins,
            'h2h_draws': h2h_draws,
            'h2h_avg_goal_diff_home': h2h_avg_goal_diff_home,
            'h2h_avg_goals_home_scored': h2h_avg_goals_home_scored,
            'h2h_avg_goals_away_scored': h2h_avg_goals_away_scored,

            'result': None
        }
        rows.append(row)

    return pd.DataFrame(rows)

def insert_training_dataset(df, table="training_dataset"):
    """
    Insert le dataset généré dans la base.
    """
    columns = list(df.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    columns_sql = ", ".join(columns)
    query = f"""
        INSERT INTO {table} ({columns_sql})
        VALUES ({placeholders})
        ON CONFLICT (match_id) DO NOTHING;
    """
    conn = get_connection()
    cur = conn.cursor()
    rows = [tuple(r) for r in df.to_numpy()]
    execute_batch(cur, query, rows, page_size=50)
    cur.close()
    conn.close()

if __name__ == "__main__":
    seasons = ["2019/2020", "2020/2021", "2021/2022", "2022/2023", "2023/2024", "2024/2025"]
    for season_label in seasons:
        print(f"📦 Génération dataset pour la saison {season_label}")
        df = generate_training_dataset(season_label)
        insert_training_dataset(df)
        print(f"✅ Saison {season_label} insérée dans la base")
