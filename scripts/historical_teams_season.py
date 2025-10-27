import sys
import os
import pandas as pd

# Ajouter le dossier racine du projet au PYTHONPATH
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from etl.load.update_team_season import upsert_teams_season

def calculate_team_stats(fixtures_df, season_label):
    """
    Calcule les statistiques par équipe pour une saison.
    Renvoie un DataFrame prêt à insérer dans team_season1.
    """
    teams = pd.unique(fixtures_df[['home_team', 'away_team']].values.ravel('K'))
    stats_rows = []

    for team in teams:
        # Séparer les matchs à domicile et à l'extérieur
        home_matches = fixtures_df[fixtures_df['home_team'] == team]
        away_matches = fixtures_df[fixtures_df['away_team'] == team]

        played = len(home_matches) + len(away_matches)
        home_wins = (home_matches['home_goals'] > home_matches['away_goals']).sum()
        away_wins = (away_matches['away_goals'] > away_matches['home_goals']).sum()
        home_points = home_wins * 3  # On peut ajuster si on veut inclure draws
        points = home_points + away_wins * 3

        home_goal_diff = (home_matches['home_goals'] - home_matches['away_goals']).sum()
        away_goal_diff = (away_matches['away_goals'] - away_matches['home_goals']).sum()

        # Moyennes des statistiques
        def mean_stat(col_home, col_away):
            h = home_matches[col_home].mean() if not home_matches.empty else 0
            a = away_matches[col_away].mean() if not away_matches.empty else 0
            return h, a

        home_possession, away_possession = mean_stat('home_possession', 'away_possession')
        home_shots_on_target, away_shots_on_target = mean_stat('home_shots_on_target', 'away_shots_on_target')
        home_fouls, away_fouls = mean_stat('home_fouls', 'away_fouls')
        home_passes, away_passes = mean_stat('home_passes', 'away_passes')
        home_corners, away_corners = mean_stat('home_corners', 'away_corners')
        home_attacks, away_attacks = mean_stat('home_attacks', 'away_attacks')
        home_dangerous_attacks, away_dangerous_attacks = mean_stat('home_dangerous_attacks', 'away_dangerous_attacks')

        stats_rows.append({
            'team': team,
            'season': season_label,
            'home_points': home_points,
            'points': points,
            'home_goal_difference': home_goal_diff,
            'away_goal_difference': away_goal_diff,
            'played': played,
            'home_wins': home_wins,
            'away_wins': away_wins,
            'home_possession': round(home_possession, 2),
            'away_possession': round(away_possession, 2),
            'home_shots_on_target': round(home_shots_on_target, 2),
            'away_shots_on_target': round(away_shots_on_target, 2),
            'home_fouls': round(home_fouls, 2),
            'away_fouls': round(away_fouls, 2),
            'home_passes': round(home_passes, 2),
            'away_passes': round(away_passes, 2),
            'home_corners': round(home_corners, 2),
            'away_corners': round(away_corners, 2),
            'home_attacks': round(home_attacks, 2),
            'away_attacks': round(away_attacks, 2),
            'home_dangerous_attacks': round(home_dangerous_attacks, 2),
            'away_dangerous_attacks': round(away_dangerous_attacks, 2)
        })

    return pd.DataFrame(stats_rows)


if __name__ == "__main__":
    fixtures_df = pd.read_csv("data/processed/fixtures_stats.csv")

    seasons = fixtures_df["season_label"].unique()
    for season_label in seasons:
        season_df = fixtures_df[fixtures_df["season_label"] == season_label]
        team_stats_df = calculate_team_stats(season_df, season_label)
        upsert_teams_season(team_stats_df)
        print(f" Saison {season_label} traitée et insérée dans team_season")
