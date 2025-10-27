import pandas as pd

def calculate_team_stats(fixtures_df, season_label):
    """
    Calcule les stats par équipe pour une saison.
    - Moyennes pour stats domicile et extérieur
    - Points = 3*wins + 1*draw
    """
    teams = set(fixtures_df["home_team"]).union(set(fixtures_df["away_team"]))
    data = []
    
    for team in teams:
        home_games = fixtures_df[fixtures_df["home_team"] == team]
        away_games = fixtures_df[fixtures_df["away_team"] == team]
        
        home_points = (home_games["home_goals"] > home_games["away_goals"]).sum() * 3 + \
                      (home_games["home_goals"] == home_games["away_goals"]).sum()
        away_points = (away_games["away_goals"] > away_games["home_goals"]).sum() * 3 + \
                      (away_games["away_goals"] == away_games["home_goals"]).sum()
        total_points = home_points + away_points
        
        home_wins = (home_games["home_goals"] > home_games["away_goals"]).sum()
        away_wins = (away_games["away_goals"] > away_games["home_goals"]).sum()
        
        played = len(home_games) + len(away_games)
        
        row = {
            "Team": team,
            "Season": season_label,
            "home_points": home_points,
            "points": total_points,
            "home_goal_difference": (home_games["home_goals"] - home_games["away_goals"]).sum(),
            "away_goal_difference": (away_games["away_goals"] - away_games["home_goals"]).sum(),
            "played": played,
            "home_wins": home_wins,
            "away_wins": away_wins,
            "home_possession": home_games["home_possession"].mean() if not home_games.empty else 0,
            "away_possession": away_games["away_possession"].mean() if not away_games.empty else 0,
            "home_shots_on_target": home_games["home_shots_on_target"].mean() if not home_games.empty else 0,
            "away_shots_on_target": away_games["away_shots_on_target"].mean() if not away_games.empty else 0,
            "home_fouls": home_games["home_fouls"].mean() if not home_games.empty else 0,
            "away_fouls": away_games["away_fouls"].mean() if not away_games.empty else 0,
            "home_passes": home_games["home_passes"].mean() if not home_games.empty else 0,
            "away_passes": away_games["away_passes"].mean() if not away_games.empty else 0,
            "home_corners": home_games["home_corners"].mean() if not home_games.empty else 0,
            "away_corners": away_games["away_corners"].mean() if not away_games.empty else 0,
            "home_attacks": home_games["home_attacks"].mean() if not home_games.empty else 0,
            "away_attacks": away_games["away_attacks"].mean() if not away_games.empty else 0,
            "home_dangerous_attacks": home_games["home_dangerous_attacks"].mean() if not home_games.empty else 0,
            "away_dangerous_attacks": away_games["away_dangerous_attacks"].mean() if not away_games.empty else 0,
        }
        data.append(row)
    
    df = pd.DataFrame(data)
    df["position"] = df["points"].rank(ascending=False, method="min").astype(int)
    return df
