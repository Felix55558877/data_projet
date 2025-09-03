import requests
import json
import csv
from dotenv import load_dotenv
import os

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")  # ⚠️ Mets ton token dans le .env
BASE_URL = "https://api.sportmonks.com/v3/football/fixtures"

SEASONS = {
    16036: "2019/2020",
    17420: "2020/2021",
    18378: "2021/2022",
    19734: "2022/2023",
    21646: "2023/2024",
    23614: "2024/2025",
    25583: "2025/2026",
}

TYPE_MAP = {
    34: "Corners",
    43: "Attacks",
    44: "Dangerous Attacks",
    45: "Ball Possession",
    52: "Goals",
    56: "Fouls",
    80: "Passes",
    86: "Shots on Target",
}

# Charger le mapping des noms avec gestion d'erreur
NAME_MAPPING = {}
try:
    with open("scripts/team_name_mapping.json", "r", encoding="utf-8") as f:
        NAME_MAPPING = json.load(f)
    print(f"✅ Mapping chargé: {len(NAME_MAPPING)} équipes mappées")
except Exception as e:
    print(f"❌ Erreur chargement mapping: {e}")
    exit(1)


def get_team_name(team_name):
    """Retourne le nom mappé d'une équipe"""
    return NAME_MAPPING.get(team_name, team_name)


def fetch_fixtures_for_season(season_id, season_label):
    fixtures = []
    url = BASE_URL
    params = {
        "api_token": API_TOKEN,
        "filters": f"fixtureSeasons:{season_id};fixtureLeagues:8",
        "include": "participants;statistics"
    }

    while url:
        print(f"Fetching season {season_label}: {url}")
        resp = requests.get(url, params=params)
        data = resp.json()

        if "data" not in data:
            print("⚠️ Pas de clé 'data' dans la réponse API :", data)
            break

        for fixture in data["data"]:
            fixture["season_label"] = season_label
            fixtures.append(fixture)

        # Pagination
        pagination = data.get("pagination", {})
        if pagination.get("has_more"):
            url = pagination.get("next_page")
            params = {"api_token": API_TOKEN}
        else:
            url = None

    return fixtures


def safe_value(value):
    if value is None:
        return 0
    try:
        return float(value)
    except:
        return 0


def extract_fixture_data(fixture):
    try:
        fixture_id = fixture.get("id")
        season_id = str(fixture.get("season_id", ""))
        date = fixture.get("starting_at")

        participants = fixture.get("participants", [])
        home = next((p for p in participants if p.get("meta", {}).get("location") == "home"), {})
        away = next((p for p in participants if p.get("meta", {}).get("location") == "away"), {})

        home_team_name = get_team_name(home.get("name", ""))
        away_team_name = get_team_name(away.get("name", ""))

        stats = fixture.get("statistics", [])
        stat_map = {}
        for stat in stats:
            team_id = stat.get("participant_id")
            type_id = stat.get("type_id")
            value = stat.get("data", {}).get("value")
            if team_id and type_id:
                stat_map[(team_id, type_id)] = value

        def get_stat(team, stat_name):
            if not team or "id" not in team:
                return None
            for tid, name in TYPE_MAP.items():
                if name == stat_name:
                    return stat_map.get((team.get("id"), tid))
            return None

        scores = fixture.get("scores", {})
        home_goals = scores.get("localteam_score") or get_stat(home, "Goals")
        away_goals = scores.get("visitorteam_score") or get_stat(away, "Goals")

        row = [
            fixture_id,
            season_id,
            fixture.get("season_label", ""),
            date,
            home_team_name,
            away_team_name,
            int(safe_value(home_goals)),
            int(safe_value(away_goals)),
            int(safe_value(get_stat(home, "Ball Possession"))),
            int(safe_value(get_stat(away, "Ball Possession"))),
            int(safe_value(get_stat(home, "Shots on Target"))),
            int(safe_value(get_stat(away, "Shots on Target"))),
            int(safe_value(get_stat(home, "Fouls"))),
            int(safe_value(get_stat(away, "Fouls"))),
            int(safe_value(get_stat(home, "Passes"))),
            int(safe_value(get_stat(away, "Passes"))),
            int(safe_value(get_stat(home, "Corners"))),
            int(safe_value(get_stat(away, "Corners"))),
            int(safe_value(get_stat(home, "Attacks"))),
            int(safe_value(get_stat(away, "Attacks"))),
            int(safe_value(get_stat(home, "Dangerous Attacks"))),
            int(safe_value(get_stat(away, "Dangerous Attacks"))),
            1  # adv_home = toujours 1 dans ce cas
        ]
        return row
    except Exception as e:
        print(f"❌ Erreur extraction fixture {fixture.get('id')}: {e}")
        return None


if __name__ == "__main__":
    all_rows = []
    for season_id, season_label in SEASONS.items():
        print(f"\n🔍 Traitement de la saison {season_label}...")
        fixtures = fetch_fixtures_for_season(season_id, season_label)
        for fixture in fixtures:
            row = extract_fixture_data(fixture)
            if row:
                all_rows.append(row)

    # Sauvegarde en CSV
    with open("fixtures_stats.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "fixture_id", "season_id", "season_label", "date_match",
            "home_team", "away_team",
            "home_goals", "away_goals",
            "home_possession", "away_possession",
            "home_shots_on_target", "away_shots_on_target",
            "home_fouls", "away_fouls",
            "home_passes", "away_passes",
            "home_corners", "away_corners",
            "home_attacks", "away_attacks",
            "home_dangerous_attacks", "away_dangerous_attacks",
            "adv_home"
        ])
        writer.writerows(all_rows)

    print(f"\n✅ {len(all_rows)} fixtures sauvegardées dans fixtures_stats.csv")
