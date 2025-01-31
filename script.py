import pandas as pd
import json
import os
import mysql.connector
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

# Create MySQL connection pool for efficiency
def get_db_engine():
    return create_engine("mysql+mysqlconnector://root:root@localhost/cricket_dataset_practice", pool_size=10, max_overflow=5)

def insert_into_database(df, table_name):
    """ Inserts DataFrame into MySQL using batch insert for speed. """
    if df.empty:
        return  

    try:
        engine = get_db_engine()
        df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi', chunksize=2000)
    except Exception as e:
        print(f"❌ Error inserting into {table_name}: {e}")

def process_json_file(filepath):
    """ Process a single JSON file and insert into MySQL """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            matchID = os.path.basename(filepath).split('.')[0]  

            d = data.get('info', {})
            registry = d.get('registry', {}).get('people', {})

            # ✅ Insert `registry` first (Bulk Insert)
            if registry:
                df_registry = pd.DataFrame({
                    "person_id": list(registry.values()),
                    "person_name": list(registry.keys())
                }).drop_duplicates()
                insert_into_database(df_registry, "registry")

            # ✅ Insert `match_details`
            match = {
                "match_id": matchID,
                "city": d.get('city', None),
                "gender": d.get('gender', None), 
                "match_type": d.get('match_type', None), 
                "match_type_number": d.get('match_type_number', None), 
                "overs": d.get('overs', None), 
                "season": d.get('season', None), 
                "team_type": d.get('team_type', None), 
                "venue": d.get('venue', None),
                "team1": d.get("teams", [None, None])[0],
                "team2": d.get("teams", [None, None])[1],
                "toss_winner": d.get("toss", {}).get("winner", None),
                "toss_decision": d.get("toss", {}).get("decision", None),
                "winner": d.get("outcome", {}).get("winner", None), 
                "outcome_type": json.dumps(d.get("outcome", {}).get("by", {})) if isinstance(d.get("outcome", {}).get("by"), dict) else d.get("outcome", {}).get("by"),
                "player_of_match": ", ".join([registry.get(i, "Unknown") for i in d.get("player_of_match", [])]),
                "balls_per_over": d.get("balls_per_over", None)
            }
            df_match_details = pd.DataFrame([match])
            insert_into_database(df_match_details, "match_details")

            # ✅ Insert `officials`
            df_officials = pd.DataFrame([
                {"match_id": matchID, "person_id": registry.get(person, None), "official_type": off}
                for off in d.get('officials', {}) for person in d['officials'][off]
            ])
            insert_into_database(df_officials, "officials")

            # ✅ Insert `players`
            df_players = pd.DataFrame([
                {"match_id": matchID, "person_id": registry.get(player, None), "team_name": team}
                for team, players in d.get('players', {}).items() for player in players
            ])
            insert_into_database(df_players, "players")

            # ✅ Insert `deliveries` (Bulk Insert)
            deliveries = []
            for inning_count, inning in enumerate(data.get('innings', []), start=1):
                for over in inning.get('overs', []):
                    for ball_count, delivery in enumerate(over.get('deliveries', []), start=1):
                        deliveries.append({
                            "match_id": matchID,
                            "innings": inning_count,
                            "team": inning.get("team"),
                            "overs": over.get("over"),
                            "balls": ball_count,
                            "batter": registry.get(delivery.get("batter")),
                            "bowler": registry.get(delivery.get("bowler")),
                            "non_striker": registry.get(delivery.get("non_striker")),
                            "runs_batter": delivery.get("runs", {}).get("batter"),
                            "runs_extras": delivery.get("runs", {}).get("extras"),
                            "runs_total": delivery.get("runs", {}).get("total"),
                            "player_out": registry.get(delivery.get("wickets", [{}])[0].get("player_out")),
                            "dismissal_kind": delivery.get("wickets", [{}])[0].get("kind")
                        })
            df_deliveries = pd.DataFrame(deliveries)
            insert_into_database(df_deliveries, "deliveries")

            return matchID  

    except Exception as e:
        print(f"❌ Error processing {filepath}: {e}")
        return None

def process_all_json_files_parallel(matches_path, formats, num_workers=50):
    """ Process JSON files in parallel using ThreadPoolExecutor. """
    all_files = []

    for format_name in formats:
        format_path = os.path.join(matches_path, format_name)
        if os.path.isdir(format_path):
            files = [os.path.join(format_path, file) for file in os.listdir(format_path) if file.endswith(".json")]
            all_files.extend(files)

    # ✅ Process files in parallel (limit to `num_workers` threads)
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(process_json_file, all_files))

    print(f"✅ Processed {len([r for r in results if r is not None])} matches successfully!")

# Example Usage:
matches_path = "matches"
formats = ["odis_json", "t20s_json", "tests_json"]
process_all_json_files_parallel(matches_path, formats, num_workers=50)