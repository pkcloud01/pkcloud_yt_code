# Databricks notebook source
# --- USER CONFIG ---
CATALOG = "sports"
SCHEMA  = "cricket_demo"
LANDING_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/cricket_landing/balls"  # UC Volume path

# --- Imports & setup ---
import json, time, random
from datetime import datetime
match_id="01"

dbutils.fs.mkdirs(LANDING_DIR)

# Teams (simple demo)
team_a_batters = [f"{match_id}_A_B{i+1}" for i in range(11)]
team_b_bowlers = [f"{match_id}_B_BL{i+1}" for i in range(5)]

match_id = "MATCH_1"
overs = 20
balls_per_over = 6
random.seed(42)

# State
total_runs = 0
total_wickets = 0
on_strike, non_striker, next_batter = 0, 1, 2

def weighted_outcome():
    # (0,1,2,3,4,6,W) with rough probabilities
    options = ["0","1","2","3","4","6","W"]
    weights = [0.30,0.38,0.10,0.02,0.13,0.05,0.02]
    return random.choices(options, weights)[0]

print(f"Streaming JSON to {LANDING_DIR} ... Ctrl+Stop to end.")

for over in range(1, overs+1):
    bowler = team_b_bowlers[(over-1) % len(team_b_bowlers)]
    for ball in range(1, balls_per_over+1):
        if total_wickets >= 10:
            break

        outcome = weighted_outcome()
        runs = 0
        wicket = 0
        dismissal_type = None

        if outcome == "W":
            wicket = 1
            total_wickets += 1
            dismissal_type = random.choice(["bowled","caught","lbw","runout"])
            if next_batter <= 10:
                on_strike = next_batter
                next_batter += 1
            else:
                # All out - end early
                pass
        else:
            runs = int(outcome)
            total_runs += runs
            # strike rotates on odd runs
            if runs % 2 == 1:
                on_strike, non_striker = non_striker, on_strike

        # write one JSON file per ball
        rec = {
            "match_id": match_id,
            "ts_utc": datetime.utcnow().isoformat() + "Z",
            "over": over,
            "ball_in_over": ball,
            "striker": team_a_batters[on_strike],
            "non_striker": team_a_batters[non_striker],
            "bowler": bowler,
            "runs_off_bat": runs,
            "is_wicket": wicket,
            "dismissal_type": dismissal_type,
            "total_runs": total_runs,
            "total_wickets": total_wickets
        }
        file = f"{LANDING_DIR}/ball_{match_id}_{over}_{ball}_{int(time.time()*1000)}.json"
        dbutils.fs.put(file, json.dumps(rec), True)
        print(f"Written {file}")

        # end-of-over strike swap
        if ball == balls_per_over:
            on_strike, non_striker = non_striker, on_strike

        time.sleep(2)  # ~0.5s per ball for demo speed

print("Simulation finished (or you stopped it).")
