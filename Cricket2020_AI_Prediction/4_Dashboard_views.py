# Databricks notebook source
# MAGIC %sql
# MAGIC -- Tile 1: Scoreboard
# MAGIC CREATE OR REPLACE VIEW sports.dashboard.match_scoreboard AS
# MAGIC SELECT * FROM sports.cricket_demo.match_summary_gold;
# MAGIC
# MAGIC -- Tile 2: Batter Stats
# MAGIC CREATE OR REPLACE VIEW sports.dashboard.batter_stats AS
# MAGIC SELECT match_id, batter, runs, balls_faced, ROUND(strike_rate,2) AS strike_rate, last_event_time
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY match_id, batter ORDER BY last_event_time DESC) AS rn
# MAGIC   FROM sports.cricket_demo.batter_stats_gold
# MAGIC )
# MAGIC WHERE rn = 1
# MAGIC GROUP BY match_id, batter, runs, balls_faced, strike_rate, last_event_time
# MAGIC ORDER BY runs DESC;
# MAGIC
# MAGIC -- Tile 3: Bowler Economy
# MAGIC CREATE OR REPLACE VIEW sports.dashboard.bowler_economy AS
# MAGIC SELECT match_id, bowler, ROUND(economy,2) AS economy, runs_conceded, balls_bowled
# MAGIC FROM sports.cricket_demo.bowler_economy_gold;
# MAGIC
# MAGIC -- Tile 4: AI Predictions
# MAGIC CREATE OR REPLACE VIEW sports.dashboard.batter_predictions AS
# MAGIC SELECT match_id, batter, strike_rate, lookahead_overs, expected_runs_next_N_overs
# MAGIC FROM sports.cricket_demo.batter_predictions_gold
# MAGIC ORDER BY expected_runs_next_N_overs DESC;
# MAGIC
# MAGIC -- Tile 5: Inning Guidance
# MAGIC CREATE OR REPLACE VIEW sports.dashboard.inning_guidance AS
# MAGIC SELECT match_id, score, wickets, overs, balls_in_over, ROUND(run_rate,2) AS run_rate,
# MAGIC        runs_needed, ROUND(overs_left,2) AS overs_left,
# MAGIC        ROUND(req_rr,2) AS req_rr,
# MAGIC        ROUND(avg_econ,2) AS avg_bowler_economy,
# MAGIC        ROUND(advised_next_over_runs,2) AS advised_next_over_runs
# MAGIC FROM sports.cricket_demo.inning_guidance_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sports.dashboard.inning_guidance

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sports.dashboard.batter_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sports.dashboard.bowler_economy

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sports.dashboard.batter_predictions
