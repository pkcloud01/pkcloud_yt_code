# Databricks notebook: 02_dlt_cricket_pipeline
# Purpose: DLT declarative pipeline for cricket match simulation with AI predictions
# Author: PK
# -------------------------------------------------------------------------------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------------------------------------------------------
# CONFIGURATION (All values can be overridden via DLT Pipeline settings)
# -------------------------------------------------------------------------------
CATALOG = spark.conf.get("demo.catalog", "sports")
SCHEMA  = spark.conf.get("demo.schema", "cricket_demo")

LANDING_DIR = spark.conf.get("demo.landing_dir",
    f"/Volumes/{CATALOG}/{SCHEMA}/cricket_landing/balls")
SYSTEM_DIR  = spark.conf.get("demo.system_dir",
    f"/Volumes/{CATALOG}/{SCHEMA}/cricket_system")

LOOKAHEAD_OVERS = int(spark.conf.get("demo.lookahead_overs", "3"))
NOTIONAL_TARGET = int(spark.conf.get("demo.notional_target", "160"))

# -------------------------------------------------------------------------------
# BRONZE LAYER - Raw ingestion from UC Volume using Auto Loader
# -------------------------------------------------------------------------------
@dlt.table(
    name="balls_bronze",
    comment="Raw ball-by-ball JSON data from UC Volume via Auto Loader."
)
def balls_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{SYSTEM_DIR}/schema/balls_bronze")
        .load(LANDING_DIR)
        .withColumn("ingest_time", current_timestamp())
    )

# -------------------------------------------------------------------------------
# SILVER LAYER - Cleaned & typed
# -------------------------------------------------------------------------------
ball_schema = StructType([
    StructField("match_id", StringType()),
    StructField("ts_utc", StringType()),
    StructField("over", IntegerType()),
    StructField("ball_in_over", IntegerType()),
    StructField("striker", StringType()),
    StructField("non_striker", StringType()),
    StructField("bowler", StringType()),
    StructField("runs_off_bat", IntegerType()),
    StructField("is_wicket", IntegerType()),
    StructField("dismissal_type", StringType()),
    StructField("total_runs", IntegerType()),
    StructField("total_wickets", IntegerType())
])

@dlt.view(name="balls_silver_view", comment="Cleansed, typed streaming records.")
def balls_silver_view():
    df = dlt.read_stream("balls_bronze")
    df = df.select([col(c).cast(ball_schema[c].dataType) if c in [f.name for f in ball_schema] else col(c)
                    for c in df.columns])
    df = df.withColumn("event_time", coalesce(to_timestamp("ts_utc"), col("ingest_time")))
    dlt.expect_or_drop("valid_over", "over BETWEEN 1 AND 20")
    dlt.expect_or_drop("valid_ball", "ball_in_over BETWEEN 1 AND 6")
    dlt.expect_or_drop("valid_runs", "runs_off_bat BETWEEN 0 AND 6")
    dlt.expect_or_drop("valid_wicket", "is_wicket IN (0,1)")
    return df

@dlt.table(name="balls_silver", comment="Validated typed records.")
def balls_silver():
    return dlt.read_stream("balls_silver_view")

# -------------------------------------------------------------------------------
# GOLD LAYER - Aggregated facts and summaries
# -------------------------------------------------------------------------------
@dlt.table(name="match_summary_gold", comment="Match scoreboard summary.")
def match_summary_gold():
    df = dlt.read_stream("balls_silver").select(
        "match_id", "total_runs", "total_wickets", "over", "ball_in_over", "event_time"
    )
    latest = (df.groupBy("match_id")
        .agg(max(struct("event_time","total_runs","total_wickets","over","ball_in_over")).alias("m"))
        .select(
            col("match_id"),
            col("m.total_runs").alias("score"),
            col("m.total_wickets").alias("wickets"),
            col("m.over").alias("overs"),
            col("m.ball_in_over").alias("balls_in_over")
        ))
    balls_bowled = (col("overs")-1)*6 + col("balls_in_over")
    return (latest
        .withColumn("balls_bowled", balls_bowled)
        .withColumn("overs_float", col("balls_bowled")/lit(6.0))
        .withColumn("run_rate", when(col("overs_float")>0, col("score")/col("overs_float")).otherwise(lit(0.0))))

@dlt.table(name="batter_stats_gold", comment="Cumulative runs, balls, strike rate per batter.")
def batter_stats_gold():
    df = dlt.read_stream("balls_silver").select("match_id","striker","runs_off_bat","event_time")
    agg = (df.groupBy("match_id","striker")
           .agg(sum("runs_off_bat").alias("runs"),
                count("*").alias("balls_faced"),
                max("event_time").alias("last_event_time"))
           .withColumn("strike_rate", when(col("balls_faced")>0, col("runs")*100.0/col("balls_faced")).otherwise(0)))
    return agg.withColumnRenamed("striker","batter")

@dlt.table(name="bowler_economy_gold", comment="Bowler economy per over.")
def bowler_economy_gold():
    df = dlt.read_stream("balls_silver").select("match_id","bowler","runs_off_bat")
    econ = (df.groupBy("match_id","bowler")
            .agg(sum("runs_off_bat").alias("runs_conceded"),
                 count("*").alias("balls_bowled"))
            .withColumn("overs_bowled", col("balls_bowled")/lit(6.0))
            .withColumn("economy", when(col("overs_bowled")>0, col("runs_conceded")/col("overs_bowled")).otherwise(None)))
    return econ

# -------------------------------------------------------------------------------
# GOLD + AI - Predictions using pre-trained coefficients (DLT-safe)
# Replace @dlt.table with @dlt.materialized_view for batter_predictions_gold

@dlt.materialized_view(
    name="batter_predictions_gold",
    comment="Predicted runs next N overs per batter using static coefficients."
)
def batter_predictions_gold():
    bats = dlt.read("batter_stats_gold").alias("b")
    bowl = dlt.read("bowler_economy_gold").alias("bo")

    best = bowl.groupBy("match_id").agg(min("economy").alias("best_economy"))
    joined = bats.join(best, "match_id", "left").withColumn("best_economy", coalesce(col("best_economy"), lit(8.0)))

    model_tbl = spark.read.table(f"{CATALOG}.{SCHEMA}.ai_model_coefficients").limit(1)
    model_joined = joined.crossJoin(model_tbl)

    preds = (model_joined
             .withColumn(
                 "expected_runs_next_N_overs",
                 (col("intercept")
                  + col("b.strike_rate") * col("coef_sr")
                  + col("best_economy")  * col("coef_econ")) * lit(LOOKAHEAD_OVERS)
             )
             .select(
                 "match_id",
                 col("b.batter").alias("batter"),
                 col("b.runs").alias("current_runs"),
                 "b.balls_faced",
                 round(col("b.strike_rate"),2).alias("strike_rate"),
                 round(col("best_economy"),2).alias("assumed_bowler_economy"),
                 lit(LOOKAHEAD_OVERS).alias("lookahead_overs"),
                 round(col("expected_runs_next_N_overs"),2).alias("expected_runs_next_N_overs")
             ))
    return preds
# -------------------------------------------------------------------------------
# GOLD + STRATEGY - Required RR vs Bowler Economy
# -------------------------------------------------------------------------------
# Use a materialized view and dlt.read for inning_guidance_gold

@dlt.materialized_view(
    name="inning_guidance_gold",
    comment="Required run rate and next-over advice."
)
def inning_guidance_gold():
    summary = dlt.read("match_summary_gold")
    econ = dlt.read("bowler_economy_gold").groupBy("match_id").agg(avg("economy").alias("avg_econ"))

    s = (
        summary.join(econ, "match_id", "left")
        .withColumn("overs_bowled", (col("overs")-1)+col("balls_in_over")/6.0)
        .withColumn("overs_left", lit(20.0)-col("overs_bowled"))
        .withColumn("runs_needed", lit(NOTIONAL_TARGET)-col("score"))
        .withColumn("req_rr", when(col("overs_left")>0, col("runs_needed")/col("overs_left")).otherwise(0))
        .withColumn(
            "advised_next_over_runs",
            greatest(round(col("req_rr"),2), round(coalesce(col("avg_econ"),lit(8.0)),2))
        )
        .select(
            "match_id","score","wickets","overs","balls_in_over","run_rate",
            "runs_needed","overs_left","req_rr","avg_econ","advised_next_over_runs"
        )
    )
    return s