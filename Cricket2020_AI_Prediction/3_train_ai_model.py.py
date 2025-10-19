# Databricks notebook source
# Databricks notebook: 03_train_ai_model
# ---------------------------------------
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Simple mock training data
data = [
    (100.0, 7.0, 12.0),
    (120.0, 8.0, 14.0),
    (140.0, 9.0, 16.0),
    (90.0, 6.0, 10.0),
    (110.0, 7.5, 13.0),
]
df = spark.createDataFrame(data, ["strike_rate","bowler_economy","next_over_runs"])

# We'll just compute coefficients manually instead of using MLlib
# approximate linear regression by solving via pandas/numpy

import numpy as np
X = df.select("strike_rate","bowler_economy").toPandas()
y = df.select("next_over_runs").toPandas().values.ravel()

X_np = np.column_stack([np.ones(len(X)), X.values])  # add intercept term
beta = np.linalg.inv(X_np.T.dot(X_np)).dot(X_np.T).dot(y)

intercept = float(beta[0])
coef_sr = float(beta[1])
coef_econ = float(beta[2])

model = [(intercept, coef_sr, coef_econ)]
model_df = spark.createDataFrame(model, ["intercept","coef_sr","coef_econ"])

# Save to Delta table
CATALOG = "sports"
SCHEMA  = "cricket_demo"
MODEL_TABLE = f"{CATALOG}.{SCHEMA}.ai_model_coefficients"

model_df.write.mode("overwrite").format("delta").saveAsTable(MODEL_TABLE)

display(spark.table(MODEL_TABLE))
print(f"✅ Model coefficients saved to {MODEL_TABLE}")
