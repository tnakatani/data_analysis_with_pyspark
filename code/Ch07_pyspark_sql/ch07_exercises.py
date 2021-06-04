import logging
import os

import pandas as pd

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf
from pyspark.sql import SparkSession

####################################################################################################
# Setup
####################################################################################################

conf = SparkConf()
conf.set("spark.logConf", "true")
spark = SparkSession.builder.config(conf=conf).appName("Ch07_pyspark_sql Exercises").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logging.getLogger().setLevel(logging.INFO)

####################################################################################################
# Data Ingestion
####################################################################################################

DATA_DIRECTORY = "../../data/Ch07_pyspark_sql/"
backblaze_2019 = spark.read.csv(
    os.path.join(DATA_DIRECTORY, "data_Q3_2019", "2019-07-01.csv"), header=True, inferSchema=True
)

# Setting the layout for each column according to the schema
q = backblaze_2019.select(
    [
        F.col(x).cast(T.LongType()) if x.startswith("smart") else F.col(x)
        for x in backblaze_2019.columns
    ]
)

# Register the view
# backblaze_2019.createOrReplaceTempView("backblaze_stats_2019")

full_data = backblaze_2019.selectExpr(
    "model", "capacity_bytes / pow(1024, 3) capacity_GB", "date", "failure"
)

"""full_data output
+--------------------+-----------------+----------+-------+
|               model|      capacity_GB|      date|failure|
+--------------------+-----------------+----------+-------+
|         ST4000DM000|3726.023277282715|2019-07-05|      0|
|       ST12000NM0007|          11176.0|2019-07-05|      0|
|       ST12000NM0007|          11176.0|2019-07-05|      0|
|       ST12000NM0007|          11176.0|2019-07-05|      0|
|HGST HMS5C4040ALE640|3726.023277282715|2019-07-05|      0|
+--------------------+-----------------+----------+-------+
"""


def failure_rate(df):
    drive_days = df.groupby("model", "capacity_GB").agg(F.expr("COUNT(*) AS drive_days"))

    failures = (
        df.where("failure = 1").groupby("model", "capacity_GB").agg(F.expr("COUNT(*) AS failures"))
    )

    result = (
        drive_days.join(failures, on=["model", "capacity_GB"], how="inner")
        .withColumn("failure_rate", F.col("failures") / F.col("drive_days"))
        .orderBy(F.col("failure_rate").desc())
    )
    return result


failure_rate(full_data).show(5)

####################################################################################################
# Exercise 1
#
# Write a summarized_data without having to use another table than full_data and no join
# Columns: model, capacity_GB, failure_rate (failures / drive_days)
####################################################################################################

# summarized_data = (
#     full_data
#     .fillna(0.0, ["failure"])
#     .withColumn(
#         "failures",
#         full_data.where("failure = 1").groupBy("model", "capacity_GB").agg(F.expr("COUNT(*) failures"))
#     )
#     .selectExpr("model", "capacity_GB", "failures")
# )
# )

# summarized_data = spark.sql(
#     """
#     SELECT *
#     FROM (
#         SELECT model, capacity_bytes, COUNT(*) AS drive_days FROM backblaze_stats_2019 GROUP BY model, capacity_bytes
#         UNION ALL
#         SELECT model, capacity_bytes, COUNT(*) AS failures FROM backblaze_stats_2019 WHERE failure = 1 GROUP BY model, capacity_bytes
#     )
# """
# )

# summarized_data.show(5)

"""
+--------------------+-----------------+----------+
|               model|      capacity_GB|drive_days|
+--------------------+-----------------+----------+
|      WDC WD5000LPCX|465.7617416381836|        54|
|         ST6000DM004| 5589.02986907959|         1|
|         ST4000DM005|3726.023277282715|        40|
|HGST HMS5C4040BLE641|3726.023277282715|         1|
|       ST500LM012 HN|465.7617416381836|       503|
+--------------------+-----------------+----------+
"""

####################################################################################################
# Exercise 2
#
# The analysis done through the chapter is flawed in that the age of a drive is not taken into consideration.
# Instead of ordering the drives by failure rate, order them by average age at failure (assume that every
# drive fails on 2020-01-01 if they are still alive at the end of the year).
####################################################################################################

# def failure_rate_by_avg_age(df):
#     drive_days = df.groupby("model", "capacity_GB").agg(
#         F.expr("COUNT(*) AS drive_days")
#     )
#
#     failures = (
#         df.where("failure = 1")
#             .groupby("model", "capacity_GB")
#             .agg(F.expr("COUNT(*) AS failures"))
#     )
#
#     result = (
#         drive_days.join(failures, on=["model", "capacity_GB"], how="inner")
#             .withColumn("failure_rate", F.avg("drive_days"))
#             .orderBy(F.col("failure_rate").desc())
#     )
#     return result
#
# failure_rate_by_avg_age(full_data).show(5)

####################################################################################################
# Exercise 3
#
# What is the total capacity (in TB) that BackBlaze records at the end of the first day?
# Note: 1 kilobyte = 1024 bytes, 1 megabyte = 1024 kilobytes, 1 gigabyte = 1024 megabytes, 1 terabyte = 1024 gigabytes
####################################################################################################


def total_capacity(df):
    return df.selectExpr("SUM(capacity_GB) / pow(1024, 1) AS TOTAL_CAPACITY_TB")


total_capacity(full_data).show(5)


df = pd.read_csv("../../data/ch07/data_Q3_2019/2019-07-01.csv")
print(df["capacity_bytes"].sum() / pow(1024, 4))
