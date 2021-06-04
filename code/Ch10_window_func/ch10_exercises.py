import logging
import os
from functools import reduce

import pandas as pd

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

####################################################################################################
# Setup
####################################################################################################

conf = SparkConf()
conf.set("spark.logConf", "true")
spark = SparkSession.builder.config(conf=conf).appName("Ch10 Exercises").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logging.getLogger().setLevel(logging.INFO)


spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Read temperature data from local parquet
gsod = (
    reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True),
        [spark.read.parquet(f"../../data/gsod_noaa/gsod{i}.parquet") for i in range(2015, 2018)],
    )
    .dropna(subset=["year", "mo", "da", "temp"])
    .where(F.col("temp") != 9999.9)
    .drop("date")
)
# gsod = spark.read.parquet(data)

"""
# Calculate coldest day per year
each_year = Window.partitionBy("year")

gsod.withColumn("min_temp", F.min("temp").over(each_year)).where(
    "temp = min_temp"
).select("year", "mo", "da", "stn", "temp").orderBy(
    "year", "mo", "da"
).show()
"""

####################################################################################################
# Exercise 1
#
# Identify station/day with warmest temperature, then with the average temperature.
####################################################################################################

each_year = Window.partitionBy("year")

# Get max temperature each year
# logging.info("Date and station with highest temperature for each year")
# gsod.withColumn("max_temp", F.max("temp").over(each_year)).where("temp = max_temp").select(
#     "year", "mo", "da", "stn", "temp"
# ).orderBy("year", "mo", "da").show()

# Get station with warmest average temperature for each year.
logging.info("Date and station with highest average temperature for each year")
gsod_highest_avg = (
    gsod.withColumn("avg_temp", F.avg("temp").over(each_year))
    .select(
        "year", "stn", "avg_temp"
    )
    .orderBy("year")
)
gsod_highest_avg.show()