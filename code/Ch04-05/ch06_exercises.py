import logging

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession

from ch06_schema import shows_schema

conf = SparkConf()
conf.set("spark.logConf", "true")
spark = SparkSession.builder.config(conf=conf).appName("Ch06 Exercises").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logging.getLogger().setLevel(logging.INFO)

# Import a single JSON document using handmade schema
shows = spark.read.json(
    "../../data/ch06/shows-silicon-valley.json", schema=shows_schema, mode="FAILFAST"
)
# shows.printSchema()

"""Exercise 1
Taking the shows data frame, extract the airdate and name of each episode in an array column each.
"""
# airdate_and_name = shows.select(
#     "_embedded.episodes.airdate",
#     "_embedded.episodes.name",
# )
# airdate_and_name = (
#     airdate_and_name.withColumn("new", F.arrays_zip("airdate", "name"))
#     .withColumn("new", F.explode("new"))
#     .select(F.col("new.airdate").alias("airdate"), F.col("new.name").alias("name"))
# )
# airdate_and_name.show(5, False)


"""Exercise 2
Starting with the shows schema, create a new column under _embedded.episodes that contains the 
length (hint: F.length()) of each episodes' name.
"""
# shows.printSchema()
#
# # TODO: Refactor so episode_name_length is within episodes struct
# episodes = shows.select("id", F.explode("_embedded.episodes").alias("episodes"))
# episodes = episodes.withColumn(
#     "episode_name_length",
#     F.length("episodes.name").alias("episode_name_length")
# )
# episodes.printSchema()

"""Exercise 3
Using three_shows, compute the time between the first and last episodes for each show. 
Which show had the longest tenure?
"""


def calc_interval(col):
    return F.array_max(col) - F.array_min(col)


# Read all JSONs
three_shows = spark.read.json(
    "../../data/ch06/shows-*.json", schema=shows_schema, multiLine=True, mode="FAILFAST"
)
# Calc date interval
show_runtime = three_shows.select(
    "name",
    calc_interval("_embedded.episodes.airdate").cast("string").alias("show_tenure"),
)
# Write to file
show_runtime.coalesce(1).write.mode("overwrite").options(delimiter="\t", header=True).csv(
    "./ch06_output/shows_runtime.tsv"
)

"""Exercise 4
Given the following data frame, create a new data frame that contains a single map from one to square.
"""
numbers = spark.createDataFrame([[1, 2], [2, 4], [3, 9]], ["one", "square"])
numbers = numbers.select(
    F.array(F.col("one")).alias("one"), F.array(F.col("square")).alias("square")
)
numbers.select(F.map_from_arrays("one", "square").alias("square_map")).show()
