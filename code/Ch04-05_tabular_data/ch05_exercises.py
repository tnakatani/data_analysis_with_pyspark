import logging

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()
conf.set("spark.logConf", "true")
spark = SparkSession.builder.config(conf=conf).appName("Ch05 Exercises").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logging.getLogger().setLevel(logging.INFO)

####################################################################################################
# Read File
####################################################################################################

DATA_PATH = "data/ch05/part-00000-edcccfb7-fcd0-41a3-967c-a77d98a0e3bb-c000.csv"
full_log = spark.read.csv(
    DATA_PATH,
    sep="|",
    quote='"',
    header=True,
    inferSchema=True,
)

####################################################################################################
# Exercises
####################################################################################################

"""Exercise 1
Using the data from the data/Ch04/Call_Signs.csv, add the Undertaking_Name to our final table to 
display a human-readable description of the channel.
"""

cs = spark.read.csv(
    "data/Ch04/Call_Signs.csv",
    sep=",",
    quote='"',
    header=True,
    inferSchema=True,
).drop("UndertakingNO")

full_log = full_log.join(cs, on="LogIdentifierID", how="left")
assert "Undertaking_Name" in full_log.columns

# un_counts = full_log.groupby("UnderTaking_Name").count().orderBy("count", ascending=False)
# logging.info("Undertaking Counts:")
# logging.info(un_counts.show(20, False))

"""Exercise 2
The government of Canada is asking for your analysis, but they’d like the PRC to be weighted 
differently. They’d like each PRC second to be considered 0.75 commercial second. Modify the 
program to account for this change.
"""

# Logic for summing commercial time, with exception to PRC
is_commercial = F.when(
    F.trim(F.col("ProgramClassCD")).isin(["COM", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]),
    F.col("duration_seconds"),
).when(
    F.trim(F.col("ProgramClassCD")).isin(["PRC"]),
    F.col("duration_seconds") * 0.75,
)

# Groupby and calc commercial ratio
commercial_time = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(is_commercial).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn("commercial_ratio", F.col("duration_commercial") / F.col("duration_total"))
    .withColumn("commercial_ratio_percent", F.round(F.col("commercial_ratio") * 100, 1))
)

# Show results
# commercial_time.orderBy("commercial_ratio_percent", ascending=False).show(20, False)


"""Exercise 3
On the data frame returned from commercials.py, return the percentage of channels in each bucket 
based on their commercial_ratio. (Hint: look at the documentation for round in how to truncate a value.)
"""

commercial_ratio_binned = (
    commercial_time.groupby(F.round(F.col("commercial_ratio"), 1))
    .count()
    .withColumnRenamed("round(commercial_ratio, 1)", "commercial_ratio")
)

commercial_ratio_binned.orderBy("commercial_ratio", ascending=False).show()
