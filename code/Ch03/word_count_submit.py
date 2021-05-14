import logging
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Analyzing the vocabulary of Pride and Prejudice."
).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

READ_PATH = "/Users/taichinakatani/learn/data_analysis_with_pyspark/data"
results = (
    spark.read.text(os.path.join(READ_PATH, "ch02", "*.txt"))
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby("word")
    .count()
    .orderBy("count", ascending=False)
)

# Show top 20 counts
results.show()

# Write all chapter 2 text word counts to one csv file
outdir = "output/all_ch2_text"
logging.info(f"Writing aggregation to {outdir}")
results.coalesce(1).write.mode("overwrite").csv("output/all_ch2_text")

# return a sample of 20 words that appear only once in Jane Austen’s Pride and Prejudice
logging.info("Words that only appear once:")
word_only_appearing_once = results.where(F.col("count") == 1)
word_only_appearing_once.show()

#  Using the substr function (refer to PySpark’s API or the pyspark shell help if needed),
#  return the top 5 most popular first letters (keep only the first letter of each word).


# Compute the number of words starting with a consonant or a vowel. (Hint: the isin() function
# might be useful)


