import logging
from fractions import Fraction
from operator import add
from typing import Optional, Tuple

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf
from pyspark.sql import SparkSession

####################################################################################################
# Setup
####################################################################################################

conf = SparkConf()
conf.set("spark.logConf", "true")
spark = SparkSession.builder.config(conf=conf).appName("Ch08_udf Exercises").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logging.getLogger().setLevel(logging.INFO)


####################################################################################################
# Exercise 1
# What is the return value of the following:
# a_rdd = sc.parallelize([0, 1, None, [], 0.0])
####################################################################################################

sc = spark.sparkContext
a_rdd = sc.parallelize([0, 1, None, [], 0.0])
print(a_rdd.filter(lambda x: x).collect())
# Answer: [1]


####################################################################################################
# Exercise 2
#
# Using following definitions, create a temp_to_temp(value, from, to) that takes a numerical value in `from`
# degrees and converts to `to` degrees.
# C = (F - 32) * 5 / 9 (Celcius)
# K = C + 273.15 (Kelvin)
# R = F + 459.67 (Rankine)
####################################################################################################


def temp_to_temp(value, _from, _to) -> Optional[int]:
    if (_from == "fahrenheit") and (_to == "celsius"):
        result = (value - 32) * 5 / 9
    elif (_from == "celsius") and (_to == "kelvin"):
        result = value + 273.15
    elif (_from == "fahrenheit") and (_to == "rankine"):
        result = value + 459.67
    return result


print(temp_to_temp(50, "fahrenheit", "celsius"))
print(temp_to_temp(10, "celsius", "kelvin"))


####################################################################################################
# Exercise 3
#
# Create a UDF that adds two fractions together, and
# test it by adding the reduced_fraction to itself in the frac_df data frame.
####################################################################################################

# Setup
fractions = [[x, y] for x in range(100) for y in range(1, 100)]
frac_df = spark.createDataFrame(fractions, ["numerator", "denominator"])
frac_df = frac_df.select(
    F.array(F.col("numerator"), F.col("denominator")).alias("fraction"),
)
frac_df.show(5, False)

# Implement
Frac = Tuple[int, int]

def py_sum_fraction(frac_1: Frac, frac_2: Frac) -> Optional[Frac]:
    """Sum two fractions"""
    num1, denom1 = frac_1
    num2, denom2 = frac_2
    if denom1 and denom2:
        answer = Fraction(num1, denom1) + Fraction(num2, denom2)
        return answer.numerator, answer.denominator
    return None

assert py_sum_fraction((1, 4), (1, 4)) == (1, 2)
assert py_sum_fraction((5, 8), (1, 4)) == (7, 8)

# Assign to udf
sum_fraction = F.udf(py_sum_fraction, T.ArrayType(T.LongType()))

# Apply sum_fraction on fraction column to itself
frac_df = frac_df.withColumn(
    "fraction_sum", sum_fraction(F.col("fraction"), F.col("fraction"))
)
frac_df.distinct().show(5, False)
