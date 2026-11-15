# Recovered from 2026 Sheridan DBAS31064 team project documentation.
# Academic prototype; requires configured Hadoop/Spark environment.

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    max as spark_max,
    concat_ws,
    lag,
    round,
    split,
    sum as spark_sum,
    to_timestamp,
    trim,
)
spark = SparkSession.builder.appName("Phase4MaximumConsumption").getOrCreate()
input_path = "hdfs:///energydata/*/*/*.txt"
raw_df = spark.read.text(input_path)
parts = split(trim(col("value")), r"\s+")
df = raw_df.select(
    parts.getItem(1).alias("HOUSE_ID"),
    parts.getItem(2).alias("CONDATE"),
    parts.getItem(3).alias("CONTIME"),
    parts.getItem(4).cast("double").alias("ENERGY_READING"),
).withColumn(
    "DATETIME",
    to_timestamp(concat_ws(" ", col("CONDATE"), col("CONTIME"))),
)
window_spec = Window.partitionBy("HOUSE_ID", "CONDATE").orderBy("DATETIME")
consumption_df = df.withColumn(
    "PREVIOUS_READING",
    lag("ENERGY_READING").over(window_spec),
).withColumn(
    "ACTUAL_CONSUMPTION",
    col("ENERGY_READING") - col("PREVIOUS_READING"),
).filter(
    col("ACTUAL_CONSUMPTION").isNotNull()
).filter(
    col("ACTUAL_CONSUMPTION") >= 0
)
hourly_df = consumption_df.withColumn(
    "HOUR",
    split(col("CONTIME"), ":").getItem(0),
)
hourly_consumption_df = hourly_df.groupBy(
    "HOUSE_ID",
    "CONDATE",
    "HOUR",
).agg(
    spark_sum("ACTUAL_CONSUMPTION").alias("HOURLY_CONSUMPTION")
)
maximum_hourly_per_house_df = hourly_consumption_df.groupBy(
    "HOUSE_ID",
).agg(
    round(spark_max("HOURLY_CONSUMPTION"), 6).alias("MAX_HOURLY_CONSUMPTION")
).orderBy(
    col("HOUSE_ID").cast("int")
)
print("Maximum hourly electricity consumption for each house across all available dates:")
maximum_hourly_per_house_df.show(100,truncate=False)
daily_consumption_df = consumption_df.groupBy(
    "HOUSE_ID",
    "CONDATE",
).agg(
    spark_sum("ACTUAL_CONSUMPTION").alias("DAILY_CONSUMPTION")
)
max_daily_per_house_df = daily_consumption_df.groupBy(
    "HOUSE_ID",
).agg(
    round(spark_max("DAILY_CONSUMPTION"), 6).alias("MAX_DAILY_CONSUMPTION")
).orderBy(
    col("HOUSE_ID").cast("int")
)
hourly_consumption_display_df = hourly_consumption_df.select(
    "HOUSE_ID",
    "CONDATE",
    "HOUR",
    round(col("HOURLY_CONSUMPTION"), 6).alias("HOURLY_CONSUMPTION")
).orderBy(
    col("HOUSE_ID").cast("int"),
    "CONDATE",
    "HOUR"
)
print("Hourly electricity consumption organized by house, date, and hour: :")
hourly_consumption_display_df.show(100,truncate=False)
print("Maximum daily electricity consumption for each house across all available dates:")
max_daily_per_house_df.show(truncate=False)
total_consumption_per_house_df = consumption_df.groupBy(
    "HOUSE_ID",
).agg(
    spark_sum("ACTUAL_CONSUMPTION").alias("TOTAL_CONSUMPTION")
)
house_with_max_total_df = total_consumption_per_house_df.orderBy(
    col("TOTAL_CONSUMPTION").desc()
).limit(1).select(
    "HOUSE_ID",
    round(col("TOTAL_CONSUMPTION"), 6).alias("TOTAL_CONSUMPTION")
)
overall_max_hourly_df = hourly_consumption_df.orderBy(
    col("HOURLY_CONSUMPTION").desc()
).limit(1).select(
    "HOUSE_ID",
    "CONDATE",
    "HOUR",
    round(col("HOURLY_CONSUMPTION"), 6).alias("HOURLY_CONSUMPTION")
)
print("Highest hourly electricity consumption across all houses and dates:")
overall_max_hourly_df.show(truncate=False)
print("House with highest total electricity consumption across all available dates:")
house_with_max_total_df.show(truncate=False)
spark.stop()
