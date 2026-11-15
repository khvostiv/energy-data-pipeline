# Recovered from 2026 Sheridan DBAS31064 team project documentation.
# Academic prototype; requires configured Hadoop/Spark environment.

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    lag,
    round,
    split,
    sum as spark_sum,
    to_timestamp,
    trim,
)
spark = SparkSession.builder.appName("Phase4AverageConsumption").getOrCreate()
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
daily_consumption_df = consumption_df.groupBy(
    "HOUSE_ID",
    "CONDATE",
).agg(
    spark_sum("ACTUAL_CONSUMPTION").alias("DAILY_CONSUMPTION")
)
daily_consumption_display_df = daily_consumption_df.select(
    "HOUSE_ID",
    "CONDATE",
    round(col("DAILY_CONSUMPTION"), 6).alias("DAILY_CONSUMPTION")
).orderBy(
    col("HOUSE_ID").cast("int"),
    "CONDATE",
)
average_daily_per_house_df = daily_consumption_df.groupBy(
    "HOUSE_ID",
).agg(
    round(avg("DAILY_CONSUMPTION"), 6).alias("AVERAGE_DAILY_CONSUMPTION")
).orderBy(
    col("HOUSE_ID").cast("int")
)
print("Daily electricity consumption for each house, organized by house and date:")
daily_consumption_display_df.show(100, truncate=False)
print("Average daily electricity consumption for each house across all available dates:")
average_daily_per_house_df.show(100, truncate=False)
spark.stop()
