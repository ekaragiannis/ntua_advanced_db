from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, desc
from time import time

APP_NAME = "DF_Query_2"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

csv_file_path = f"{HDFS_DATA_DIR}/Full_Data"

df = spark.read.csv(csv_file_path, header=True)

start_time = time()

results_df = (
    df.filter(col("Premis Desc") == "STREET")
    .withColumn(
        "day_split",
        when((col("TIME OCC") >= "0500") & (col("TIME OCC") < "1200"), "morning")
        .when((col("TIME OCC") >= "1200") & (col("TIME OCC") < "1700"), "afternoon")
        .when((col("TIME OCC") >= "1700") & (col("TIME OCC") < "2100"), "evening")
        .otherwise("night"),
    )
    .groupBy("day_split")
    .count()
    .withColumnRenamed("count", "crime_total")
    .orderBy(desc("crime_total"))
)

exec_time = time() - start_time

print(f'\n\nExec time: {exec_time} sec\n\n')
print(results_df.show(results_df.count(), truncate=False))

spark.stop()
