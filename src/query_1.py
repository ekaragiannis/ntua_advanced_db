from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, row_number, desc
from pyspark.sql.window import Window
import time

APP_NAME = "DF_Query_1"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"
HDFS_OUTPUT_DIR = "hdfs://okeanos-master:54310/results"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

csv_file_path = f"{HDFS_DATA_DIR}/Full_Data"

df = spark.read.csv(csv_file_path, header=True)

start_time = time.time()

window = Window.partitionBy("year").orderBy(desc("crime_total"))
results_df = (
    df.withColumn("year", year("Date Rptd"))
    .withColumn("month", month("Date Rptd"))
    .groupBy("year", "month")
    .count()
    .withColumnRenamed("count", "crime_total")
    .withColumn(
        "rank",
        row_number().over(window),
    )
    .filter("rank <= 3")
    .select("year", "month", "crime_total", "rank")
)

results_df.show(results_df.count(), truncate=False)
exec_time = time.time() - start_time
print(f"\n\nExec time: {exec_time} sec")

spark.stop()

