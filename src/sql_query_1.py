from pyspark.sql import SparkSession
from time import time

APP_NAME = "SQL_Query_1"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

csv_file_path = f"{HDFS_DATA_DIR}/Full_Data"
df = spark.read.csv(csv_file_path, header=True)

start_time = time()

df.createOrReplaceTempView("tmp_view")
sql_query = """
    SELECT year, month, crime_total, rank
    FROM (
        SELECT
            year(`Date Rptd`) AS year,
            month(`Date Rptd`) AS month,
            COUNT(*) AS crime_total,
            ROW_NUMBER() OVER (PARTITION BY year(`Date Rptd`) ORDER BY COUNT(*) DESC) AS rank
        FROM
            tmp_view
        GROUP BY
            year(`Date Rptd`), month(`Date Rptd`)
    ) tmp
    WHERE rank <= 3
"""
results_df = spark.sql(sql_query)

results_df.show(results_df.count(), truncate=False)
exec_time = time() - start_time
print(f"\n\nExec time: {exec_time} sec\n\n")

spark.stop()
