from pyspark import SparkContext
from pyspark.sql import SparkSession
from time import time

APP_NAME = "RDD_Query_2"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"

sc = SparkContext(appName=APP_NAME)
spark = SparkSession(sc)

csv_file_path = f"{HDFS_DATA_DIR}/Full_Data"
init_rdd = spark.read.csv(csv_file_path, header=True).rdd


def map_time_to_day_split(row):
    time_occ = row[3]
    if "0500" <= time_occ < "1200":
        return "morning", 1
    elif "1200" <= time_occ < "1700":
        return "afternoon", 1
    elif "1700" <= time_occ < "2100":
        return "evening", 1
    else:
        return "night", 1


start_time = time()

res_rdd = (
    init_rdd.filter(lambda row: row[15] == "STREET")
    .map(map_time_to_day_split)
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

exec_time = time() - start_time
print(f"\n\nExec time: {exec_time} sec\n\n")

results = res_rdd.collect()
for row in results:
    print(row[0], row[1])

sc.stop()
