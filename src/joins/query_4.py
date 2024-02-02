from pyspark.sql import SparkSession, Window
from pyspark.sql.types import FloatType
from pyspark.sql.functions import (
    col,
    length,
    udf,
    year,
    avg,
    count,
    desc,
)
from geopy.distance import geodesic
import sys, time

APP_NAME = "Inner_Join_Query_4"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

basic_csv_path = f"{HDFS_DATA_DIR}/Full_Data"
departmens_csv_path = f"{HDFS_DATA_DIR}/LAPD_Police_Stations.csv"

basic_df = spark.read.csv(basic_csv_path, header=True)
departments_df = spark.read.csv(departmens_csv_path, header=True)


def compute_distance(lat1, lon1, lat2, lon2):
    crime_location = (lat1, lon1)
    department_location = (lat2, lon2)
    return geodesic(crime_location, department_location).kilometers


compute_distance_udf = udf(compute_distance, FloatType())

results_df_list = []

start_time = time.time()

filtered_df = basic_df.filter(
    col("Weapon Used Cd").startswith("1")
    & (length(col("Weapon Used Cd")) == 3)
    & ((col("LAT") > 0) & (col("LON") < 0))
)

filtered_df = filtered_df.withColumn("AREA", filtered_df["AREA"].cast("integer"))
departments_df = departments_df.withColumn(
    "PREC", departments_df["PREC"].cast("integer")
)

departments_df = departments_df.hint(sys.argv[1])

join_df = filtered_df.join(departments_df, col("AREA") == col("PREC")).withColumn(
    "distance",
    compute_distance_udf(
        filtered_df["LAT"],
        filtered_df["LON"],
        departments_df["Y"],
        departments_df["X"],
    ),
)

results_df_list = []

results_df = (
    join_df.withColumn("year", year("Date Rptd"))
    .groupBy("year")
    .agg(
        avg("distance").alias("average distance"), count("*").alias("total crimes")
    )
    .select("year", "average distance", "total crimes")
    .orderBy("year")
)
results_df_list.append(results_df)

results_df = (
    join_df.withColumnRenamed("DIVISION", "division")
    .groupBy("division")
    .agg(
        avg("distance").alias("average distance"), count("*").alias("total crimes")
    )
    .select("division", "average distance", "total crimes")
    .orderBy(desc("total crimes"))
)
results_df_list.append(results_df)

for df in results_df_list:
    df.explain()
    df.show(df.count(), truncate=False)

exec_time = time.time() - start_time
print(f'\n\nExec time: {exec_time} sec\n\n')

spark.stop()
