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
    min as spark_min,
)
from geopy.distance import geodesic

APP_NAME = "DF_Query_4"
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

filtered_df = basic_df.filter(
    col("Weapon Used Cd").startswith("1")
    & (length(col("Weapon Used Cd")) == 3)
    & ((col("LAT") > 0) & (col("LON") < 0))
)

filtered_df = filtered_df.withColumn("AREA", filtered_df["AREA"].cast("integer"))
departments_df = departments_df.withColumn(
    "PREC", departments_df["PREC"].cast("integer")
)

inner_join_df = filtered_df.join(departments_df, col("AREA") == col("PREC")).withColumn(
    "distance",
    compute_distance_udf(
        filtered_df["LAT"],
        filtered_df["LON"],
        departments_df["Y"],
        departments_df["X"],
    ),
)

window_spec = Window.partitionBy("DR_NO").orderBy("distance")

cross_join_df = (
    filtered_df.crossJoin(
        departments_df.withColumnRenamed("LOCATION", "department location")
    )
    .withColumn(
        "distance",
        compute_distance_udf(
            filtered_df["LAT"],
            filtered_df["LON"],
            departments_df["Y"],
            departments_df["X"],
        ),
    )
    .withColumn("min_distance", spark_min("distance").over(window_spec))
    .filter(col("distance") == col("min_distance"))
    .drop("min_distance")
)

results_df_list = []

for join_df in [inner_join_df, cross_join_df]:
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
    print(df.show(df.count(), truncate=False))
    print()

spark.stop()
