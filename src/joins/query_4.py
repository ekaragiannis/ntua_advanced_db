from pyspark.sql import SparkSession, Window
from pyspark.sql.types import FloatType
from pyspark.sql.functions import (
    col,
    length,
    udf,
    min as spark_min,
)
from geopy.distance import geodesic
import sys, time

APP_NAME = "DF_Query_4"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"

basic_csv_path = f"{HDFS_DATA_DIR}/Full_Data"
departmens_csv_path = f"{HDFS_DATA_DIR}/LAPD_Police_Stations.csv"


def compute_distance(lat1, lon1, lat2, lon2):
    crime_location = (lat1, lon1)
    department_location = (lat2, lon2)
    return geodesic(crime_location, department_location).kilometers


compute_distance_udf = udf(compute_distance, FloatType())


def main():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    basic_df = spark.read.csv(basic_csv_path, header=True)
    departments_df = spark.read.csv(departmens_csv_path, header=True)

    filtered_df = basic_df.filter(
        col("Weapon Used Cd").startswith("1")
        & (length(col("Weapon Used Cd")) == 3)
        & ((col("LAT") > 0) & (col("LON") < 0))
    )

    filtered_df = filtered_df.withColumn("AREA", filtered_df["AREA"].cast("integer"))
    departments_df = departments_df.withColumn(
        "PREC", departments_df["PREC"].cast("integer")
    )

    select = sys.argv[1]
    departments_df = departments_df.hint(sys.argv[2])

    start_time = time.time()
    if select == "1":
        inner_join(filtered_df, departments_df)
    elif select == "2":
        cross_join(filtered_df, departments_df)
    else:
        print("Wrong input")
    exec_time = time.time() - start_time
    print(f"Exec time: {exec_time} seconds")


def inner_join(df1, df2):
    join_df = df1.join(df2, col("AREA") == col("PREC")).withColumn(
        "distance",
        compute_distance_udf(
            df1["LAT"],
            df1["LON"],
            df2["Y"],
            df2["X"],
        ),
    )
    join_df.explain()
    join_df.show()


def cross_join(df1, df2):
    window_spec = Window.partitionBy("DR_NO").orderBy("distance")
    join_df = (
        df1.crossJoin(df2.withColumnRenamed("LOCATION", "department location"))
        .withColumn(
            "distance",
            compute_distance_udf(
                df1["LAT"],
                df1["LON"],
                df2["Y"],
                df2["X"],
            ),
        )
        .withColumn("min_distance", spark_min("distance").over(window_spec))
        .filter(col("distance") == col("min_distance"))
        .drop("min_distance")
    )
    join_df.explain()
    join_df.show()


main()