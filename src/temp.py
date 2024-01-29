from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, year, desc, udf, split, regexp_replace
from time import time

APP_NAME = "DF_Query_3"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

basic_csv_path = f"{HDFS_DATA_DIR}/Full_Data"
revgecoding_csv_path = f"{HDFS_DATA_DIR}/revgecoding.csv"
income_csv_path = f"{HDFS_DATA_DIR}/LA_income_2015.csv"

basic_df = spark.read.csv(basic_csv_path, header=True)
revgecoding_df = spark.read.csv(revgecoding_csv_path, header=True)
income_df = spark.read.csv(income_csv_path, header=True)

victim_descent_mapping = {
    "A": "Other Asian",
    "B": "Black",
    "C": "Chinese",
    "D": "Cambodian",
    "F": "Filipino",
    "G": "Guamanian",
    "H": "Hispanic/Latin/Mexican",
    "I": "American Indian/Alaskan Native",
    "J": "Japanese",
    "K": "Korean",
    "L": "Laotian",
    "O": "Other",
    "P": "Pacific Islander",
    "S": "Samoan",
    "U": "Hawaiian",
    "V": "Vietnamese",
    "W": "White",
    "X": "Unknown",
    "Z": "Asian Indian",
}

mapping_udf = udf(lambda x: victim_descent_mapping.get(x, x), StringType())

income_df = (
    income_df.withColumn(
        "Estimated Median Income",
        regexp_replace("Estimated Median Income", "\\$", ""),
    )
    .withColumn(
        "Estimated Median Income",
        regexp_replace("Estimated Median Income", ",", ""),
    )
    .withColumn(
        "Estimated Median Income", col("Estimated Median Income").cast("integer")
    )
)


def query_3(strategy: str):
    results = []
    start_time = time()
    inner_join_df = (
        basic_df.filter((year(col("Date Rptd")) == 2015) & (col("Vict Descent") != ""))
        .join(
            revgecoding_df.hint(strategy),
            (basic_df["LAT"] == revgecoding_df["LAT"])
            & (basic_df["LON"] == revgecoding_df["LON"]),
        )
        .select("Vict Descent", "ZIPcode")
    )

    print(inner_join_df.explain())
    for asc in [True, False]:
        sorted_income_ZIP_codes_df = (
            income_df.sort("Estimated Median Income", ascending=asc)
            .limit(3)
            .select("Zip Code")
            .collect()
        )

        data = [row["Zip Code"] for row in sorted_income_ZIP_codes_df]

        results_df = (
            inner_join_df.filter(split(col("ZIPcode"), "-").getItem(0).isin(data))
            .withColumnRenamed("Vict Descent", "Victim Descent")
            .groupBy("Victim Descent")
            .count()
            .withColumnRenamed("count", "crime_total")
            .orderBy(desc("crime_total"))
            .withColumn("Victim Descent", mapping_udf(col("Victim Descent")))
        )

        results.append(results_df)

    for df in results:
        df.show()


strategies = ["broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]
for strategy in strategies:
    start_time = time()
    query_3(strategy)
    exec_time = time() - start_time
    print(f"\n\n{strategy} exec time: {exec_time} sec \n\n")

spark.stop()
