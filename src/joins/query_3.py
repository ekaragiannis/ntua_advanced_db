from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, year, desc, udf, split, regexp_replace
from time import time
import sys

APP_NAME = "DF_Query_3"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

basic_csv_path = f"{HDFS_DATA_DIR}/Full_Data"
revgecoding_csv_path = f"{HDFS_DATA_DIR}/revgecoding.csv"

basic_df = spark.read.csv(basic_csv_path, header=True)
revgecoding_df = spark.read.csv(revgecoding_csv_path, header=True)

revgecoding_df = revgecoding_df.hint(sys.argv[1])

start_time = time()

df = (
    basic_df.filter((year(col("Date Rptd")) == 2015) & (col("Vict Descent") != ""))
    .join(
        revgecoding_df,
        (basic_df["LAT"] == revgecoding_df["LAT"])
        & (basic_df["LON"] == revgecoding_df["LON"]),
    )
    .select("Vict Descent", "ZIPcode")
)

df.explain()

df.show()
exec_time = time() - start_time
print(f"\n\nExec time: {exec_time} sec\n\n")

spark.stop()
