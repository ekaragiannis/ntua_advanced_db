from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, DoubleType, DateType
from datetime import date

APP_NAME = "Generate_DataFrame"
HDFS_DATA_DIR = "hdfs://okeanos-master:54310/data"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()


def convert_date_format(input_date: str):
    date_part, _, _ = input_date.split(" ")
    month, day, year = date_part.split("/")
    return date(int(year), int(month), int(day))


convert_date_udf = udf(convert_date_format, DateType())

csv_file_path1 = f"{HDFS_DATA_DIR}/Crime_Data_from_2010_to_2019.csv"
csv_file_path2 = f"{HDFS_DATA_DIR}/Crime_Data_from_2020_to_Present.csv"

df1 = spark.read.csv(csv_file_path1, header=True)
df2 = spark.read.csv(csv_file_path2, header=True)

df = df1.union(df2)


df = (
    df.withColumn("Date Rptd", convert_date_udf(col("Date Rptd")))
    .withColumn("DATE OCC", convert_date_udf(col("DATE OCC")))
    .withColumn("Vict Age", df["Vict Age"].cast(IntegerType()))
    .withColumn("LAT", df["LAT"].cast(DoubleType()))
    .withColumn("LON", df["LON"].cast(DoubleType()))
)

output_csv_path = f"{HDFS_DATA_DIR}/Full_Data"
df.write.option("header", True).mode("overwrite").csv(output_csv_path)

