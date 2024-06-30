import sys
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import input_file_name
import os
from sys import argv

ref = argv[1]
spark = SparkSession.builder.getOrCreate()

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info(f"current directory: {os.getcwd()}")

spark.sql(f"CREATE BRANCH IF NOT EXISTS {ref} IN nessie FROM main")
data = [
    (1, "Antonio", "Murgia", "abcd"),
    (2, "Thomas", "Edison", "abcda"),
    (3, "Albert", "Einsten", "albi"),
]
# spark.read.table("nessie.customer").show()
columns = ["id", "name", "surname", "tax_id"]
df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("toBeInserted")

spark.sql(
    f"""MERGE INTO nessie.`customer@{ref}` t
       USING toBeInserted s
       ON t.id = s.id
       WHEN MATCHED THEN UPDATE SET t.id = s.id, t.name = s.name, t.surname = s.surname, t.tax_id = s.tax_id
       WHEN NOT MATCHED THEN INSERT *"""
)

spark.read.table(f"nessie.`customer@{ref}`").show()


spark.read.table(f"nessie.`customer@{ref}`").withColumn("filename", input_file_name()).show(truncate=False)
