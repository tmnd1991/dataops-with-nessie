import sys
from pyspark.sql import SparkSession
url = "http://nessie:19120/api/v1"
full_path_to_warehouse = './data'
ref = "main"
auth_type = "NONE"
# here we are assuming NONE authorisation
spark = SparkSession.builder \
        .config("spark.jars.packages","org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0,org.projectnessie:nessie-spark-extensions-3.3_2.12:0.50.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie.uri", url) \
        .config("spark.sql.catalog.nessie.ref", ref) \
        .config("spark.sql.catalog.nessie.authentication.type", auth_type) \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", full_path_to_warehouse) \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .getOrCreate()

spark.sql("""CREATE TABLE nessie.customer (
    id bigint,
    name string,
    surname string,
    tax_id string) USING ICEBERG""")
