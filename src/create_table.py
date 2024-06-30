import sys
from pyspark.sql import SparkSession
url = "http://nessie:19120/api/v1"
full_path_to_warehouse = '/data'
ref = "main"
auth_type = "NONE"
# here we are assuming NONE authorisation
spark = SparkSession.builder \
        .config("spark.sql.catalog.nessie.uri", url) \
        .config("spark.sql.catalog.nessie.ref", ref) \
        .config("spark.sql.catalog.nessie.authentication.type", auth_type) \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", full_path_to_warehouse) \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .getOrCreate()

spark.sql("""CREATE TABLE IF NOT EXISTS nessie.customer (
    id bigint,
    name string,
    surname string,
    tax_id string) USING ICEBERG""")
