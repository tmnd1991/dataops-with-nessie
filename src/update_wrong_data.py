from datetime import datetime
from pyspark.sql import SparkSession

url = "http://nessie:19120/api/v1"
full_path_to_warehouse = '/data'
date = datetime.now()
ref = "customer_update_" + str(date.year) + str(date.month) + str(date.day)
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

data = [
    (1,"Antonio","Murgia", "abcd"),
    (2,"Thomas","Edison", "abcda"),
    (3,"Albert","Einsten", "albi"),
    (4,"Albertino","Einstenino", "albi"),
  ]
spark.read.table("nessie.customer").show()
columns = ["id", "name", "surname", "tax_id"]
df = spark.createDataFrame(data,  columns)
df.createOrReplaceTempView("toBeInserted")
spark.sql(
    f"""MERGE INTO nessie.`customer@{ref}` t
       USING toBeInserted s
       ON t.id = s.id
       WHEN MATCHED THEN UPDATE SET t.id = s.id, t.name = s.name, t.surname = s.surname, t.tax_id = s.tax_id
       WHEN NOT MATCHED THEN INSERT *"""
)

spark.read.table(f"nessie.`customer@{ref}`").show()