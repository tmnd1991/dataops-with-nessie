from datetime import datetime
from pyspark.sql import SparkSession
from sys import argv

ref = argv[1]
# here we are assuming NONE authorisation
spark = SparkSession.builder.getOrCreate()

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