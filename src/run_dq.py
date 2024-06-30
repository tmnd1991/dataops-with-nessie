from great_expectations.dataset import SparkDFDataset
from datetime import datetime

# Set up a basic spark session
import sys
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

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")
LOGGER.info(f"Reading from customer@{ref}")
# basic dataframe
table = spark.read.table(f"nessie.`customer@{ref}`")
table.show()
df = SparkDFDataset(table)
res = df.validate(
{
  "expectation_suite_name": "suite_customer_v1",
  "expectations": [
    {
      "expectation_type": "expect_table_columns_to_match_ordered_list",
      "kwargs": {
        "column_list": [
          "id",
          "name",
          "surname",
          "tax_id"
        ]
      },
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": {
        "column": "tax_id"
      }
    }
  ]
}
)
if res['statistics']['unsuccessful_expectations'] > 0:
  print('failure')
  print(str(res))
  print('failure')
else:
  print('success')

sys.exit(res['statistics']['unsuccessful_expectations'])