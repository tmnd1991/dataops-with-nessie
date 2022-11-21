from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.dataset import SparkDFDataset
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

# Set up a basic spark session
import sys
from pyspark.sql import SparkSession
url = "http://localhost:19120/api/v1"
full_path_to_warehouse = './data'
ref = sys.argv[1]
auth_type = "NONE"
# here we are assuming NONE authorisation
spark = SparkSession.builder \
        .config("spark.jars.packages","org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:0.14.1,org.projectnessie:nessie-spark-extensions-3.3_2.12:0.44.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie.uri", url) \
        .config("spark.sql.catalog.nessie.ref", ref) \
        .config("spark.sql.catalog.nessie.authentication.type", auth_type) \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", full_path_to_warehouse) \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .getOrCreate()

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