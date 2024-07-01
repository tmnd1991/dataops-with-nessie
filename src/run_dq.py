from great_expectations.dataset import SparkDFDataset
from pyspark.sql import SparkSession
from sys import argv
import sys

ref = argv[1]
spark = SparkSession.builder.getOrCreate()

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