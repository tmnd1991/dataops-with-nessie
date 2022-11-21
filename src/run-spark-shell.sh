spark-shell --conf "spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:0.14.1,org.projectnessie:nessie-spark-extensions-3.3_2.12:0.44.0" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
--conf "spark.sql.catalog.nessie.uri=http://localhost:19120/api/v1" \
--conf "spark.sql.catalog.nessie.ref=main" \
--conf "spark.sql.catalog.nessie.authentication.type=none" \
--conf "spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog" \
--conf "spark.sql.catalog.nessie.warehouse=./data" \
--conf "spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog"