FROM python:3.9-buster

RUN apt update && apt install default-jdk -y
ADD requirements.txt /
RUN pip install -r /requirements.txt
RUN nessie config --add endpoint http://nessie:19120/api/v1
RUN ["python", "-c", "from pyspark.sql import SparkSession; SparkSession.builder.config('spark.jars.packages','org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0,org.projectnessie:nessie-spark-extensions-3.3_2.12:0.50.0').getOrCreate()"]
