from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

conf = {
        'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.80.0',
        'spark.driver.extraJavaOptions': '-Divy.cache.dir=/tmp -Divy.home=/tmp',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions',
        'spark.master': 'spark://spark-master:7077'
        }

with DAG(
    dag_id="WAP_Demo",
    default_args=default_args,
    description=f"A simple DAG for WAP demo",
    schedule_interval=timedelta(days=1),
) as dag:

    create_table = SparkSubmitOperator(
        name="create_table",
        task_id='create_table',
        application='/usr/local/spark/app/create_table.py',
        conf=conf,
        conn_id='spark',
        dag=dag,
        retries=0
    )

    update_data = SparkSubmitOperator(
        name="update_data",
        task_id='update_data',
        application='/usr/local/spark/app/update_data.py',
        conf=conf,
        conn_id='spark',
        dag=dag,
        retries=0
    )

    run_dq = SparkSubmitOperator(
        name="run_dq",
        task_id='run_dq',
        application='/usr/local/spark/app/run_dq.py',
        py_files='/opt/airflow/ge.zip',
        conf=conf,
        conn_id='spark',
        dag=dag,
        retries=0
    )

    create_table >> update_data >> run_dq
