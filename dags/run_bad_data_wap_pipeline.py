from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
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

# Setup parameters for spark session
url = "http://nessie:19120/api/v1"
full_path_to_warehouse = '/data'
date = datetime.now()
ref = "customer_update_" + str(date.year) + str(date.month) + str(date.day)
auth_type = "NONE"

conf = {
        'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.80.0',
        'spark.driver.extraJavaOptions': '-Divy.cache.dir=/tmp -Divy.home=/tmp',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions',
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.catalog.nessie.uri': url,
        'spark.sql.catalog.nessie.ref': ref,
        'spark.sql.catalog.nessie.authentication.type': auth_type,
        'spark.sql.catalog.nessie.catalog-impl': 'org.apache.iceberg.nessie.NessieCatalog',
        'spark.sql.catalog.nessie.warehouse': full_path_to_warehouse,
        'spark.sql.catalog.nessie': 'org.apache.iceberg.spark.SparkCatalog'
    }

with DAG(
    dag_id="WAP_Demo_Fail",
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

    update_wrong_data = SparkSubmitOperator(
        name="update_wrong_data",
        task_id='update_wrong_data',
        application='/usr/local/spark/app/update_wrong_data.py',
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

    # delete_branch = SparkSqlOperator(
    #     name="delete_branch",
    #     task_id="delete_branch",
    #     conf=conf,
    #     conn_id='spark',
    #     dag=dag,
    #     sql=f"DROP BRANCH {ref} IN nessie",
    #     retries=0
    # )

    create_table >> update_wrong_data >> run_dq # >> delete_branch
