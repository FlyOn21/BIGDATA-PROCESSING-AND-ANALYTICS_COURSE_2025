from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from HW_20.dags.dag_utils.check_connection import check_connect_data
from HW_20.dags.dag_utils.spark_postgres_validate_result import validate_results

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        dag_id='hw_20_nyc_taxi_processing',
        default_args=default_args,
        description='Simple NYC Taxi data processing pipeline',
        schedule=None,
        catchup=False,
        tags=['hw_20', 'spark', 'nyc_taxi'],
) as dag:
    check_connect_data_task = PythonOperator(
        task_id='check_connect_data',
        python_callable=check_connect_data
    )

    spark_processing = BashOperator(
        task_id="spark_processing",
        bash_command=r"""
          set -e
          cd /opt/airflow/dags/spark_job
          python3 nyt_processing_job.py
        """,
        env={
            "POSTGRES_URL": "jdbc:postgresql://dbt_hw_20_host:5432/dbt_hw_20",
            "POSTGRES_USER": "dbt_user",
            "POSTGRES_PASSWORD": "12345678",
            "PYTHONPATH": "/opt/airflow/dags:$PYTHONPATH",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk",
        },
    )

    validate_results_task = PythonOperator(
        task_id='validate_results',
        python_callable=validate_results
    )

    check_connect_data_task >> spark_processing >> validate_results_task
