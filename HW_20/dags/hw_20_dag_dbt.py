from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'nyt_taxi_dbt_pipeline',
    default_args=default_args,
    description='NYC Taxi data transformation with dbt',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['dbt', 'nyt_taxi'],
) as dag:
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=r"""
        set -e
        cd /dbt_nyt/nyt_dbt_airflow
        echo "Installing dbt dependencies..."
        dbt deps --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_nyt/nyt_dbt_airflow
        """
    )
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=r"""
        set -e
        cd /dbt_nyt/nyt_dbt_airflow
        echo "Running dbt models..."
        dbt run --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_nyt/nyt_dbt_airflow
        """
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=r"""
        set -e
        cd /dbt_nyt/nyt_dbt_airflow
        echo "Running dbt tests..."
        dbt test --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_nyt/nyt_dbt_airflow
        """
    )


    dbt_deps >> dbt_run >> dbt_test
