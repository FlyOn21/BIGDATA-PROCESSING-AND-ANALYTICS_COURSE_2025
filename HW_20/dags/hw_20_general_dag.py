from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def check_connect_data():
    """Check connection to data sources and PostgreSQL."""
    import logging
    import os

    logger = logging.getLogger('hw_20_dag.check_connect_data')

    dags_path = "/opt/airflow/dags"
    logger.info(f"Contents of {dags_path}:")
    try:
        for item in os.listdir(dags_path):
            logger.info(f"  {item}")
    except Exception as e:
        logger.error(f"Cannot list directory {dags_path}: {e}")

    data_path = "/opt/airflow/dags/source_data/home_work_data"
    required_paths = [
        f"{data_path}/green_taxi",
        f"{data_path}/yellow_taxi",
        f"{data_path}/taxi_zone_lookup.csv"
    ]

    for path in required_paths:
        if not os.path.exists(path):
            parent_dir = os.path.dirname(path)
            if os.path.exists(parent_dir):
                logger.info(f"Contents of {parent_dir}:")
                try:
                    for item in os.listdir(parent_dir):
                        logger.info(f"  {item}")
                except Exception as e:
                    logger.error(f"Cannot list directory {parent_dir}: {e}")
            raise FileNotFoundError(f"Data not found: {path}")
        logger.info(f"Data found: {path}")

    try:
        import psycopg2
        conn = psycopg2.connect(
            host='postgres_db_dbt_hw_20',
            port=5432,
            database='dbt_hw_20',
            user='dbt_user',
            password='12345678'
        )
        conn.close()
        logger.info("PostgreSQL connection successful")
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        raise

    logger.info("All connections and data checks passed!")
    return True


def validate_results():
    """Validate processed data in PostgreSQL."""
    import logging

    import psycopg2

    logger = logging.getLogger('hw_20_dag.validate_results')

    conn = None  # Инициализируем переменную
    try:
        conn = psycopg2.connect(
            host='postgres_db_dbt_hw_20',
            port=5432,
            database='dbt_hw_20',
            user='dbt_user',
            password='12345678'
        )
        cursor = conn.cursor()

        # Check tables exist and have data
        tables = ['yellow_taxi_stage', 'green_taxi_stage', 'taxi_lookup_zone_stage']

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"{table}: {count:,} records")

                if count == 0:
                    raise ValueError(f"No data in {table}")
            except psycopg2.ProgrammingError as e:
                if "does not exist" not in str(e):
                    raise e
                logger.error(f"Table {table} does not exist")
                raise ValueError(f"Table {table} does not exist")

        logger.info("Data validation successful!")

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise
    finally:
        if conn:
            conn.close()

    return True


with DAG(
        dag_id='hw_20_nyc_taxi_general_processing',
        default_args=default_args,
        description='Simple NYC Taxi data processing pipeline',
        schedule=None,
        catchup=False,
        tags=['hw_20', 'spark', 'nyc_taxi', 'dbt'],
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


    check_connect_data_task >> spark_processing >> validate_results_task >> dbt_deps >> dbt_run >> dbt_test
