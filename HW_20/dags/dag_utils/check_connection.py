import psycopg2
import logging
import os


def check_connect_data():
    """Check connection to data sources and PostgreSQL."""

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
