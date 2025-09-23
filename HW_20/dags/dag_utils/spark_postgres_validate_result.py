import logging
import psycopg2


def validate_results():
    """Validate processed data in PostgreSQL."""

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

        table_queries = {
            'yellow_taxi_stage': 'SELECT COUNT(*) FROM yellow_taxi_stage',
            'green_taxi_stage': 'SELECT COUNT(*) FROM green_taxi_stage',
            'taxi_lookup_zone_stage': 'SELECT COUNT(*) FROM taxi_lookup_zone_stage'
        }

        for table in tables:
            try:
                if table not in table_queries:
                    raise ValueError(f"Unknown table: {table}")
                cursor.execute(table_queries[table])
                count = cursor.fetchone()[0]
                logger.info(f"{table}: {count:,} records")

                if count == 0:
                    raise ValueError(f"No data in {table}")
            except psycopg2.ProgrammingError as e:
                if "does not exist" not in str(e):
                    raise e
                logger.error(f"Table {table} does not exist")
                raise ValueError(f"Table {table} does not exist") from e

        logger.info("Data validation successful!")

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise
    finally:
        if conn:
            conn.close()

    return True
