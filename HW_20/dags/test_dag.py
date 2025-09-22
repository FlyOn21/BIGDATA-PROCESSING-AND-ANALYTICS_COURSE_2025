from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_function():
    print("Test function executed successfully!")
    return "success"

def debug_function():
    import os
    import sys
    import logging

    logger = logging.getLogger(__name__)

    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Python path: {sys.path}")
    logger.info(f"Environment variables: {dict(os.environ)}")

    try:
        import airflow
        logger.info(f"Airflow version: {airflow.__version__}")
    except Exception as e:
        logger.error(f"Airflow import error: {e}")

    # Тест записи файла
    try:
        with open('/tmp/test_write.txt', 'w') as f:
            f.write('Test write successful')
        logger.info("File write test successful")
    except Exception as e:
        logger.error(f"File write error: {e}")

    return "Debug complete"

with DAG(
    'test_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    debug_task = PythonOperator(
        task_id='debug_task',
        python_callable=debug_function,
        dag=dag
    )

    bash_python_task = BashOperator(
        task_id='bash_python_test',
        bash_command='python3 -c "print(\'Python via bash works\')"',
        dag=dag
    )

    bash_python_task >> debug_task