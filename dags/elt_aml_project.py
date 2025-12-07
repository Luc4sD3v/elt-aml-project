import sys
sys.path.append("/opt/airflow")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import run_extract
from src.bronze_to_silver import run_bronze_to_silver
from src.silver_to_gold import run_silver_to_gold

default_args = {
    "owner": "lucasrafael",
    "start_date": datetime(2022, 1, 1),
}

with DAG(
    dag_id="elt_aml_project",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=run_extract,
    )

    task_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    task_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
    )

    task_extract >> task_silver >> task_gold