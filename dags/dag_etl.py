# NOTE: Importing from aws_utils - custom code folder inside plugins
from datetime import datetime
from airflow import DAG

from ..spark_jobs.etl import process_category_product

# Creates the songs_etl dag, to be executed daily.
dag = DAG(dag_id='brazilian_deputies_dag', description='Simple tutorial DAG',
          schedule_interval='@daily',
          start_date=datetime(2017, 3, 20), catchup=False)

# Usage of PythonOperators with the imported custom code from plugins
