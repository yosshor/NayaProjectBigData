from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import datetime


start_Date = datetime.datetime(2023, 3, 23, 17)

default_args = {
    'owner': 'inbalg',
    'depends_on_past': False,
    'start_date': start_Date,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
  }

dag = DAG('Inbal_API2',
          default_args=default_args,
          schedule_interval='0 * * * *')

t1 = BashOperator(
    task_id='read_api',
    bash_command='python "/home/naya/airflow/dags/scripts/MapAPI_ImportReviews_Extended.py"',
    dag=dag)