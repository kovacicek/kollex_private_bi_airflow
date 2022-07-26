from datetime import datetime, timedelta
import airflow
from airflow import DAG


# import psycopg2
# import csv
import io
#from tkinter.messagebox import QUESTION
# import mysql.connector
import pandas as pd
import os
# import numpy as np
import time
import io
import csv
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import requests
# from dotenv import load_dotenv
from include.gsheet_to_postgres import run_gsheet_load
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


with DAG(
    dag_id="Name Matching",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 */1 * * *",
    catchup=False,

    concurrency=100
) as dag:


    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log = DummyOperator(task_id='data_dog_log', retries=3)

    
    COPY_MERCHANT_CS = PythonOperator(
                                                                task_id='Reading Input Data into DB'
                                                                , python_callable=run_gsheet_load,
                                                           op_kwargs={'pg_schema': 'sheet_loader'
                                                                    , 'pg_tables_to_use': 'Input_data_for_name_matching'
                                                                    ,'url' :'https://docs.google.com/spreadsheets/d/1QhsizWoJ-7wyUuO8kwVWfbxmu3h__tDgOTNgLG9mYFg/edit#gid=1580347407'
                                                                    , 'sheet_name':'Input_data'
                                                                   }, retries=5
                                        )
    data_dog_log_final = DummyOperator(task_id='data_dog_log_final', retries=3,trigger_rule='none_failed')
data_dog_log >>data_dog_log_final
    