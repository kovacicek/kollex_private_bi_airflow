 # pyright: reportMissingImports=false

from datetime import datetime, timedelta
import psycopg2
import csv
import io
import os
import numpy as np
import time
import io
import csv
import requests
from include.dbt_run_all_layers import dbt_run_all_layers

import airflow 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash import BashOperator 
from include.monday_api import run_monday_api

from include.dbt_run_all_layers import dbt_run_all_layers
from include.my_sql_to_postgres_prod import My_SQL_to_Postgres_prod
from include.dbt_run_diffs import dbt_run_diffs


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
    dag_id="MySQL_to_Postgres_copy",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="30 23 * * *",
    concurrency=100,
    catchup=False
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log = BashOperator(
        task_id='Started_MySQL_to_Postgres_copy_DAG',
        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
        dag=dag,

    )
    Copy_CSV_Merchants = PythonOperator(task_id='Copy_CSV_Merchants', python_callable=My_SQL_to_Postgres_prod,
                                              op_kwargs={  'pg_schema': 'csvexchange'
                                                         , 'pg_tables_to_use': 'merchants_csv'
                                                         , 'mysql_tables_to_copy': 'merchants'
                                                         , 'mysql_schema': 'csvexchange'
                                                         , 'delta_load': 'FULL_RELOAD'
                                                         , 'unique_column': 'id'
                                                         , 'timestamp_column': 'updated_at'
                                                         , 'look_back_period': 0
                                                         , 'chunksize_to_use': 10000}
                                                , retries=5
                                              )
   
    data_dog_log_final = BashOperator(

        task_id='Finished_MySQL_to_Postgres_copy_DAG',
        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
        dag=dag,

    )


data_dog_log >> Copy_CSV_Merchants >> data_dog_log_final
