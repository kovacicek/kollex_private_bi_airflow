from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from include.hubspot_crm_customer_sync import hubspot_sync

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
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
    dag_id="hubspot",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 7 * * *",
    catchup=False,

    concurrency=100
) as dag:

    Hubspot_task = PythonOperator(
        task_id='hubspot_task'
        , python_callable=hubspot_sync,
        trigger_rule='all_success'
    )
