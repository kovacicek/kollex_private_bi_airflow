from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from include.hubspot_crm_api import upsert_hubspot_contacts
from include.load_customer_hubspot_upload import prepare_data_for_hubspot


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="hubspot",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 */4 * * *",
    catchup=False,
    concurrency=100,
) as dag:

    hubspot_loading = PythonOperator(
        task_id="load_hubspot_data",
        python_callable=prepare_data_for_hubspot,
        trigger_rule="all_success",
    )
    hubspot_upsert = PythonOperator(
        task_id="upsert_hubspot_contacts",
        python_callable=upsert_hubspot_contacts,
        trigger_rule="all_success",
    )

hubspot_loading >> hubspot_upsert
