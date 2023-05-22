from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from dags.include.full_load_all_skus import run_full_load
from include.my_sql_to_postgres import my_sql_to_postgres


with DAG(
    dag_id="full_reload_pipeline",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 2 * * *",
    concurrency=100,
    catchup=False,
    max_active_runs=1,
) as dag:
    full_load = PythonOperator(
        task_id="run_full_load",
        python_callable=run_full_load,
        dag=dag,
        trigger_rule="none_failed",
        retries=5,
    )
    copy_PIM_CATALOG_PRODUCT_from_my_sql = PythonOperator(
        task_id="copy_PIM_CATALOG_PRODUCT_from_my_sql",
        python_callable=my_sql_to_postgres,
        op_kwargs={
            "pg_schema": "from_pim",
            "pg_tables_to_use": "cp_pim_catalog_product",
            "mysql_tables_to_copy": "pim_catalog_product",
            "mysql_schema": "akeneo",
            "unique_column": "id",
            "delta_load": "FULL_RELOAD",
            "timestamp_column": " updated",
            "look_back_period": 0,
            "chunksize_to_use": 2000,
        },
        retries=5,
    )
    copy_PIM_CATALOG_PRODUCT_model_from_my_sql = PythonOperator(
        task_id="copy_PIM_CATALOG_PRODUCT_model_from_my_sql",
        python_callable=my_sql_to_postgres,
        op_kwargs={
            "pg_schema": "from_pim",
            "pg_tables_to_use": "cp_pim_catalog_product_model",
            "mysql_tables_to_copy": "pim_catalog_product_model",
            "mysql_schema": "akeneo",
            "timestamp_column": "updated",
            "unique_column": "id",
            "delta_load": "FULL_RELOAD",
            "look_back_period": 0,
            "chunksize_to_use": 10000,
        },
        retries=5,
    )
    copy_GFGH_DATA_from_my_sql = PythonOperator(
        task_id="copy_GFGH_DATA_from_my_sql",
        python_callable=my_sql_to_postgres,
        op_kwargs={
            "pg_schema": "from_pim",
            "pg_tables_to_use": "cp_gfgh_product",
            "mysql_tables_to_copy": "product",
            "timestamp_column": "updated_at",
            "mysql_schema": "gfghdata",
            "unique_column": "id",
            "delta_load": "FULL_RELOAD",
            "look_back_period": 0,
            "chunksize_to_use": 10000,
        },
        retries=5,
    )

    data_dog_log = DummyOperator(
        task_id="data_dog_log", retries=3, trigger_rule="none_failed"
    )

    (
        data_dog_log
        >> [
            run_full_load,
            copy_PIM_CATALOG_PRODUCT_from_my_sql,
            copy_PIM_CATALOG_PRODUCT_model_from_my_sql,
            copy_GFGH_DATA_from_my_sql,
        ]
    )
