import pandas as pd

from airflow import DAG
from airflow.models import Variable

# from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from datetime import datetime, timedelta
from sqlalchemy import create_engine

from include.delta_load_all_skus import run_delta_load
from include.full_load_all_skus import run_full_load
from include.dbt_run_raw_layer import dbt_run_raw_layers
from include.dbt_run_all_layers import dbt_run_all_layers
from include.my_sql_to_postgres import My_SQL_to_Postgres


def branch_on():
    # create engine
    pg_host = Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password = Variable.get("PG_PASSWORD_WRITE")
    pg_database = Variable.get("PG_DATABASE")
    pg_schema = Variable.get("PG_RAW_SCHEMA")
    pg_connect_string = (
        f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    )
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    # get merchants active
    merchants_active = pd.read_sql(
        """
            SELECT
                merchant_key
            FROM
                prod_raw_layer.merchants_new
            UNION
            SELECT
                merchant_key
            FROM
                prod_raw_layer.merchants_active
        """,
        con=pg_engine,
    )
    merchants_active = merchants_active[
        merchants_active["merchant_key"] != "trinkkontor"
    ]
    merchants_active = merchants_active[
        merchants_active["merchant_key"] != "trinkkontor_trr"
    ]
    merchants_active_count = pd.read_sql_table(
        "current_merchant_active_count", con=pg_engine, schema=pg_schema
    )
    print(merchants_active_count)
    print(merchants_active["merchant_key"].size)

    dt_now = datetime.now()
    dt_trigger = datetime.now().replace(
        hour=17, minute=30, second=0, microsecond=0
    )
    if dt_now > dt_trigger:
        return ["run_full_load"]
    else:
        if (
            merchants_active["merchant_key"].size
            != merchants_active_count.loc[0, "merchant_count"]
        ):
            merchants_active_count.loc[0, "merchant_count"] = merchants_active[
                "merchant_key"
            ].size
            pg_tables_to_use = "current_merchant_active_count"
            merchants_active_count.to_sql(
                pg_tables_to_use,
                pg_engine,
                schema=pg_schema,
                if_exists="replace",
                index=False,
            )
            print("changed the count")
            return ["run_full_load"]
        else:
            return ["run_delta_load"]


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
    dag_id="PIM_Pipeline",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 04-18/2 * * *",
    concurrency=100,
    catchup=False,
    max_active_runs=1,
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    data_dog_log = BashOperator(
        task_id="Started_All_SKUs_DAG",
        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
        dag=dag,
    )
    full_load = PythonOperator(
        task_id="run_full_load",
        python_callable=run_full_load,
        dag=dag,
        trigger_rule="none_failed",
        retries=5,
    )
    delta_load = PythonOperator(
        task_id="run_delta_load",
        python_callable=run_delta_load,
        dag=dag,
        trigger_rule="none_failed",
        retries=5,
    )
    branch_operator = BranchPythonOperator(
        task_id="choose_delta_or_full_load", python_callable=branch_on
    )

    copy_PIM_CATALOUG_PRODUCT_from_mySQL = PythonOperator(
        task_id="copy_PIM_CATALOUG_PRODUCT_from_mySQL",
        python_callable=My_SQL_to_Postgres,
        op_kwargs={
            "pg_schema": "from_pim",
            "pg_tables_to_use": "cp_pim_catalog_product",
            "mysql_tables_to_copy": "pim_catalog_product",
            "mysql_schema": "akeneo",
            "unique_column": "id",
            "delta_load": "UPSERT",
            "timestamp_column": " updated",
            "look_back_period": 0,
            "chunksize_to_use": 2000,
        },
        retries=5,
    )
    copy_PIM_CATALOUG_PRODUCT_model_from_mySQL = PythonOperator(
        task_id="copy_PIM_CATALOUG_PRODUCT_model_from_mySQL",
        python_callable=My_SQL_to_Postgres,
        op_kwargs={
            "pg_schema": "from_pim",
            "pg_tables_to_use": "cp_pim_catalog_product_model",
            "mysql_tables_to_copy": "pim_catalog_product_model",
            "mysql_schema": "akeneo",
            "timestamp_column": "updated",
            "unique_column": "id",
            "delta_load": "UPSERT",
            "look_back_period": 0,
            "chunksize_to_use": 10000,
        },
        retries=5,
    )
    copy_GFGH_DATA_from_mySQL = PythonOperator(
        task_id="copy_GFGH_DATA_from_mySQL",
        python_callable=My_SQL_to_Postgres,
        op_kwargs={
            "pg_schema": "from_pim",
            "pg_tables_to_use": "cp_gfgh_product",
            "mysql_tables_to_copy": "product",
            "timestamp_column": "updated_at",
            "mysql_schema": "gfghdata",
            "unique_column": "id",
            "delta_load": "UPSERT",
            "look_back_period": 0,
            "chunksize_to_use": 10000,
        },
        retries=5,
    )
    dbt_job_raw_layers = PythonOperator(
        task_id="dbt_job_raw_layers",
        python_callable=dbt_run_raw_layers,
        trigger_rule="all_success",
    )
    dbt_job_all_layers = PythonOperator(
        task_id="dbt_run_all_layers",
        python_callable=dbt_run_all_layers,
        trigger_rule="all_success",
    )
    data_dog_log_final = BashOperator(
        task_id="Finished_All_SKUs_fully",
        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
        dag=dag,
        trigger_rule="none_failed",
    )
    data_dog_log_middle = BashOperator(
        task_id="Finished_delta_or_full_load",
        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
        dag=dag,
        trigger_rule="none_failed",
    )
    data_dog_log_middle_2 = BashOperator(
        task_id="Finished_Copying_tables_from_MySQL",
        bash_command='echo "{{ task_instance_key_str }} {{ ts }}"',
        dag=dag,
        trigger_rule="none_failed",
    )


(
    data_dog_log
    >> branch_operator
    >> [full_load, delta_load]
    >> data_dog_log_middle
)
(
    data_dog_log_middle
    >> [
        copy_PIM_CATALOUG_PRODUCT_model_from_mySQL,
        copy_GFGH_DATA_from_mySQL,
        copy_PIM_CATALOUG_PRODUCT_from_mySQL,
    ]
    >> data_dog_log_middle_2
)
(
    data_dog_log_middle_2
    >> [dbt_job_raw_layers, dbt_job_all_layers]
    >> data_dog_log_final
)
