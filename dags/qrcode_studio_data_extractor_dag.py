from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime

from include.qrcode_studio_data_extractor import download_qrcode_studio_data, insert_qr_code_studio_data

with DAG(
        "qrcode_studio_data_extractor_dag",
        start_date=datetime(2023, 4, 18),
        schedule_interval=None,
        catchup=False
) as dag:
    download_qrcode_studio_data = PythonOperator(
        task_id="download_qrcode_studio_data",
        python_callable=download_qrcode_studio_data
    )
    insert_qr_code_studio_data = PythonOperator(
        task_id="insert_qr_code_studio_data",
        python_callable=insert_qr_code_studio_data
    )
    remove_files = BashOperator(
        task_id='remove_files',
        bash_command='rm -rf /tmp/qr_code_studio/*'
    )

download_qrcode_studio_data >> insert_qr_code_studio_data >> remove_files
