from datetime import datetime

from airflow.models import Variable
from include.db import prepare_pg_connection
import pandas as pd


def prepare_data_for_hubspot():
    pg_engine = prepare_pg_connection()
    connection = pg_engine.connect()
    pg_info_schema = Variable.get("PG_INFO_SCHEMA")

    connection.execute(
        f"drop table if exists prod_info_layer.customer_hubspot_upload;"
    )

    df = pd.read_sql("""
        SELECT * FROM 
            prod_info_layer.customer_information 
                WHERE customer_state = 'Registered' 
                    OR  customer_state = 'Connected';
    """)

    df['updated_at'] = datetime.now()
    df['last_updated_sync'] = datetime.now()
    df.to_sql(
        "customer_hubspot_upload",
        con=pg_engine,
        if_exists="replace",
        method="multi",
        schema=pg_info_schema,
        index=False,
    )