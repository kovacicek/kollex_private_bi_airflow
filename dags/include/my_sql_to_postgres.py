from datetime import datetime

import pandas as pd
from pandas import read_sql_table
from sqlalchemy import create_engine, types
from psycopg2 import connect
from pandas import read_sql
from airflow.models import Variable


def my_sql_to_postgres(**kwargs):
    # Postgres Credentials
    pg_host = Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password = Variable.get("PG_PASSWORD_WRITE")
    pg_database = Variable.get("PG_DATABASE")
    pg_connect_string = (
        f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    )
    pg_engine = create_engine(
        f"{pg_connect_string}",
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    # Params
    pg_schema = kwargs["pg_schema"]
    pg_tables_to_use = kwargs["pg_tables_to_use"]
    delta_load = kwargs["delta_load"]
    unique_column = kwargs["unique_column"]
    timestamp_column = kwargs["timestamp_column"]

    # My SQL Credentials
    mysql_host = Variable.get("MYSQL_HOST")
    mysql_port = Variable.get("MYSQL_PORT")
    mysql_user = Variable.get("MYSQL_USERNAME")
    mysql_password = Variable.get("MYSQL_PASSWORD")
    mysql_schema = kwargs["mysql_schema"]
    mysql_tables_to_copy = kwargs["mysql_tables_to_copy"]
    chunksize_to_use = kwargs["chunksize_to_use"]
    look_back_period = kwargs["look_back_period"]
    mysql_connect_string = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_schema}"
    mysql_engine = create_engine(
        f"{mysql_connect_string}",
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
    )

    if delta_load == "FULL_RELOAD":
        df = read_sql_table(
            mysql_tables_to_copy, con=mysql_engine, chunksize=chunksize_to_use
        )
        print("Finished Reading the table")
        pg_conn_args = dict(
            host=pg_host,
            user=pg_user,
            password=pg_password,
            database=pg_database,
        )
        connection = connect(**pg_conn_args)
        cur = connection.cursor()

        cur.execute(f"DROP TABLE if exists {pg_schema}.{pg_tables_to_use} ;")
        connection.commit()
        print(
            "Table {}.{}, emptied before adding updated data.".format(
                pg_schema, pg_tables_to_use
            )
        )

    elif delta_load == "UPSERT":
        df = read_sql(
            f"""
            select {unique_column} 
            from {mysql_schema}.{mysql_tables_to_copy} 
            where 
                {timestamp_column} >= CURDATE() - INTERVAL 2 DAY
                AND    {timestamp_column} < CURDATE() + INTERVAL 1 DAY
            """,
            con=mysql_engine,
        )

        df_identifiers_to_delete = df[unique_column].to_list()
        if len(df_identifiers_to_delete) == 0:
            return "no updates"

        df_identifiers_to_delete = ",".join(
            [f"'{str(_id)}'" for _id in df_identifiers_to_delete]
        )
        connection = pg_engine.connect()

        connection.execute(
            f"delete from {pg_schema}.{pg_tables_to_use} where id in ({df_identifiers_to_delete})"
        )
        df = read_sql(
            f"""
            select * 
            from  {mysql_schema}.{mysql_tables_to_copy} 
            where 
                {timestamp_column} >= CURDATE() - INTERVAL 2 DAY
                AND {timestamp_column} < CURDATE() + INTERVAL 1 DAY
            """,
            con=mysql_engine,
            chunksize=chunksize_to_use,
        )
        print("Finished Reading the table")
    elif delta_load == "INSERT_NEW_ROWS":
        df = read_sql(
            f"""
            select * 
            from {mysql_schema}.{mysql_tables_to_copy} 
            where 
                {timestamp_column} >= CURDATE() - INTERVAL 2 DAY
                AND {timestamp_column} < CURDATE() + INTERVAL 1 DAY
            """,
            con=mysql_engine,
            chunksize=chunksize_to_use,
        )
        print("Finished Reading the table")
    elif delta_load == "INSERT_NEW_ROWS_DROP_OLD_TABLE":
        pg_conn_args = dict(
            host=pg_host,
            user=pg_user,
            password=pg_password,
            database=pg_database,
        )
        connection = connect(**pg_conn_args)
        cur = connection.cursor()

        cur.execute(f"DROP TABLE if exists {pg_schema}.{pg_tables_to_use};")
        connection.commit()
        print(
            "Table {}.{}, emptied before adding updated data.".format(
                pg_schema, pg_tables_to_use
            )
        )

        df = read_sql(
            f"""
            select * 
            from {mysql_schema}.{mysql_tables_to_copy} 
            where {timestamp_column} >= curdate() - INTERVAL DAYOFWEEK(curdate()) + {look_back_period} DAY
            """,
            con=mysql_engine,
            chunksize=chunksize_to_use,
        )
        print("Finished Reading the table")
    else:
        df = pd.DataFrame()
    for i, df_chunk in enumerate(df):
        print(i, df_chunk.shape)
        if not df_chunk.empty:
            # TODO fix this and make dtype flexible (dict(column,table))
            # col_dtype = {dtype_column: types.JSON} if pg_table == dtype_table else None
            df_chunk["_updated_at"] = datetime.now()
            df_chunk.to_sql(
                pg_tables_to_use,
                dtype={"raw_values": types.JSON, "data": types.JSON},
                con=pg_engine,
                chunksize=chunksize_to_use,
                if_exists="append",
                method="multi",
                schema=pg_schema,
                index=False,
            )
