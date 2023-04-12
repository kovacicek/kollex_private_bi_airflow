from airflow.models import Variable
from sqlalchemy import create_engine


def prepare_pg_connection():
    pg_host = Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password = Variable.get("PG_PASSWORD_WRITE")
    pg_database = Variable.get("PG_DATABASE")
    pg_connect_string = (
        f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    )
    pg_engine = create_engine(f"{pg_connect_string}", echo=False)
    return pg_engine


def prepare_mysql_akeneo_connection():
    mysql_host = Variable.get("MYSQL_HOST")
    mysql_port = Variable.get("MYSQL_PORT")
    mysql_schema = Variable.get("MYSQL_DATABASE_akeneo")
    mysql_user = Variable.get("MYSQL_USERNAME")
    mysql_password = Variable.get("MYSQL_PASSWORD")
    mysql_connect_string = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_schema}"
    mysql_engine = create_engine(f"{mysql_connect_string}", echo=False)
    return mysql_engine
