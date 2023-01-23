import boto3
import logging
import os
import pandas as pd
from airflow.models import Variable
from sqlalchemy import create_engine


def pictures_sku_load():
    pg_host = Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password = Variable.get("PG_PASSWORD_WRITE")
    pg_database = Variable.get("PG_DATABASE")
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False,
                              pool_pre_ping=True, pool_recycle=800)

    bucket_name = Variable.get("BUCKET_NAME")
    bucket_object = Variable.get("BUCKET_OBJECT")
    pictures = []
    logging.info('Connecting with AWS S3')
    s3 = boto3.resource(
        's3',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=bucket_object):
        logging.info(f"Added file: {obj.key}")
        pictures.append(obj.key)

    picture_names = [os.path.basename(name) for name in pictures if
                     name.endswith('.png')]
    skus_prepared = [x.split('.png')[0] for x in picture_names]
    final_skus = [x for x in skus_prepared if x.isdigit()]
    data = {"sku": final_skus}
    df = pd.DataFrame(data)
    df.drop_duplicates(subset='sku', inplace=True)
    df.to_sql(
        'pictures_with_sku',
        con=pg_engine,
        schema='sheet_loader',
        if_exists='replace',
        index=False,
    )
