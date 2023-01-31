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
    logging.info('Connecting with AWS S3')
    skus = []
    sku_to_last_modified = {}
    s3 = boto3.client(
        's3',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': bucket_object}
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for obj in page['Contents']:
            filename = obj['Key'].split('/')[-1]
            if filename.endswith('.png') and filename.split('.png')[
                0].isdigit():
                sku = filename.split('.png')[0]
                sku_to_last_modified[sku] = obj['LastModified']
                skus.append(sku)
                logging.info(f"Added file: {obj['Key']}")

    data = {"sku": skus}
    df = pd.DataFrame(data)
    df['uploaded_at'] = df['sku'].map(sku_to_last_modified)
    df.drop_duplicates(subset='sku', inplace=True)
    df.to_sql(
        'pictures_with_sku',
        con=pg_engine,
        schema='sheet_loader',
        if_exists='replace',
        index=False,
    )
