import gspread
import gspread_pandas
import gspread_pandas.conf
import gspread_dataframe as gd
import logging
import os
import pandas as pd

from airflow.models import Variable
from gspread import SpreadsheetNotFound
from gspread_pandas import Spread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine

logging.getLogger().setLevel(logging.INFO)


def hubspot_sync():
    # database connection
    pg_host = Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password = Variable.get("PG_PASSWORD_WRITE")
    pg_database = Variable.get("PG_DATABASE")
    pg_connect_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    pg_engine = create_engine(f"{pg_connect_string}", echo=False,
                              pool_pre_ping=True, pool_recycle=800)

    spreadsheet_name = "Hubspot_contacts"
    sheet_name = "contacts"
    df = pd.read_sql(
        """SELECT * FROM prod_info_layer.customer_hubspot_upload""",
        con=pg_engine
    )

    ### already used secret credentials file
    gapi_keyfile = "gsheet-loading-into-dwh-d51d3a31aff8.json"
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    google_creds = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_name(gapi_keyfile, scope)
    )
    ### configuration for local execution???
    config = gspread_pandas.conf.get_config(
        conf_dir=os.path.abspath(os.getcwd())
    )
    try:
        # if exist spreadsheet only append
        sheet = google_creds.open(spreadsheet_name).worksheet(sheet_name)
        sheet_as_df = gd.get_as_dataframe(sheet)

        df.columns = [c.replace(' ', '_') for c in df.columns]
        sheet_as_df.columns = [c.replace(' ', '_') for c in sheet_as_df.columns]

        current_ids = sheet_as_df['Parent_ID'].tolist()
        new_ids = df['Parent_ID'].tolist()
        missing_ids = list(set(new_ids) - set(current_ids))
        filter_df = df[df['Parent_ID'].isin(missing_ids)]

        df.columns = [c.replace('_', ' ') for c in df.columns]
        sheet_as_df.columns = [c.replace('_', ' ') for c in sheet_as_df.columns]
        filter_df.columns = [c.replace('_', ' ') for c in filter_df.columns]

        sheet_as_df.append(filter_df)

        spreadsheet = google_creds.open(spreadsheet_name)
        spread = Spread(spreadsheet_name)
        spread.df_to_sheet(sheet_as_df,
                           index=False,
                           sheet=sheet_name,
                           start='A1',
                           replace=False)
        logging.info(f'Found Existing Spreadsheet {spreadsheet.url}')
    except SpreadsheetNotFound:
        # if not exist spreadsheet create new
        logging.info('Creating New Spreadsheet')
        spreadsheet = google_creds.create(spreadsheet_name)
        spread = Spread(spreadsheet_name)
        spread.df_to_sheet(df,
                           index=False,
                           sheet=sheet_name,
                           start='A1',
                           replace=True)
        # spreadsheet.share('kollex.de',
        #                   perm_type='domain',
        #                   role='writer')
        # spreadsheet.share('pim@kollex.de',
        #                   perm_type='user',
        #                   role='writer')
        spreadsheet.share('milan.kovacic.extern@kollex.de',
                          perm_type='user',
                          role='writer')
        logging.info(spreadsheet.url)
