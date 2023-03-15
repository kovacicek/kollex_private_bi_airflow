import gspread as gs
import pandas as pd

import gspread_dataframe as gd
from sqlalchemy import create_engine
from airflow.models import Variable


def hubspot_sync():
    pg_host = Variable.get("PG_HOST")
    pg_user = Variable.get("PG_USERNAME_WRITE")
    pg_password = Variable.get("PG_PASSWORD_WRITE")
    pg_database = Variable.get("PG_DATABASE")
    pg_connect_string = (
        f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_database}"
    )
    pg_engine = create_engine(
        f"{pg_connect_string}", echo=False, pool_pre_ping=True, pool_recycle=800
    )

    sheet_name = Variable.get("HUBSPOT_SHEET_NAME")
    sql = """
       WITH hubspot AS (
	SELECT 
		*, 
		CASE 
			WHEN parent_customer_owner_lead_source LIKE '%Krombacher Brauerei%' THEN REPLACE(parent_customer_owner_lead_source, 'Krombacher Brauerei', 'Krombacher')
			WHEN parent_customer_owner_lead_source LIKE '%krombacher%' THEN REPLACE(parent_customer_owner_lead_source, 'krombacher', 'Krombacher')
			ELSE parent_customer_owner_lead_source
		END AS parent_customer_owner_lead_source_updated
	FROM prod_info_layer.customer_hubspot_upload
)

SELECT 
	hub."customer_uuid",
	hub."parent_customer_name",
	hub."parent_customer_creation_date",
	hub."parent_customer_owner_first_name",
	hub."parent_customer_owner_last_name",
	hub."parent_customer_owner_email",
	hub."parent_customer_owner_mobile_phone",
	hub."parent_customer_owner_lead_source_updated" AS "parent_customer_owner_lead_source",
	hub."Wann JV-Typeform ausgefüllt",
	hub."type",
	hub."First Order Date",
	hub."Child Customer Name",
	hub."Customer Status",
	hub."Number Of Orders",
	hub."Status Last Order",
	hub."Status Last Supplier Request",
	hub."street",
	hub."PLZ",
	hub."Ort",
	hub."Wann zuletzt bestellt",
	hub."Gilt als Aktiv",
	hub."Gilt als inaktiv (hat bereits bestellt, ist seit 28 inaktiv)",
	hub."Registrierter Kunde ohne Bestellung",
	hub."Wie oft durchschnittlichen in den letzten 3 Monaten bestellt",
	hub."Durchschnittlicher Bestellrythmus (allgemein)",
	hub."Wann eingeladen",
	hub.merchant_who_invited_customer
FROM hubspot as hub
    """
    df = pd.read_sql(
        sql,
        con=pg_engine,
    )
    df["Number Of Orders"] = df["Number Of Orders"].astype(str)
    df["Number Of Orders"] = df["Number Of Orders"].replace("nan", "0")
    gsheet_credentials = {
        "type": Variable.get("gsheet_creds_type"),
        "project_id": Variable.get("gsheet_creds_project_id"),
        "private_key_id": Variable.get("hubspot_private_key_id"),
        "private_key": Variable.get("hubspot_private_key"),
        "client_email": Variable.get("hubspot_client_email"),
        "client_id": Variable.get("hubspot_client_id"),
        "auth_uri": Variable.get("gsheet_creds_auth_uri"),
        "token_uri": Variable.get("hubspot_token_uri"),
        "auth_provider_x509_cert_url": Variable.get(
            "gsheet_creds_auth_provider_x509_cert_url"
        ),
        "client_x509_cert_url": Variable.get("hubspot_x509_cert_url"),
    }
    gc = gs.service_account_from_dict(gsheet_credentials)
    print("loaded credentials")

    sh = gc.open_by_url(Variable.get("HUBSPOT_SPREADSHEET"))

    ws = sh.worksheet(sheet_name)
    # Get the existing data as a dataframe
    # sheet_as_df = gd.get_as_dataframe(ws)
    # # Append the new dataframe to the existing dataframe
    # df.columns = [c.replace(" ", "_") for c in df.columns]
    # sheet_as_df.columns = [c.replace(" ", "_") for c in sheet_as_df.columns]
    #
    # df["Customer_UUID"] = df["Customer_UUID"].astype(str)
    # sheet_as_df["Customer_UUID"] = sheet_as_df["Customer_UUID"].astype(str)
    #
    # current_ids = sheet_as_df["Customer_UUID"].tolist()
    # new_ids = df["Customer_UUID"].tolist()
    # missing_ids = list(set(new_ids) - set(current_ids))
    # filter_df = df[df["Customer_UUID"].isin(missing_ids)]
    #
    # df.columns = [c.replace("_", " ") for c in df.columns]
    # sheet_as_df.columns = [c.replace("_", " ") for c in sheet_as_df.columns]
    # filter_df.columns = [c.replace("_", " ") for c in filter_df.columns]
    #
    # appended_df = sheet_as_df.append(filter_df)
    # Clear the existing sheet
    ws.clear()
    gd.set_with_dataframe(ws, df)
    print("Dataframe has been appended to the sheet")
