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
    df = pd.read_sql(
        """
        with main as (
        select    cth."id_customer",
            cth."UUID",
            cth1."First Order Date"
        from 
            prod_info_layer.customer_table_horeca_children_customers_classified cth 
        join 
            prod_info_layer.customer_table_horeca_children_customers cth1
        using(id_customer)
        group by cth."id_customer", cth."UUID",  cth1."First Order Date"
        )
        , merchants as (
                select "fk_customer", "name" as "merchant_name" from fdw_customer_service.customer_has_merchant join fdw_customer_service.merchant on "fk_merchant" = "id_merchant"
        )

        SELECT 
        main."UUID" as "Customer UUID",
        "typ",
        main."First Order Date",
        "Unternehmensname",
        "E-Mail Adresse Owner",
        "Customer Status",
        "Number Of Orders",
        "Wann registriert",
        "Wann zuletzt bestellt",
        "Status Last Order",
        "Wann letzter Upload"
        "Status Last Supplier Request",
        "Straße",
        "PLZ",
        "Ort",
        "Gilt als aktiv",
        "Registrierter Kunde ohne Bestellung",
        "Gilt als inaktiv (hat bereits bestellt, ist seit 28 inaktiv)",
        "Durchschnittlicher Bestellrythmus (allgemein)",
        "Wie oft durchschnittlichen in den letzten 3 Monaten bestellt",
        "email",
        "Name Owner",
        "Vorname Owner",
        "Telefonnummer Owner",
        "Coke Kunde",
        "Bitburger Kunde",
        "Krombacher Kunde",
        "Rottkapechen Kunde",
        "Wann JV-Typeform ausgefüllt",
        "Wann eingeladen",
        merchants."merchant_name" as "merchant who invited customer",
        count(*) over (partition by "typ") as num_of_mails
        FROM
        prod_info_layer.customer_hubspot_upload chu
        left join
        main 
        on chu."Parent ID" = main."id_customer"
        left join merchants
        on chu."Parent ID" = merchants."fk_customer"
        where  "email" is null or ("email" not like '%%kollex.io%%' and "email" not like '%%kollex.de%%')
        """,
        con=pg_engine,
    )

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
    sheet_as_df = gd.get_as_dataframe(ws)
    # Append the new dataframe to the existing dataframe
    df.columns = [c.replace(" ", "_") for c in df.columns]
    sheet_as_df.columns = [c.replace(" ", "_") for c in sheet_as_df.columns]

    df["Customer_UUID"] = df["Customer_UUID"].astype(str)
    sheet_as_df["Customer_UUID"] = sheet_as_df["Customer_UUID"].astype(str)

    current_ids = sheet_as_df["Customer_UUID"].tolist()
    new_ids = df["Customer_UUID"].tolist()
    missing_ids = list(set(new_ids) - set(current_ids))
    filter_df = df[df["Customer_UUID"].isin(missing_ids)]

    df.columns = [c.replace("_", " ") for c in df.columns]
    sheet_as_df.columns = [c.replace("_", " ") for c in sheet_as_df.columns]
    filter_df.columns = [c.replace("_", " ") for c in filter_df.columns]

    appended_df = sheet_as_df.append(filter_df)
    # Clear the existing sheet
    ws.clear()
    # Write the appended dataframe to the sheet
    gd.set_with_dataframe(ws, appended_df)
    print("Dataframe has been appended to the sheet")
