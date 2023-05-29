import pandas as pd
import requests
import json

from datetime import datetime
from airflow.models import Variable
from include.db import prepare_pg_connection
from pytz import utc


def make_unix_timestamp(data):
    if data:
        try:
            utc_dt = utc.localize(
                datetime(
                    year=data.year,
                    month=data.month,
                    day=data.day,
                )
            )
            ts = int(utc_dt.timestamp() * 1000)
        except:
            ts = None
    else:
        ts = None
    return ts


def add_property(property_name, value, data_str):
    if value is not None:
        data_obj = json.loads(data_str)
        data_obj["properties"].append(
            {
                "property": property_name,
                "value": value,
            }
        )
        return json.dumps(data_obj)
    return data_str


def create_or_update_contact(data):
    api_key = Variable.get("hubspot_private_key")
    url = f"{Variable.get('hubspot_api')}/{data.parent_customer_owner_email}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

    ts1 = make_unix_timestamp(data["parent_customer_owner_creation_date"])
    data_str = json.dumps(
        {
            "properties": [
                {"property": "bestellaktivitat", "value": data["customer_order_state"]},
                {
                    "property": "vorname_nutzer",
                    "value": data["parent_customer_owner_first_name"],
                },
                {
                    "property": "nachname_nutzer",
                    "value": data["parent_customer_owner_last_name"],
                },
                {"property": "email", "value": data["parent_customer_owner_email"]},
                {
                    "property": "lead_source",
                    "value": data["parent_customer_owner_lead_source"]
                    if data["parent_customer_owner_lead_source"] != "NaN"
                    else "",
                },
                {"property": "wann_registriert_nutzer", "value": ts1},
                {
                    "property": "Customer_UUID",
                    "value": str(data["child_customer_uuid"]),
                },
                {
                    "property": "betriebsname",
                    "value": data["child_customer_name"]
                    if data["child_customer_name"] != "NaN"
                    else "",
                },
                {
                    "property": "customer_status",
                    "value": data["child_customer_status"]
                    if data["child_customer_status"] != "NaN"
                    else "",
                },
                {
                    "property": "stra_e",
                    "value": data["child_customer_address_street"]
                    if data["child_customer_address_street"] != "NaN"
                    else "",
                },
                {
                    "property": "ort",
                    "value": data["child_customer_address_city"]
                    if data["child_customer_address_city"] != "NaN"
                    else "",
                },
                {
                    "property": "durchschnittlicher_bestellrhythmus__b_",
                    "value": data["child_customer_average_days_between_orders"],
                },
                {
                    "property": "wie_oft_durchschnittlichen_in_den_letzten_3_monaten_bestellt_betrieb",
                    "value": data["child_customer_number_of_orders_last_3_months"],
                },
                {
                    "property": "PLZ",
                    "value": data["child_customer_address_zip"],
                },
                {
                    "property": "Telefonnummer",
                    "value": data["parent_customer_owner_mobile_phone"],
                },
            ]
        }
    )

    data_str = add_property(
        "letzte_bestellung",
        make_unix_timestamp(data["child_customer_last_order_date"]),
        data_str,
    )
    data_str = add_property(
        "datum_der_ersten_bestellung",
        make_unix_timestamp(data["child_customer_first_order_date"]),
        data_str,
    )
    data_str = add_property(
        "datum_einladung",
        make_unix_timestamp(data["child_customer_invitation_date"]),
        data_str,
    )

    r = requests.post(data=data_str, url=url, headers=headers)
    print(r.status_code)
    print(r.json())


def upsert_hubspot_contacts():
    pg_engine = prepare_pg_connection()
    df = pd.read_sql(
        """
        SELECT * FROM prod_info_layer.customer_hubspot_upload 
            WHERE updated_at 
                BETWEEN current_date - interval '4 days' AND current_date;
    """,
        con=pg_engine,
    )

    df["child_customer_average_days_between_orders"] = df[
        "child_customer_average_days_between_orders"
    ].fillna(0)
    df["child_customer_number_of_orders_last_3_months"] = df[
        "child_customer_number_of_orders_last_3_months"
    ].fillna(0)
    df.apply(lambda x: create_or_update_contact(x), axis=1)
