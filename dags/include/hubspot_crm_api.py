import pandas as pd
import requests
import json

from include.db import prepare_pg_connection


def create_or_update_contact(data):
    api_key = "pat-eu1-152cd1dd-d744-4ff0-b343-ebff9bb5cf0c"
    url = f"https://api.hubapi.com/contacts/v1/contact/createOrUpdate/email/{data.parent_customer_owner_email}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

    data = json.dumps(
        {
            "properties": [
                {
                    "property": "Bestellaktivität",
                    "value": data["customer_order_state"],
                },
                {
                    "property": "Vorname Nutzer",
                    "value": data["parent_customer_owner_first_name"],
                },
                {
                    "property": "Nachname_Nutzer",
                    "value": data["parent_customer_owner_last_name"],
                },
                {
                    "property": "E-Mail",
                    "value": data["parent_customer_owner_email"],
                },
                {
                    "property": "Telefonnummer_Nutzer",
                    "value": data["parent_customer_owner_mobile_phone"],
                },
                {
                    "property": "Lead Source",
                    "value": data["parent_customer_owner_lead_source"],
                },
                {
                    "property": "Datum Registrierung (N)",
                    "value": str(data["parent_customer_owner_creation_date"]),
                },
                {
                    "property": "Customer_UUID",
                    "value": str(data["child_customer_uuid"]),
                },
                {
                    "property": "Betriebsname",
                    "value": data["child_customer_name"],
                },
                {
                    "property": "Status (N)",
                    "value": data["child_customer_status"],
                },
                {
                    "property": "Datum Einladung",
                    "value": str(data["child_customer_invitation_date"]),
                },
                {
                    "property": "Erste Bestellung",
                    "value": str(data["child_customer_first_order_date"]),
                },
                {
                    "property": "Letzte Bestellung",
                    "value": str(data["child_customer_last_order_date"]),
                },
                {
                    "property": "Number of orders",
                    "value": data["child_customer_number_of_orders"],
                },
                {
                    "property": "Wie oft durchschnittlichen in den letzten 3 Monaten bestellt (B)",
                    "value": data["child_customer_number_of_orders_last_3_months"],
                },
                {
                    "property": "Straße",
                    "value": data["child_customer_address_street"],
                },
                {
                    "property": "PLZ",
                    "value": data["child_customer_address_zip"],
                },
                {
                    "property": "Ort",
                    "value": data["child_customer_address_city"],
                },
                {
                    "property": "durchschnittlicher Bestellrythmus (B)",
                    "value": data["child_customer_average_days_between_orders"],
                },
                {
                    "property": "durchschnittlicher Bestellrythmus (B)",
                    "value": data["child_customer_average_days_between_orders"],
                },
            ]
        }
    )

    r = requests.post(data=data, url=url, headers=headers)
    print(r.status_code)
    print(r.json())


def upsert_hubspot_contacts():
    pg_engine = prepare_pg_connection()
    df = pd.read_sql(
        """
        SELECT * FROM prod_info_layer.customer_hubspot_upload 
            WHERE updated_at 
                BETWEEN current_date - interval '2 days' AND current_date;
    """,
        con=pg_engine,
    )

    df.apply(lambda x: create_or_update_contact(x), axis=1)
