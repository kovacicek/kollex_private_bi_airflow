import pandas as pd
import requests
import json


def create_or_update_contact(data):
    api_key = ""
    url = f'https://api.hubapi.com/contacts/v1/contact/createOrUpdate/email/{data.email}?hapikey={api_key}'
    headers = {'Content-Type': 'application/json'}

    data = json.dumps({
        "properties": [
            {
                "property": "email",
                "value": "tester123@hubspot.com"
            },
            {
                "property": "firstname",
                "value": "test"
            },
            {
                "property": "lastname",
                "value": "testerson"
            },
            {
                "property": "website",
                "value": "http://updated.example.com"
            },
            {
                "property": "lifecyclestage",
                "value": "customer"
            }
        ]
    })

    r = requests.post(data=data, url=url, headers=headers)
    print(r.status_code)
    print(r.data)


def upsert_hubspot_contacts():
    df = pd.read_sql("""
        SELECT * FROM prod_info_layer.customer_hubspot_upload 
            WHERE updated_at 
                BETWEEN current_date - interval '2 days' AND current_date;
    """)

    df.apply(lambda x: create_or_update_contact(x))


