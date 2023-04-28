import requests
from airflow.models import Variable


def dbt_run_raw_layers():
    my_token = Variable.get("dbt_token")
    my_url = Variable.get("PIM_url")

    head = {'Authorization': 'token {}'.format(my_token)}
    body = {'cause': 'Kicked Off From Airflow PIM Pipeline'}
    r = requests.post(my_url,
                      headers=head,
                      data=body
                      )
    r_dictionary = r.json()
    print(r_dictionary)
    print(r.status_code)
    print(r.text)
