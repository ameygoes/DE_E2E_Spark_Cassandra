from datetime import datetime
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'Amey Bhilegaonkar',
    'start_date': datetime(2024, 2, 6, 10, 00),
}


def get_data():
    import json
    import requests

    response = requests.get('https://randomuser.me/api')
    response = response.json()['results'][0]
    return response

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    res = get_data()
    res = format_data(res)

# with DAG('user_automation',
#          dafault_args=default_args,
#          catchup=False
#          ) as dag:
#     straming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )
stream_data()