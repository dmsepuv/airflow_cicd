import urllib.request
import os

from airflow.models import Variable

def download_dataset():
    settings = Variable.get('avocado_dag_dataset_settings', deserialize_json=True)
    output = settings['filepath'] + settings['filename']
    urllib.request.urlretrieve(settings['url'], output)
    return os.path.getsize(output)

def read_rmse():
    accuracy = 0
    with open('/usr/local/airflow/include/tmp/out-model-avocado-prediction-rmse.txt') as f:
        accuracy = float(f.readline())
    return 'accurate' if accuracy < 0.15 else 'inaccurate'

def check_dataset(**context):
    filesize = int(context['ti'].xcom_pull(key=None, task_ids=['downloading_data'])[0])
    if filesize <= 0:
        raise ValueError('Dataset is empty')
