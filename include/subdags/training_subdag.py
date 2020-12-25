from airflow import DAG
from airflow.operators.papermill_operator import PapermillOperator
from airflow.models import Variable

def subdag_factory(parent_dag_name, child_dag_name, default_args):
    with DAG(
        dag_id = '{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args = default_args
    ) as dag:
        
        model_settings = Variable.get('avocado_dag_model_settings', deserialize_json = True)
        training_model_tasks = []

        for feature in model_settings['max_features']:
            for estimator in model_settings['n_estimators']:
                ml_id = f'{feature}_{estimator}'
                training_model_tasks.append(PapermillOperator(
                    task_id = 'training_model_{0}'.format(ml_id),
                    input_nb = '/usr/local/airflow/include/notebooks/avocado_prediction.ipynb',
                    output_nb = '/usr/local/airflow/include/tmp/out-model-avocado-prediction-{0}.ipynb'.format(ml_id),
                    parameters = {
                        'filepath': '/usr/local/airflow/include/tmp/avocado.csv',
                        'n_estimators': estimator,
                        'max_features': feature,
                        'ml_id': ml_id
                    },
                    pool='training_pool'
                ))
    
        return dag