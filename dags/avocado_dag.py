from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator #To run Python functions
from airflow.contrib.sensors.file_sensor import FileSensor #To check files arrives in the right folder
from airflow.operators.papermill_operator import PapermillOperator #Machine Learning operator
from airflow.operators.postgres_operator import PostgresOperator #To store tasks data in tables
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.notebook_plugin import NotebookToKeepOperator
from notebook_plugin.operators.notebook_to_git_operator import NotebookToGitOperator

from include.helpers.astro import download_dataset, read_rmse, check_dataset
from include.subdags.training_subdag import subdag_factory

from datetime import datetime, timedelta

default_args = {
    'retries': 3,   #Tries in case the DAG retry
    'email_on_retry': False,   #Get an email in case the DAG retries   
    'email_on_failure': False,   #Get an email in case the DAG fails
    'retry_delay': timedelta(minutes=5),   #Waiting time to run the retry for the DAG
    'depends_on_past': False,   #Dependencies between DAGs
    'start_date': datetime(2020, 1, 1) #Start time of execution
}

with DAG('avocado_dag', default_args=default_args, description='Forecasting avocado prices', schedule_interval='*/10 * * * *', catchup=False) as dag:

    creating_accuracy_table = PostgresOperator(
        task_id = 'creating_accuracy_table',
        sql = 'sql/CREATE_TABLE_ACCURACIES.sql',
        postgres_conn_id = 'postgres'
    )
    
    downloading_data = PythonOperator(
        task_id = 'downloading_data',
        python_callable = download_dataset
    )

    sanity_check = PythonOperator(
        task_id = 'sanity_check',
        python_callable = check_dataset,
        provide_context = True
    )

    waiting_for_data = FileSensor(
        task_id = 'waiting_for_data',
        fs_conn_id = 'fs_default',
        filepath = '/usr/local/airflow/include/tmp/avocado.csv',
        poke_interval = 15 #Seconds in which the sensor check if the files arrived
    )

    training_model_tasks = SubDagOperator(
        task_id = 'training_model_tasks',
        subdag = subdag_factory('avocado_dag', 'training_model_tasks', default_args)
    )

    evaluating_rmse = BranchSQLOperator(
        task_id = 'evaluating_rmse',
        sql = 'sql/FETCH_MIN_RMSE.sql',
        conn_id = 'postgres',
        follow_task_ids_if_true = 'accurate',
        follow_task_ids_if_false = 'inaccurate'
    )

    accurate = DummyOperator(
        task_id = 'accurate'
    )

    inaccurate = DummyOperator(
        task_id = 'inaccurate'
    )

    fetching_notebook = NotebookToKeepOperator(
        task_id = 'fetching_notebook',
        sql = 'sql/FETCH_BEST_MODEL.sql',
        postgres_conn_id = 'postgres'
    )

    publish_notebook = NotebookToGitOperator(
        task_id = 'publish_notebook',
        conn_id = 'git',
        nb_path = '/usr/local/airflow/include/tmp',
        nb_name = 'out-model-avocado-prediction-{{ task_instance.xcom_pull(key=None, task_ids="fetching_notebook") }}.ipynb'

    )

#Assigning order to the tasks
    creating_accuracy_table >> downloading_data >> sanity_check >> waiting_for_data 
    waiting_for_data >> training_model_tasks >> evaluating_rmse
    evaluating_rmse >> [accurate, inaccurate]
    accurate >> fetching_notebook >> publish_notebook
	