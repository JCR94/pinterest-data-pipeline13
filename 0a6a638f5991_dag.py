from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/joelcosta94i@gmail.com/data_cleaning_and_queries',
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'Joel Costa',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG('0a6a638f5991_dag',
    start_date=datetime(2023,12,18),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='run_notebook',
        databricks_conn_id='databricks_default',
        existing_cluster_id= '1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run