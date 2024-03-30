from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from crypto_etl import extract_data, transform_data, create_table_if_not_exists, load_data, query_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_data_ingestion',
    default_args=default_args,
    description='ETL process for ingesting cryptocurrency data into Redshift',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'data': '{{ ti.xcom_pull(task_ids="extract_data") }}'},
    dag=dag,
)

task_create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)

task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'df': '{{ ti.xcom_pull(task_ids="transform_data") }}'},
    dag=dag,
)

task_query_data = PythonOperator(
    task_id='query_data',
    python_callable=query_data,
    dag=dag,
)

task_extract_data >> task_transform_data >> task_create_table >> task_load_data >> task_query_data
