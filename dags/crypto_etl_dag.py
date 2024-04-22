from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from crypto_etl import extract_data, transform_data, create_table_if_not_exists, load_data, query_data, verify_thresholds_and_alert

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

# Tarea para extraer los datos de la api
task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Tarea para transformar los datos de la api a una estructura permitida en nuestra base de datos
task_transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'data': '{{ ti.xcom_pull(task_ids="extract_data") }}'},
    dag=dag,
)

# Tarea para crear la tabla en caso de que no exista
task_create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)

# Tarea para subir los datos a la base de datos
task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'df': '{{ ti.xcom_pull(task_ids="transform_data") }}'},
    dag=dag,
)

# Tarea para verificar los umbrales y enviar alertas de acuerdo a los datos insertados
task_verify_thresholds_and_alert = PythonOperator(
    task_id='verify_thresholds_and_alert',
    python_callable=verify_thresholds_and_alert,
    dag=dag,
)

task_extract_data >> task_transform_data >> task_create_table >> task_load_data >> task_verify_thresholds_and_alert
