from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import clickhouse_connect

def insert_data_to_clickhouse():
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8123, 
        username='admin', 
        password='admin'
    )

    # Вставка данных в существующие таблицы
    # Пример обновления активности пользователей (добавьте реальный код вставки новых данных)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clickhouse_data_loader', 
    default_args=default_args, 
    schedule_interval='@daily',
    catchup=False
)

load_data_task = PythonOperator(
    task_id='load_data_to_clickhouse',
    python_callable=insert_data_to_clickhouse,
    dag=dag,
)

load_data_task
