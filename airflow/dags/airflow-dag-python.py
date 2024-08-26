from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import clickhouse_connect
import random

def update_db():
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='admin', password='admin')
    
    # Обновление данных в таблице user_activity
    user_activity = []
    for i in range(101, 201):
        user_activity.append((
            i,
            random.randint(1, 100),
            [datetime.now() - timedelta(days=random.randint(0, 30)) for _ in range(random.randint(1, 5))]
        ))
    client.insert('game_stats.user_activity', user_activity)

    # Обновление данных в таблице win_loss_stats
    win_loss_stats = []
    for i in range(101, 201):
        win_loss_stats.append((i, random.randint(0, 50), random.randint(0, 50)))
    client.insert('game_stats.win_loss_stats', win_loss_stats)

    # Обновление данных в таблице avg_play_time
    avg_play_time = []
    for i in range(101, 201):
        avg_play_time.append((i, random.uniform(1.0, 5.0)))
    client.insert('game_stats.avg_play_time', avg_play_time)

    # Обновление данных в таблице activity_heatmap
    activity_heatmap = []
    for i in range(101, 201):
        for hour in range(0, 24):
            activity_heatmap.append((i, hour, random.randint(0, 100)))
    client.insert('game_stats.activity_heatmap', activity_heatmap)

    # Обновление данных в таблице financial_transactions
    financial_transactions = []
    for i in range(501, 1001):
        financial_transactions.append((
            i,
            random.randint(101, 200),
            random.uniform(5.0, 500.0),
            random.choice(['purchase', 'sale']),
            datetime.now() - timedelta(days=random.randint(0, 30))
        ))
    client.insert('game_stats.financial_transactions', financial_transactions)

    # Обновление данных в таблице game_events
    game_events = []
    for i in range(501, 1001):
        game_events.append((
            i,
            random.randint(101, 200),
            random.choice(['kill', 'death', 'quest_completed']),
            datetime.now() - timedelta(days=random.randint(0, 90)),
            f'Event description {i}'
        ))
    client.insert('game_stats.game_events', game_events)

    # Обновление данных в таблице error_logs
    error_logs = []
    for i in range(201, 401):
        error_logs.append((
            i,
            random.randint(101, 200),
            random.choice(['network_error', 'server_error']),
            datetime.now() - timedelta(days=random.randint(0, 30)),
            f'Error message {i}'
        ))
    client.insert('game_stats.error_logs', error_logs)

    # Обновление данных в таблице user_growth
    user_growth = []
    start_date = datetime.now() - timedelta(days=365)
    for i in range(366, 731):
        user_growth.append((
            (start_date + timedelta(days=i)).date(),
            random.randint(1000, 5000)
        ))
    client.insert('game_stats.user_growth', user_growth)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_game_stats_db',
    default_args=default_args,
    description='Update game stats in ClickHouse',
    schedule_interval=timedelta(days=1),
)

update_db_task = PythonOperator(
    task_id='update_db',
    python_callable=update_db,
    dag=dag,
)
