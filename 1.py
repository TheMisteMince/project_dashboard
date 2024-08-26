import clickhouse_connect
from datetime import datetime, timedelta
import random

# Подключение к базе данных ClickHouse
client = clickhouse_connect.get_client(host='localhost', port=8123, username='admin', password='admin')

# Удаление старой базы данных, если существует
client.command("DROP DATABASE IF EXISTS game_stats")

# Создание базы данных
client.command('CREATE DATABASE IF NOT EXISTS game_stats')

# Создание таблиц
client.command('''
CREATE TABLE IF NOT EXISTS game_stats.user_activity (
    user_id UInt32,
    activity_score UInt32,
    login_times Array(DateTime)
) ENGINE = MergeTree() ORDER BY user_id
''')

client.command('''
CREATE TABLE IF NOT EXISTS game_stats.win_loss_stats (
    user_id UInt32,
    wins UInt32,
    losses UInt32
) ENGINE = MergeTree() ORDER BY user_id
''')

client.command('''
CREATE TABLE IF NOT EXISTS game_stats.avg_play_time (
    user_id UInt32,
    avg_time Float32
) ENGINE = MergeTree() ORDER BY user_id
''')

client.command('''
CREATE TABLE IF NOT EXISTS game_stats.activity_heatmap (
    user_id UInt32,
    hour UInt8,
    activity_score UInt32
) ENGINE = MergeTree() ORDER BY (user_id, hour)
''')

client.command('''
CREATE TABLE IF NOT EXISTS game_stats.financial_transactions (
    transaction_id UInt32,
    user_id UInt32,
    amount Float32,
    transaction_type String,
    transaction_date DateTime
) ENGINE = MergeTree() ORDER BY transaction_id
''')

client.command('''
CREATE TABLE IF NOT EXISTS game_stats.game_events (
    event_id UInt32,
    user_id UInt32,
    event_type String,
    event_time DateTime,
    event_description String
) ENGINE = MergeTree() ORDER BY event_id
''')

client.command('''
CREATE TABLE IF NOT EXISTS game_stats.error_logs (
    error_id UInt32,
    user_id UInt32,
    error_type String,
    error_time DateTime,
    error_message String
) ENGINE = MergeTree() ORDER BY error_id
''')

client.command('''
CREATE TABLE IF NOT EXISTS game_stats.user_growth (
    date Date,
    user_count UInt32
) ENGINE = MergeTree() ORDER BY date
''')

# Заполнение таблиц

# Генерация и вставка активности пользователей
user_activity = []
for i in range(1, 101):
    user_activity.append((
        i,
        random.randint(1, 100),
        [datetime.now() - timedelta(days=random.randint(0, 30)) for _ in range(random.randint(1, 5))]
    ))
client.insert('game_stats.user_activity', user_activity)

# Генерация и вставка статистики побед и поражений
win_loss_stats = []
for i in range(1, 101):
    win_loss_stats.append((i, random.randint(0, 50), random.randint(0, 50)))
client.insert('game_stats.win_loss_stats', win_loss_stats)

# Генерация и вставка среднего времени в игре
avg_play_time = []
for i in range(1, 101):
    avg_play_time.append((i, random.uniform(1.0, 5.0)))
client.insert('game_stats.avg_play_time', avg_play_time)

# Генерация и вставка тепловой карты активности по времени суток
activity_heatmap = []
for i in range(1, 101):
    for hour in range(0, 24):
        activity_heatmap.append((i, hour, random.randint(0, 100)))
client.insert('game_stats.activity_heatmap', activity_heatmap)

# Генерация и вставка финансовых транзакций
financial_transactions = []
for i in range(1, 501):
    financial_transactions.append((
        i,
        random.randint(1, 100),
        random.uniform(5.0, 500.0),
        random.choice(['purchase', 'sale']),
        datetime.now() - timedelta(days=random.randint(0, 30))
    ))
client.insert('game_stats.financial_transactions', financial_transactions)

# Генерация и вставка игровых событий
game_events = []
for i in range(1, 501):
    game_events.append((
        i,
        random.randint(1, 100),
        random.choice(['kill', 'death', 'quest_completed']),
        datetime.now() - timedelta(days=random.randint(0, 90)),
        f'Event description {i}'
    ))
client.insert('game_stats.game_events', game_events)

# Генерация и вставка ошибок
error_logs = []
for i in range(1, 201):
    error_logs.append((
        i,
        random.randint(1, 100),
        random.choice(['network_error', 'server_error']),
        datetime.now() - timedelta(days=random.randint(0, 30)),
        f'Error message {i}'
    ))
client.insert('game_stats.error_logs', error_logs)

# Генерация и вставка роста пользователей
user_growth = []
start_date = datetime.now() - timedelta(days=365)
for i in range(0, 366):
    user_growth.append((
        (start_date + timedelta(days=i)).date(),
        random.randint(1000, 5000)
    ))
client.insert('game_stats.user_growth', user_growth)


