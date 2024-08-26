Game Analytics Dashboard
Этот проект предоставляет аналитику игрового процесса, включая метрики пользователей, ошибки, финансовые транзакции и другие аспекты, на базе ClickHouse и Apache Airflow. 
Настроенный дашборд в Grafana визуализирует данные, позволяя отслеживать динамику пользователей, активность и другие ключевые метрики.

Структура проекта:
clickhouse: Папка с Docker Compose файлами для развёртывания базы данных ClickHouse.
airflow: Папка с Docker Compose файлами для развёртывания Apache Airflow и DAG для инициализации и заполнения базы данных.
1.py: Python-скрипт для инициализации базы данных и её заполнения начальными данными.
New dashboard.json: JSON файл с конфигурацией дашборда для Grafana.

Установка и настройка
1)Скачайте/установите официальные Python и Docker
2)Развёртывание ClickHouse и Airflow
Перейдите в папку clickhouse и запустите:
docker-compose up -d
Перейдите в папку airflow и запустите:
docker-compose up -d
3)Установите необходимые библиотеки в Airflow, например, clickhouse-connect:
pip install clickhouse-connect
4)Инициализация и заполнение базы данных
Запустите скрипт 1.py, чтобы создать необходимые таблицы и заполнить их данными:
python 1.py
5)Настройка Grafana
Импортируйте JSON файл дашборда New dashboard.json в Grafana, чтобы получить готовый дашборд.

Используемые технологии:
Python
Docker
ClickHouse
Apache Airflow
Grafana
