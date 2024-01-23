from datetime import datetime, timedelta
from airflow.decorators import dag, task
import json
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import pandas as pd

# 2. Создайте новый dag;
@dag(
    dag_id="hw_8_full_etl",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def hw_8_full_etl():
# 1. Скачайте файлы boking.csv, client.csv и hotel.csv;
# 3. Создайте три оператора для получения данных и загрузите файлы. Передайте дата фреймы в оператор трансформации;
    @task
    def hw_8_fetch_bookings():
        bookings = pd.read_csv("/home/nataliyad/airflow/dags/files/booking.csv")
        return bookings.to_json()

    @task
    def hw_8_fetch_clients():
        clients = pd.read_csv("/home/nataliyad/airflow/dags/files/client.csv")
        return clients.to_json()

    @task
    def hw_8_fetch_hotels():
        hotels = pd.read_csv("/home/nataliyad/airflow/dags/files/hotel.csv")
        return hotels.to_json()

# 4. Создайте оператор который будет трансформировать данные:
# — Объедините все таблицы в одну;
# — Приведите даты к одному виду;
# — Удалите невалидные колонки;
# — Приведите все валюты к одной;
    @task
    def hw_8_transform(**kwargs):
        ti = kwargs['ti']
        xcom_bookings = ti.xcom_pull(task_ids="hw_8_fetch_bookings")
        xcom_hotels = ti.xcom_pull(task_ids="hw_8_fetch_hotels")
        xcom_clients = ti.xcom_pull(task_ids="hw_8_fetch_clients")
        print(xcom_bookings)

        data_dict = json.loads(xcom_bookings)
        booking = pd.DataFrame(data_dict)
        print(booking)
        data_dict = json.loads(xcom_hotels)
        hotel = pd.DataFrame(data_dict)
        data_dict = json.loads(xcom_clients)
        client = pd.DataFrame(data_dict)
        client['age'].fillna(client['age'].mean(), inplace = True)
        client['age'] = client['age'].astype(int)

        # merge booking with client
        data = pd.merge(booking, client, on='client_id')
        data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)

        # merge booking, client & hotel
        data = pd.merge(data, hotel, on='hotel_id')
        data.rename(columns={'name': 'hotel_name'}, inplace=True)

        # make date format consistent
        data['booking_date'] = data['booking_date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))


        # make all cost in GBP currency
        data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
        data.currency.replace("EUR", "GBP", inplace=True)

        # remove unnecessary columns
        data = data.drop('address', axis=1)

        # load processed data
        data.to_csv("/home/nataliyad/airflow/dags/files/processed_data.csv", index=False)

# 5. Создайте оператор загрузки в базу данных;
    @task
    def hw_8_load_data():
        # conn = sqlite3.connect("/usr/local/airflow/db/datascience.db")
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        c = conn.cursor()
        c.execute('''
                    CREATE TABLE IF NOT EXISTS hw_8_booking_record (
                        client_id INTEGER NOT NULL,
                        booking_date TEXT NOT NULL,
                        room_type TEXT NOT NULL,
                        hotel_id INTEGER NOT NULL,
                        booking_cost NUMERIC,
                        currency TEXT,
                        age INTEGER,
                        client_name TEXT,
                        client_type TEXT,
                        hotel_name TEXT
                    );
                 ''')
        with open("/home/nataliyad/airflow/dags/files/processed_data.csv", "r") as file:
            c.copy_expert(
			"COPY hw_8_booking_record FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
			file,
		)
        conn.commit()


    [hw_8_fetch_bookings(), hw_8_fetch_hotels(), hw_8_fetch_clients()] >> hw_8_transform() >> hw_8_load_data()


# 6. Запустите dag.
dag = hw_8_full_etl()
