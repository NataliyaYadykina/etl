from datetime import datetime
from airflow import DAG
import os
import requests
import pendulum
from airflow.decorators import task, dag
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule

def sem_8_check_conn(url, headers={}, payload={}):
	return requests.request("GET", url, headers=headers, data=payload).status_code

def sem_8_check_conn_yandex_weather():
	if sem_8_check_conn(
		"https://api.weather.yandex.ru/v2/forecast/?lat=55.75396&lon=37.620393",
		headers={'X-Yandex-API-Key': 'xxxxxxx'}
	) == 200:
		return ['sem_8_get_data_yandex_weather', 'sem_8_check_conn_open_weather_task']
	else:
		return 'sem_8_print_error_bash'

def sem_8_check_conn_open_weather():
        if sem_8_check_conn("https://api.openweathermap.org/data/2.5/weather?q=Moscow&appid=xxxxxxxxxxxxx") == 200:
                return 'sem_8_get_data_open_weather'
        else:
                return 'sem_8_print_error_bash'

with DAG(
	'sem_8_weather_to_telegram',
	start_date=datetime(2024, 1, 1),
	catchup=False,
	tags=['seminars_ETL'],
) as dag:

	sem_8_check_conn_yandex_weather_task=BranchPythonOperator(
		task_id='sem_8_check_conn_yandex_weather_task',
		python_callable=sem_8_check_conn_yandex_weather,
	)

	sem_8_check_conn_open_weather_task=BranchPythonOperator(
                task_id='sem_8_check_conn_open_weather_task',
                python_callable=sem_8_check_conn_open_weather,
		trigger_rule=TriggerRule.ALL_DONE,
        )

	sem_8_print_error_bash_yandex=BashOperator(
		task_id='sem_8_print_error_bash_yandex',
		bash_command='Error! Try again later.'
	)

	sem_8_print_error_bash_open=BashOperator(
                task_id='sem_8_print_error_bash_open',
                bash_command='Error! Try again later.'
        )

	sem_8_get_data_yandex_weather = SimpleHttpOperator(
		task_id='sem_8_get_data_yandex_weather',
		method='GET',
		http_conn_id='hw_6_weather',
		endpoint='',
		headers={'X-Yandex-API-Key': 'xxxxxxxxxxx'},
		response_filter=lambda response: response.json()['fact']['temp'],
		log_response=True
	)

	sem_8_get_data_open_weather = SimpleHttpOperator(
                task_id='sem_8_get_data_open_weather',
                method='GET',
                http_conn_id='sem_8_open_weather',
                endpoint='',
                headers={},
                response_filter=lambda response: round(float(response.json()['main']['temp']) - 273.15, 2),
                log_response=True
        )

	sem_8_send_message_to_telegram_bot=TelegramOperator(
		task_id='sem_8_send_message_to_telegram_bot',
		token='xxx',
		chat_id='yyy',
		text='Weather in Moscow:\nYandex: ' + "{{ ti.xcom_pull(task_ids='sem_8_get_data_yandex_weather') }}" + "\nOpen weather: " + "{{ ti.xcom_pull(task_ids='sem_8_get_data_open_weather') }}",
		trigger_rule=TriggerRule.ALL_DONE,
	)


	sem_8_check_conn_yandex_weather_task >> [sem_8_print_error_bash_yandex, sem_8_get_data_yandex_weather] >> sem_8_check_conn_open_weather_task >> [sem_8_print_error_bash_open, sem_8_get_data_open_weather] >> sem_8_send_message_to_telegram_bot
