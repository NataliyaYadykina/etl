from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import random


dag = DAG(
	'hw_6',
	description = 'Homework 6',
	schedule_interval = '*/5 * * * *',
	start_date = datetime(2024, 1, 17),
	catchup = False
)


# 1. Создайте новый граф добавьте в него BashOperator,
# который будет генерировать рандомное число и печатать его в консоль
# -------------------------------------------------------------------

hw_6_task_1_random_number_bash = BashOperator(
	task_id="hw_6_task_1_random_number_bash",
	bash_command='echo $RANDOM',
	dag=dag,
)


# 2. Создайте PythonOperator, который генерирует рандомное число,
# возводит его в квадрат и выводит в консоль исходное число и результат
# ---------------------------------------------------------------------

def py_random_number():
	number = random.randrange(10)
	sqr = number*number
	print(f"Number: {number}")
	print(f"Squared number: {sqr}")

hw_6_task_2_random_number_py = PythonOperator(
	task_id="hw_6_task_2_random_number_py",
	python_callable=py_random_number,
	dag=dag,
)


# 3. Сделайте оператор, который отправляет запрос
# к https://goweather.herokuapp.com/weather/"location".
# Вместо location используйте ваше местоположение.
# -----------------------------------------------------

hw_6_task_3_http_weather = HttpSensor(
	task_id="hw_6_task_3_http_weather",
	http_conn_id="hw_6_weather",
	endpoint="",
	request_params={},
	response_check=lambda response: response.json()["fact"]["temp"],
	poke_interval=5,
	headers={'X-Yandex-API-Key': 'xxx'},
	dag=dag,
)


# Получим данные о погоде
hw_6_task_3_http_weather_get_data = SimpleHttpOperator(
    task_id='hw_6_task_3_http_weather_get_data',
    method='GET',
    http_conn_id='hw_6_weather',
    endpoint='',
    headers={'X-Yandex-API-Key': 'xxx'},
    log_response=True
)

hw_6_task_1_random_number_bash >> hw_6_task_2_random_number_py >> hw_6_task_3_http_weather >> hw_6_task_3_http_weather_get_data


















