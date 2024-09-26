from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

# Function to print "Hello, World!"
def getApiWeather():
    api_key = "be3cd750818b498b0379e988ed69cd85"  # แทนที่ด้วย API key ของคุณ
    city = 'Bangkok'
    units = "metric"
    lang = 'th'
    url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units={}&lang={}'
  
    weather = requests.get(url.format(city, api_key, units, lang)).json()
    print(f'weather: {weather}')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=datetime(2024, 9, 26),  # Start date
    catchup=False,
) as dag:

    # Define the PythonOperator
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=getApiWeather,
    )

    # Set task dependencies if any (not needed in this simple example)
    hello_task

