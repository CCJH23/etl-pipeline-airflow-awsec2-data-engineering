from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json


# convert kelvin temperature to celsius
def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius


def transform_load_data(task_instance): # task_instance is the previous task you want to pass to this task
    data = task_instance.xcom_pull(task_ids="extract_weather_data") # pull data from the previous task that extracted the data from the openweather api

    # get individual fields from data
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    feels_like_celsius = kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    # use the fields to form a new dictionary that represents the transformed data
    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (Celsius)": temp_celsius,
                        "Feels Like (Celsius)": feels_like_celsius,
                        "Minimun Temp (Celsius)":min_temp_celsius,
                        "Maximum Temp (Celsius)": max_temp_celsius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time      
                        }
    
    # convert to pandas dataframe
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    # provide aws credentials as a dictionary for dag to run
    aws_credentials = {"key": "<access key id>", "secret": "<secret access key>", "token": "<session token>"}

    # file name to include the date and the time too and joining to the s3 path string
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_singapore_' + dt_string
    
    # convert dataframe to csv with the provided name and save in the s3 bucket, providing the aws_credentials to give the dag the privilege to save the csv inside the s3 bucket
    df_data.to_csv(f"s3://weatherapiairflowbucket-yml-clovis/{dt_string}.csv", index=False, storage_options=aws_credentials) 


# define dag configurations
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['clovischowjh@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2) # if fail, retry every 2 minutes
}

with DAG('weather_dag', # needs to be unique across all dags
    default_args=default_args,
    schedule_interval = '@daily', # to run this dag daily
    catchup=False) as dag: # catchup=False means if run now, don't run from 8 Jan 2023 onwards 

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready', # needs to be unique in this dag
        http_conn_id='weathermap_api', # http_conn_id is the connection id to connect to airflow
        endpoint='/data/2.5/weather?q=Singapore&APPID=<OpenWeatherMap API Key>' # endpoint to run 
    )

    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Singapore&APPID=<OpenWeatherMap API Key>',
        method='GET',
        response_filter=lambda r: json.loads(r.text), # to get the data and convert into a python dictionary
        log_response=True
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data # python function to run for this task
    )


# means is_weather_api_ready task will go first followed by extract_weather_data task, followed by transform_load_weather_data
is_weather_api_ready >> extract_weather_data >> transform_load_weather_data

