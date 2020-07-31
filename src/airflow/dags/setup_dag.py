import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageCsvToPostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers import SqlQueries

dag = DAG('setup_database',
          description='Create table schema in database',
          schedule_interval=None,
          start_date=datetime.datetime(2019, 1, 1)
          )

# Create staging table for log data
create_staging_immigration_table = PostgresOperator(
    task_id="create_staging_immigration_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_immigration
)

# Create staging table for song data
create_staging_countries_table = PostgresOperator(
    task_id="create_staging_countries_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_countries
)

create_staging_ports_table = PostgresOperator(
    task_id="create_staging_ports_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_ports_table
)

create_staging_airports_table = PostgresOperator(
    task_id="create_staging_airports_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_airports_table
)

create_staging_demographics_table = PostgresOperator(
    task_id="create_staging_demographics_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_demographics_table
)

stage_port_codes_task = StageCsvToPostgresOperator(
    task_id="stage_port_codes",
    postgres_conn_id="redshift",
    dag=dag,
    table_name="public.staging_ports",
    csv_path="/data/datalake/i94_ports.csv",
    truncate_table=True
)

stage_country_codes_task = StageCsvToPostgresOperator(
    task_id="stage_country_codes",
    postgres_conn_id="redshift",
    dag=dag,
    table_name="public.staging_countries",
    csv_path="/data/datalake/i94_countries.csv",
    truncate_table=True
)

stage_airports_codes_task = StageCsvToPostgresOperator(
    task_id="stage_airports_codes",
    postgres_conn_id="redshift",
    dag=dag,
    table_name="public.staging_airports",
    csv_path="/data/datalake/airport-codes_csv.csv",
    truncate_table=True
)

stage_demographics_task = StageCsvToPostgresOperator(
    task_id="stage_demographics",
    postgres_conn_id="redshift",
    dag=dag,
    table_name="public.staging_demographics",
    csv_path="/data/datalake/us-cities-demographics.csv",
    truncate_table=True,
    delimiter = ";"
)

def stage_city_temperatures():

    data_lake_path = "/data/datalake"
    partition_path = f"{data_lake_path}/us_city_temperature_data"
    df = pd.read_parquet(partition_path)
    postgres_hook = PostgresHook('redshift')
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql('staging_city_temperatures', engine,if_exists='replace')


stage_city_temperatures_task = PythonOperator(
    task_id="stage_city_temperatures",
    dag=dag,
    python_callable = stage_city_temperatures
)

def stage_country_temperatures():
    data_lake_path = "/data/datalake"
    partition_path = f"{data_lake_path}/country_temperature_data"
    df = pd.read_parquet(partition_path)
    postgres_hook = PostgresHook('redshift')
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql('staging_country_temperatures', engine,if_exists='replace')


stage_country_temperatures_task = PythonOperator(
    task_id="stage_country_temperatures",
    dag=dag,
    python_callable = stage_country_temperatures

)



start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_staging_immigration_table
start_operator >> create_staging_countries_table
start_operator >> create_staging_ports_table
start_operator >> create_staging_airports_table
start_operator >> create_staging_demographics_table
start_operator >> stage_country_temperatures_task
start_operator >> stage_city_temperatures_task

create_staging_countries_table >> stage_country_codes_task
create_staging_ports_table >> stage_port_codes_task
create_staging_airports_table >> stage_airports_codes_task
create_staging_demographics_table >> stage_demographics_task

create_staging_immigration_table >> end_operator
stage_country_codes_task >> end_operator
stage_port_codes_task >> end_operator
stage_airports_codes_task >> end_operator
stage_demographics_task >> end_operator
stage_city_temperatures_task >> end_operator
stage_country_temperatures_task >> end_operator
