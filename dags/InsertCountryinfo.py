from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json



def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_Country_info():
    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)
    data = json.loads(response.text)
    return data

@task
def load(schema, table, records):
    logging.info("load started")
    
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} (country text, population bigint, area text)")
        
        cur.execute(f"CREATE TEMP TABLE temp_{table}(country text, population bigint, area text)")
        for r in records:
            country = r['name']['official']
            population = r['population']
            area = r['area']
            
            cur.execute(f"INSERT INTO temp_{table} VALUES (%s, %s, %s)", [country, population, area])
            
        cur.execute(f"DELETE FROM {schema}.{table} WHERE country IN (SELECT country FROM temp_{table})")
        cur.execute(f"INSERT INTO {schema}.{table} SELECT * FROM temp_{table}")
        cur.execute("COMMIT;")    
    except Exception as error:
        logging.error(error)
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
    logging.info("load done")


with DAG(
    dag_id = 'InsertCountryinfo',
    start_date = datetime(2025,5,26),
    catchup = False,
    tags = ['API'],
    schedule = '30 6 * * 6'
) as dag:
    country_info = get_Country_info()
    load("cksgh3422", "country_info", country_info)
    