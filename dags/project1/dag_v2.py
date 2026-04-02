from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import yfinance as yf
from datetime import datetime
import pandas as pd
import psycopg2
import logging

# сделать загрузку данных из yfinance в postgres 

logger = logging.getLogger(__name__)

def download_data(**context):
    
    start_date = context['logical_date']

    if start_date.weekday() >= 5:
        print(f"{start_date} - выходной нет торгов")
        return
    
    logger.info(f"Загрузка данных c yfinance за: {start_date}")
    
    data = yf.download("AAPL", start=start_date, period='1d', progress=False)
    data.columns = data.columns.droplevel(1)
    data = data.reset_index()
    data_to_pg = data.to_numpy().tolist()

    logger.info(f"Загружено {len(data)} строк данных")

    hook = PostgresHook(postgres_conn_id="postgres_pet_project")
    
    conn = hook.get_conn()

    cursor = conn.cursor()
        
    cursor.executemany("""
    INSERT INTO ks23.aapl_stock_prices (date, close, high, low, open, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    """,
    data_to_pg)

    conn.commit()

    logger.info(f"Успешно загружено в posgres {len(data_to_pg)} строк")

    cursor.close()
    conn.close()
    

with DAG(
    dag_id="aapl_to_pg_dag_v2",
    schedule_interval="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=True,
    max_active_runs=1,
    description="Загрузка данных AAPL из yfinance в PostgreSQL",
) as dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=download_data,
        provide_context=True,
    )