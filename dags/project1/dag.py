from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import yfinance as yf
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime
import logging

# сделать загрузку данных из yfinance в postgres 

logger = logging.getLogger(__name__)

def download_data(**context):
    
    start_date = context['logical_date']
    
    logger.info(f"Загрузка данных c yfinance за: {start_date}")
    logger.info(f"Конец интервала: {context['data_interval_end']}")
    
    data = yf.download("AAPL", start=start_date, period='1d', progress=False)
    data.columns = data.columns.droplevel(1)
    data = data.reset_index()
    data_to_pg = data.to_records(index=False).tolist()

    logger.info(f"Загружено {len(data)} строк данных")

    hook = PostgresHook(postgres_conn_id="postgres_pet_project")
    
    conn = hook.get_conn()

    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS ks23")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS ks23.aapl_stock_prices (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        close FLOAT,
        high FLOAT,
        low FLOAT,
        open FLOAT,
        volume BIGINT
    )
    """
    cursor.execute(create_table_query)
        
    insert_query = """
    INSERT INTO ks23.aapl_stock_prices (date, close, high, low, open, volume)
    VALUES %s
    """

    execute_values(cursor, insert_query, data_to_pg)

    conn.commit()

    logger.info(f"✅ Успешно загружено в posgres {len(data_to_pg)} строк")

    cursor.close()
    conn.close()
    

with DAG(
    dag_id="aapl_to_pg_dag",
    schedule_interval="0 0 * * 1-5",
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