from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from stock_market.collector import run_collection


with DAG(
    dag_id="dnse_stock_ohlc_daily",
    description="Collect DNSE OHLC candles after the Vietnam trading session",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="30 16 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "stock-market-project",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dnse", "stock-market", "batch"],
) as dag:
    collect_market_data = PythonOperator(
        task_id="collect_and_store_raw",
        python_callable=run_collection,
    )