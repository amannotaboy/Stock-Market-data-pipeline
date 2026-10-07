from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from stock_market.collector import run_market_universe_daily


with DAG(
    dag_id="dnse_market_universe_daily",
    description=(
        "Collect the discovered DNSE stock universe and 1-minute OHLCV for "
        "market breadth and cross-sectional analysis"
    ),
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="30 17 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "stock-market-project",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["dnse", "market-universe", "minute-ohlcv", "batch"],
) as dag:
    collect_universe_and_minute_ohlcv = PythonOperator(
        task_id="collect_universe_and_minute_ohlcv",
        python_callable=run_market_universe_daily,
    )
