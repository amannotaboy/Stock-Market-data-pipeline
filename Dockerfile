FROM apache/airflow:2.10.5-python3.11

USER airflow
RUN pip install --no-cache-dir "psycopg2-binary==2.9.10"