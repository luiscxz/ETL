# librerias de airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


from datetime import datetime, timedelta # Se requiere para iniciar el DAG

default_args = {
    'owner': 'Codigo facilito Tema',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # cantidad de reintentos
    'retry_delay':timedelta(minutes=2) # si falla reintenta en 2 minutos
}

# Procedemos a construir el DAG
with DAG(
    'DAG_ETL_PostgreSQL',  # Nombre del DAG
    default_args=default_args,
    description='Creación del DAG ETL postgreSQL',
    schedule=None,  # No tenemos intervalo de programación
    start_date=datetime(2024, 1, 1),  # Necesario para evitar errores de inicio
    catchup=False,  # Para evitar ejecuciones retroactivas
    tags=['ETL', 'Ciencia de datos','PostgreSQL']
) as dag: