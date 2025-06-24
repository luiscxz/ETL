from airflow import DAG
from airflow.operators.empty import EmptyOperator  # Cambio de DummyOperator a EmptyOperator
from datetime import datetime  # Se requiere para iniciar el DAG

default_args = {
    'owner': 'Codigo facilito Tema',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Procedemos a construir el DAG
with DAG(
    'DAG_ETL_Dummy',  # Nombre del DAG
    default_args=default_args,
    description='Creación del DAG ETL Dummy',
    schedule=None,  # No tenemos intervalo de programación
    start_date=datetime(2024, 1, 1),  # Necesario para evitar errores de inicio
    catchup=False,  # Para evitar ejecuciones retroactivas
    tags=['ETL', 'Ciencia de datos']
) as dag:
    
    # Procedemos a llamar los operadores
    get_api_bash = EmptyOperator(task_id='get_api_bash')
    get_api_python = EmptyOperator(task_id='get_api_python')

    # Tarea de unión y transformación 
    join_trans = EmptyOperator(task_id='join_trans')

    # Tarea que guarda en PostgreSQL
    load_postgreSQL = EmptyOperator(task_id='load_postgreSQL')

    # Definamos las dependencias
    [get_api_bash, get_api_python] >> join_trans >> load_postgreSQL

