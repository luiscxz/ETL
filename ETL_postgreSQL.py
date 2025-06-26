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


#---------------------------------------------------------------------------#
#                            Area de funciones                              #

# función que se conecta a la API de mockaroo
def _get_api():
    import requests
    url = 'https://my.api.mockaroo.com/a_pi.json'
    headers = {'X-API-Key': '2d672d60'}
    response = requests.get(url,headers)
    with open('/tmp/sales_db_py.csv','wb') as file:
        file.write(response.content)
        file.close

# función que transforma los datos
def _join_trans():
    import pandas as pd
    df_py = pd.read_csv('/tmp/sales_db_py.csv')
    df_bash = pd.read_csv('/tmp/sales_db_bash.csv')
    df = pd.concat([df_py,df_bash], ignore_index=True)
    df = df.groupby(['Fecha','Region'])['Costo'].sum().reset_index()
    df.to_csv('/tmp/sales_db.csv',sep='\t', index=False, header=False)
    print(df.head())



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
    # area donde se llaman las funciones
    get_api_python = PythonOperator(
        task_id = 'get_api_python',
        python_callable = _get_api
    )

    # definamos el bashOperator para que descargue la información
    get_api_bash = BashOperator(
        task_id = 'get_api_bash',
        bash_command = 'curl -H "X-API-Key: 2d672d60" https://my.api.mockaroo.com/a_pi.json > /tmp/sales_db_bash.csv'
    )