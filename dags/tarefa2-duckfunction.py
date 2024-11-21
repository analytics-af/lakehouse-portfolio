import duckdb
from datetime import datetime,timedelta
import pandas as pd
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from deltalake import DeltaTable, write_deltalake

import os  
from dotenv import load_dotenv, find_dotenv 

# Configuração dos argumentos padrões
default_args = {
    'owner': 'airflow',              # Indica o dono da DAG.
    'depends_on_past': False,        # Se as execuções futuras devem esperar que a execução anterior complete com sucesso.
    'email_on_failure': False,       # Se deve enviar email quando a tarefa falhar.
    'email_on_retry': False,         # Se deve enviar email quando houver retry.
    'retries': 1,                    # Número de vezes que a tarefa irá tentar em caso de falha.
    'retry_delay': timedelta(minutes=5),  # Tempo de espera entre tentativas.
}

# Carrega as variáveis de ambiente definidas no arquivo .env
load_dotenv(find_dotenv())

# Define as credenciais de acesso ao MinIO a partir das variáveis de ambiente
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_MINIO") 
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY_MINIO")  
HOST_MINIO = os.getenv("HOST_MINIO")  

# Conecta ao DuckDB, criando uma instância de conexão
con = duckdb.connect()

# Cria uma secret no DuckDB para acessar o MinIO com as credenciais fornecidas
con.execute(f"""
    CREATE SECRET my_minio_secret (
        TYPE 'S3',
        KEY_ID '{AWS_ACCESS_KEY}',
        SECRET '{AWS_SECRET_KEY}',
        REGION 'us-east-1',
        ENDPOINT '{HOST_MINIO}:9000',
        URL_STYLE 'path',
        USE_SSL false
    );
""")

# Define o caminho de destino no MinIO para os arquivos Parquet
path_minio = 's3://landing/comex'

lista_exp_imp = ['EXP', 'IMP'] 
lista_anos = [2023, 2024] 

def roda_arquivos_landing():
    for ano in lista_anos:
        for exp_imp in lista_exp_imp:
            con.sql(f"""
                COPY (SELECT * FROM '{path_minio}/EXP_IMP/{exp_imp}_{ano}.parquet') 
                TO '{path_minio}/{exp_imp}/{ano}.parquet';
                """)
            
dag = DAG(
    'hello_world_dag',        
    default_args=default_args,       
    description='Minha primeira DAG no Airflow',  
    schedule_interval=timedelta(days=1),  
    start_date=days_ago(1),          
    catchup=False,                   
)

roda_duck = PythonOperator(
    task_id='roda_duck',
    python_callable=roda_arquivos_landing,
    dag=dag
)

roda_arquivos_landing