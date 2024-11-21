import duckdb
import pandas as pd
import os  
from dotenv import load_dotenv, find_dotenv
import requests
from io import StringIO

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


lista = ['PAIS_BLOCO', 'UF', 'PAIS', 'UF_MUN', 'NCM_SH', 'NCM_FAT_AGREG','NCM']
lista_exp_imp = ['EXP', 'IMP'] 
lista_anos = [2023, 2024] 

# Função para copiar dados das tabelas de dimensões do site para o MinIO em formato Parquet
def process_csvs_to_parquet(lista, path_minio):
    for tabela in lista:
        # URL do arquivo CSV
        url = f"https://balanca.economia.gov.br/balanca/bd/tabelas/{tabela}.csv"
        response = requests.get(url, verify=False)
        
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data, sep=';', encoding='ISO-8859-1')

        # Salvar diretamente como Parquet no caminho especificado
        con.sql(f""" 
            COPY (SELECT * FROM df) 
            TO '{path_minio}/{tabela}.parquet' (FORMAT 'parquet');
         """)

# Caminho de destino (path_minio) para salvar os arquivos parquet
path_minio = 's3://landing/comex' # Altere para o seu caminho



# Função para copiar dados de exportação e importação para o MinIO em formato Parquet
def copy_exp_imp():
    for tabela in lista_exp_imp:
        for ano in lista_anos:
            con.sql(f""" 
                COPY (SELECT * FROM read_csv('https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/{tabela}_{ano}.csv', ignore_errors=true)) 
                TO '{path_minio}/{tabela}/{ano}.parquet' (FORMAT 'parquet');
            """)

# Chama as funções para iniciar o processo de cópia das tabelas
process_csvs_to_parquet(lista, path_minio)
copy_exp_imp()

# Fecha a conexão com o DuckDB após todas as operações
con.close()

