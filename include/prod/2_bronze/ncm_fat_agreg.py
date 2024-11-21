import duckdb  # Importa o DuckDB para manipulação de dados e execução de SQL
import os  # Importa o módulo os para interagir com variáveis de ambiente do sistema
from dotenv import load_dotenv, find_dotenv  # Importa funções para carregar variáveis de ambiente de um arquivo .env
from datetime import datetime
import pandas as pd
from deltalake import DeltaTable, write_deltalake
# Carrega as variáveis de ambiente definidas no arquivo .env
load_dotenv(find_dotenv())

# Define as credenciais de acesso ao MinIO a partir das variáveis de ambiente
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_MINIO")  # Chave de acesso do MinIO
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY_MINIO")  # Chave secreta do MinIO
HOST_MINIO = os.getenv("HOST_MINIO")  # Host do MinIO

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
path_minio_landing = 's3://landing/comex'
path_minio_bronze = 's3://bronze/comex/ncm_fat_agreg'

storage_options = {
    "AWS_ENDPOINT_URL": f"http://{HOST_MINIO}:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true"
}

df = con.sql(f"""
        SELECT 
        CAST(CO_FAT_AGREG  as int) AS cod_fator_agregado,
        NO_FAT_AGREG AS nome_fator_agregado,
        NO_FAT_AGREG_GP AS grupo_fator_agregado
         from '{path_minio_landing}/NCM_FAT_AGREG.parquet';        
        """).to_arrow_table()

table_path = f'{path_minio_bronze}'

# Conecte à tabela Delta existente
table = DeltaTable(table_path, storage_options=storage_options)

table.merge(
    source=df,
    predicate='target.cod_fator_agregado = source.cod_fator_agregado',
    source_alias="source",
    target_alias="target",
).when_not_matched_insert_all().execute()

con.close()

