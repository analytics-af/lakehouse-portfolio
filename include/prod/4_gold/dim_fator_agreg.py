import duckdb  # Importa o DuckDB para manipulação de dados e execução de SQL
import os  # Importa o módulo os para interagir com variáveis de ambiente do sistema
from dotenv import load_dotenv, find_dotenv  # Importa funções para carregar variáveis de ambiente de um arquivo .env
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
path_minio_gold = 's3://gold/comex/dim_fator_agregado'
path_minio_silver = 's3://silver/comex'

storage_options = {
    "AWS_ENDPOINT_URL": f"http://{HOST_MINIO}:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true"
}

dim_fator_agregado = con.sql(f"""
        WITH silver_fator_agregado AS 
        (
        SELECT DISTINCT
            cod_fator_agregado,
            nome_fator_agregado,
            grupo_fator_agregado 
        FROM delta_scan('{path_minio_silver}/importacoes')
        WHERE nome_fator_agregado IS NOT NULL
        UNION 
        SELECT DISTINCT
            cod_fator_agregado,
            nome_fator_agregado,
            grupo_fator_agregado 
        FROM delta_scan('{path_minio_silver}/importacoes')
        WHERE nome_fator_agregado IS NOT NULL
        )
        
        SELECT 
            S.cod_fator_agregado,
            S.nome_fator_agregado,
            S.grupo_fator_agregado 
        FROM silver_fator_agregado S
        WHERE  S.cod_fator_agregado NOT IN
        (
            SELECT DISTINCT cod_fator_agregado FROM delta_scan('{path_minio_gold}')
        )

    """).to_arrow_table()

if len(dim_fator_agregado) > 0:
    write_deltalake(
            f'{path_minio_gold}',
            dim_fator_agregado,
            mode='append',
            storage_options=storage_options
    )

con.close()