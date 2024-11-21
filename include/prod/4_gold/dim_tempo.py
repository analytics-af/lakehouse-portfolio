import duckdb  # Importa o DuckDB para manipulação de dados e execução de SQL
import os  # Importa o módulo os para interagir com variáveis de ambiente do sistema
from dotenv import load_dotenv, find_dotenv  # Importa funções para carregar variáveis de ambiente de um arquivo .env
from deltalake import DeltaTable, write_deltalake
from datetime import datetime

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
path_minio_gold = 's3://gold/comex/dim_tempo'
path_minio_silver = 's3://silver/comex'

storage_options = {
    "AWS_ENDPOINT_URL": f"http://{HOST_MINIO}:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true"
}

ANO_MAX = datetime.now().year + 1

dim_tempo = con.sql(f"""
                WITH generate_date AS (
                    SELECT CAST(RANGE AS DATE) AS date_key 
                  FROM RANGE(DATE '1997-01-01', DATE '{ANO_MAX}-12-31', INTERVAL 1 DAY)
                )

                SELECT date_key AS data,
                    YEAR(date_key) || MONTH(date_key) || DAYOFMONTH(date_key) AS cod_date,
                    MONTH(date_key) AS mes,
                    MONTHNAME(date_key) AS mes_nome,
                    DAYOFMONTH(date_key) AS dia,
                    CAST(YEAR(date_key) AS INT) AS ano
                FROM generate_date

        """).to_arrow_table()

write_deltalake(
        f'{path_minio_gold}',
        dim_tempo,
        mode='overwrite',
        storage_options=storage_options
)

con.close()