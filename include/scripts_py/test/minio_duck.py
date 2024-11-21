import duckdb
from datetime import datetime
import pandas as pd
from deltalake import DeltaTable, write_deltalake
import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import sys

#.env localizado no diretorio: include/.env

path = find_dotenv()
load_dotenv(path)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_MINIO")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY_MINIO")  # Ajuste conforme necessário para capturar o secret key
HOST_MINIO = os.getenv("HOST_MINIO")

# Conectar ao DuckDB
con = duckdb.connect()

# Instalar e carregar o módulo HTTPFS para acesso ao S3/MinIO
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Criar o segredo no DuckDB usando as variáveis capturadas
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

con.sql("""CREATE TABLE devolucoes AS
        SELECT * FROM 's3://comercio-importacao-exportacao/devolucoes_2.json';
        
        """)

con.sql("""
        COPY devolucoes TO 's3://comercio-importacao-exportacao/devolucoes.parquet' (FORMAT 'parquet');
        
        """)





