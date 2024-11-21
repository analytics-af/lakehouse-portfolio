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
path_minio_landing = 's3://landing/comex/IMP'
path_minio_bronze = 's3://bronze/comex/importacoes'

storage_options = {
    "AWS_ENDPOINT_URL": f"http://{HOST_MINIO}:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true"
}

df = con.sql(f"""
        SELECT * FROM delta_scan('{path_minio_bronze}')
        """).to_df()

# ano máximo do bronze
max_ano = con.sql("SELECT max(ano) FROM df").to_df().iloc[0, 0]

# mês máximo correspondente ao ano máximo do bronze
max_mes = con.sql(f"SELECT max(mes) FROM df WHERE ano = {max_ano}").to_df().iloc[0, 0]

df_imp = con.sql(f"""   
            SELECT
            CAST(CO_ANO as INT) as ano,
            CAST(CO_MES as INT) as mes,
            CO_NCM as cod_NCM,
            CO_UNID as cod_unidade,
            CO_PAIS as cod_pais,
            SG_UF_NCM as sigla_estado,
            CO_VIA as cod_via,
            CO_URF as cod_urf,
            QT_ESTAT as qtd,
            KG_LIQUIDO as kg_liquido,
            VL_FOB as valor_fob,
            VL_FRETE as valor_frete,
            VL_SEGURO as valor_seguro
        FROM '{path_minio_landing}/*.parquet'
        WHERE CAST(CO_ANO as INT) > {max_ano} OR
        (CAST(CO_ANO as INT) = {max_ano}
        AND CAST(CO_MES as INT) > {max_mes})
        """).to_arrow_table()

if len(df_imp) > 0:
    write_deltalake(
            f'{path_minio_bronze}',
            df_imp,
            storage_options=storage_options,
            mode='append',
            partition_by=["ano","mes"]
        )

con.close()