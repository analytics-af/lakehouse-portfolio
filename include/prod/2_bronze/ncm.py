#
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
path_minio_landing = 's3://landing/comex'
path_minio_bronze = 's3://bronze/comex/ncm'

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
        CO_NCM AS cod_ncm,
        CO_UNID AS cod_unid,
        CO_SH6 AS cod_sh6,
        CO_PPE AS cod_ppe,
        CO_PPI AS cod_ppi,
        CO_FAT_AGREG AS cod_fat_agreg,
        CO_CUCI_ITEM AS cod_cuci_item,
        CO_CGCE_N3 AS cod_cgce_n3,
        CO_SIIT AS cod_siit,
        CO_ISIC_CLASSE AS cod_isic_classe,
        CO_EXP_SUBSET AS cod_exp_subset,
        NO_NCM_POR AS no_ncm_por
    FROM '{path_minio_landing}/NCM.parquet';
        """).to_arrow_table()

table_path = f'{path_minio_bronze}'
table = DeltaTable(table_path, storage_options=storage_options)

table.merge(
    source=df,
    predicate='target.cod_ncm = source.cod_ncm',
    source_alias="source",
    target_alias="target",
).when_not_matched_insert_all().execute()

con.close()