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
path_minio_gold = 's3://gold/comex'
path_minio_silver = 's3://silver/comex'

storage_options = {
    "AWS_ENDPOINT_URL": f"http://{HOST_MINIO}:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true"
}

fat_exportacoes = con.sql(f"""
        
        SELECT 
            ex.ano,
            ex.mes,
            dt.cod_date,
            sh.cod_ncm,
            fa.cod_fator_agregado ,
            uf.cod_estado,
            p.cod_pais,
            ex.qtd,
            ex.kg_liquido,
            ex.valor_fob
        FROM delta_scan('{path_minio_silver}/exportacoes') as ex
        LEFT JOIN delta_scan('{path_minio_gold}/dim_ncm_sh') as sh ON ex.cod_ncm = sh.cod_ncm
        LEFT JOIN delta_scan('{path_minio_gold}/dim_fator_agregado') as fa ON ex.cod_fator_agregado = fa.cod_fator_agregado
        LEFT JOIN delta_scan('{path_minio_gold}/dim_pais') as p ON ex.cod_pais = p.cod_pais
        LEFT JOIN delta_scan('{path_minio_gold}/dim_uf') as uf ON ex.cod_estado = uf.cod_estado
        LEFT JOIN delta_scan('{path_minio_gold}/dim_tempo') dt ON dt.cod_date = (ex.ano || ex.mes || '1')
        WHERE 
        dt.cod_date > (SELECT MAX(cod_date) FROM delta_scan('{path_minio_gold}/fat_exportacoes'))
        
""").to_arrow_table()

if len(fat_exportacoes) > 0:
    write_deltalake(
            f'{path_minio_gold}/fat_exportacoes',
            fat_exportacoes,
            mode='append',
            storage_options=storage_options,
            partition_by=["ano","mes"]
            #max_rows_per_file=100000,        # Máximo de 300.000 linhas por arquivo
            #max_rows_per_group=100000,       # Máximo de 100.000 linhas por grupo
            #min_rows_per_group=10000 
    )

con.close()