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
path_minio_silver = 's3://silver/comex/exportacoes'
path_minio_bronze = 's3://bronze/comex'

storage_options = {
    "AWS_ENDPOINT_URL": f"http://{HOST_MINIO}:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true"
}

df = con.sql(f"""
        SELECT * FROM delta_scan('{path_minio_silver}')
        """).to_arrow_table()

# ano máximo do bronze
max_ano = con.sql("SELECT max(ano) FROM df").to_df().iloc[0, 0]
# mês máximo correspondente ao ano máximo do bronze
max_mes = con.sql(f"SELECT max(mes) FROM df WHERE ano = {max_ano}").to_df().iloc[0, 0]


#Roda Incremental
df_incremental = con.sql(f"""
        WITH  exportacoes AS 
        (
        SELECT 
                ano,
                mes,
                cod_NCM,
                cod_unidade,
                cod_pais,
                sigla_estado,
                cod_via,
                cod_urf,
                qtd,
                kg_liquido,
                valor_fob
        FROM delta_scan('{path_minio_bronze}/exportacoes')
        ),
        ncm as
        (
        SELECT 
             cod_ncm,
             cod_sh6,
             cod_fat_agreg,
             no_ncm_por
        FROM delta_scan('{path_minio_bronze}/ncm')
        ),
        ncm_sh as
        (
        SELECT 
             cod_sh6,
             nome_sh6,
             nome_sh4,
             nome_sh2,
             nome_secao
        FROM delta_scan('{path_minio_bronze}/ncm_sh')
        ),
        ncm_fat_agreg as
        (
        SELECT 
             cod_fator_agregado,
             nome_fator_agregado,
             grupo_fator_agregado
        FROM delta_scan('{path_minio_bronze}/ncm_fat_agreg')
        ),
        pais as
        (
        SELECT 
             p.cod_pais,
             p.cod_pais_iso,
             p.nome_pais,
             pb.nome_bloco
        FROM delta_scan('{path_minio_bronze}/pais') p
        left join delta_scan('{path_minio_bronze}/pais_bloco') as pb on p.cod_pais = pb.cod_pais
        ),
        uf as
        (
        SELECT 
             cod_estado,
             sigla_estado,
             nome_estado,
             nome_regiao
        FROM delta_scan('{path_minio_bronze}/uf') 
        )
        -- consilta que gera a silver

        SELECT 
               exp.ano,
               exp.mes,
               ncm.cod_ncm,
               ncm.no_ncm_por,
               ncm_sh.cod_sh6,
               ncm_sh.nome_sh6,
               ncm_sh.nome_sh4,
               ncm_sh.nome_sh2,
               ncm_sh.nome_secao,
               nfa.cod_fator_agregado,
               nfa.nome_fator_agregado,
               nfa.grupo_fator_agregado,
               uf.cod_estado,
               uf.sigla_estado,
               uf.nome_estado,
               uf.nome_regiao,
               pais.cod_pais,
               pais.nome_pais,
               pais.nome_bloco,
               exp.qtd,
               exp.kg_liquido,
               exp.valor_fob
        FROM exportacoes exp      
        LEFT JOIN ncm ON exp.cod_NCM = ncm.cod_ncm
        LEFT JOIN ncm_sh ON ncm.cod_sh6 = ncm_sh.cod_sh6
        LEFT JOIN ncm_fat_agreg as nfa ON nfa.cod_fator_agregado = ncm.cod_fat_agreg
        LEFT JOIN pais ON pais.cod_pais = exp.cod_pais
        LEFT JOIN uf ON uf.sigla_estado = exp.sigla_estado
        WHERE exp.ano > {max_ano} OR
        (exp.ano = {max_ano}
        AND exp.mes > {max_mes})
""").to_arrow_table()

if len(df_incremental) > 0:
     write_deltalake(
          f'{path_minio_silver}',
          df_incremental,
          mode='append',
          storage_options=storage_options,
          partition_by=["ano","mes"]
     )

con.close()