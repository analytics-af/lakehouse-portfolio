from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models.baseoperator import cross_downstream


# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),
}

# Definir a DAG
with DAG(
    'execute_python_lakehouse',
    default_args=default_args,
    description='DAG para executar scripts Python com BashOperator',
    schedule_interval='@daily',
    catchup=False,
) as dag:

 # Task para rodar o script Python da landing
    roda_landing = BashOperator(
        task_id='run_python_script_landing',
        bash_command='python3 /usr/local/airflow/include/prod/1_landing/origem_to_landing.py'
    )

    # Tasks para rodar os scripts Python da bronze
    roda_bronze_exportacao = BashOperator(
        task_id='run_python_script_bronze_exportacao',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/exportacoes.py',
    )

    roda_bronze_importacao = BashOperator(
        task_id='run_python_script_bronze_importacao',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/importacoes.py',
    )

    roda_bronze_ncm_fat_agreg = BashOperator(
        task_id='run_python_script_bronze_ncm_fat_agreg',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/ncm_fat_agreg.py',
    )

    roda_bronze_ncm_sh = BashOperator(
        task_id='run_python_script_bronze_ncm_sh',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/ncm_sh.py',
    )

    roda_bronze_ncm = BashOperator(
        task_id='run_python_script_bronze_ncm',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/ncm.py',
    )

    roda_bronze_pais_bloco = BashOperator(
        task_id='run_python_script_bronze_pais_bloco',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/pais_bloco.py',
    )

    roda_bronze_pais = BashOperator(
        task_id='run_python_script_bronze_pais',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/pais.py',
    )

    roda_bronze_uf_mun = BashOperator(
        task_id='run_python_script_bronze_uf_mun',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/uf_mun.py',
    )

    roda_bronze_uf = BashOperator(
        task_id='run_python_script_bronze_uf',
        bash_command='python3 /usr/local/airflow/include/prod/2_bronze/uf.py',
    )

    # Tasks para rodar os scripts Python da silver
    roda_silver_exportacoes = BashOperator(
        task_id='run_python_script_silver_exportacoes',
        bash_command='python3 /usr/local/airflow/include/prod/3_silver/exportacoes.py',
    )

    roda_silver_importacoes = BashOperator(
        task_id='run_python_script_silver_importacoes',
        bash_command='python3 /usr/local/airflow/include/prod/3_silver/importacoes.py',
    )

    # Tasks para rodar os scripts Python da gold
    roda_gold_dim_fator_agreg = BashOperator(
        task_id='run_python_script_gold_dim_fator_agreg',
        bash_command='python3 /usr/local/airflow/include/prod/4_gold/dim_fator_agreg.py',
    )

    roda_gold_dim_ncm = BashOperator(
        task_id='run_python_script_gold_dim_ncm',
        bash_command='python3 /usr/local/airflow/include/prod/4_gold/dim_ncm.py',
    )

    roda_gold_dim_pais = BashOperator(
        task_id='run_python_script_gold_dim_pais',
        bash_command='python3 /usr/local/airflow/include/prod/4_gold/dim_pais.py',
    )

    roda_gold_dim_tempo = BashOperator(
        task_id='run_python_script_gold_dim_tempo',
        bash_command='python3 /usr/local/airflow/include/prod/4_gold/dim_tempo.py',
    )

    roda_gold_dim_uf = BashOperator(
        task_id='run_python_script_gold_dim_uf',
        bash_command='python3 /usr/local/airflow/include/prod/4_gold/dim_uf.py',
    )

    roda_gold_fat_exportacoes = BashOperator(
        task_id='run_python_script_gold_fat_exportacoes',
        bash_command='python3 /usr/local/airflow/include/prod/4_gold/fat_exportacoes.py',
    )

    roda_gold_fat_importacoes = BashOperator(
        task_id='run_python_script_gold_fat_importacoes',
        bash_command='python3 /usr/local/airflow/include/prod/4_gold/fat_importacoes.py',
    )


    cross_downstream(
        [
            roda_landing
        ],
        [
            roda_bronze_exportacao,
            roda_bronze_importacao,
            roda_bronze_ncm_fat_agreg,
            roda_bronze_ncm_sh,
            roda_bronze_ncm,
            roda_bronze_pais_bloco,
            roda_bronze_pais,
            roda_bronze_uf_mun,
            roda_bronze_uf
        ]
    )

    cross_downstream(
        [
            roda_bronze_exportacao,
            roda_bronze_importacao,
            roda_bronze_ncm_fat_agreg,
            roda_bronze_ncm_sh,
            roda_bronze_ncm,
            roda_bronze_pais_bloco,
            roda_bronze_pais,
            roda_bronze_uf_mun,
            roda_bronze_uf
        ],
        [
            roda_silver_exportacoes,
            roda_silver_importacoes
        ]
    )

    cross_downstream(
        [
            roda_silver_exportacoes,
            roda_silver_importacoes
        ],
        [
            roda_gold_dim_fator_agreg,
            roda_gold_dim_ncm,
            roda_gold_dim_pais,
            roda_gold_dim_tempo,
            roda_gold_dim_uf
        ]
    )

    cross_downstream(
        [
            roda_gold_dim_fator_agreg,
            roda_gold_dim_ncm,
            roda_gold_dim_pais,
            roda_gold_dim_tempo,
            roda_gold_dim_uf
        ],
        [
            roda_gold_fat_exportacoes,
            roda_gold_fat_importacoes
        ]
    )
