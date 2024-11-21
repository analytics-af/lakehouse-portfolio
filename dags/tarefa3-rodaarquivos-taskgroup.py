from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from datetime import datetime

# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),
}

# Função auxiliar para criar BashOperators
def create_bash_operator(task_id, script_path):
    return BashOperator(
        task_id=task_id,
        bash_command=f'python3 /usr/local/airflow/include/{script_path}'
    )

# Definir a DAG
with DAG(
    'execute_python_lakehouse_taskgroup',
    default_args=default_args,
    description='DAG para executar scripts Python com BashOperator',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Grupo Landing
    with TaskGroup("landing") as landing:
        roda_landing = create_bash_operator(
            task_id='run_python_script_landing',
            script_path='prod/1_landing/origem_to_landing.py'
        )

    # Grupo Bronze
    with TaskGroup("bronze") as bronze:
        roda_bronze_exportacao = create_bash_operator(
            task_id='run_python_script_bronze_exportacao',
            script_path='prod/2_bronze/exportacoes.py'
        )
        roda_bronze_importacao = create_bash_operator(
            task_id='run_python_script_bronze_importacao',
            script_path='prod/2_bronze/importacoes.py'
        )
        roda_bronze_ncm_fat_agreg = create_bash_operator(
            task_id='run_python_script_bronze_ncm_fat_agreg',
            script_path='prod/2_bronze/ncm_fat_agreg.py'
        )
        roda_bronze_ncm_sh = create_bash_operator(
            task_id='run_python_script_bronze_ncm_sh',
            script_path='prod/2_bronze/ncm_sh.py'
        )
        roda_bronze_ncm = create_bash_operator(
            task_id='run_python_script_bronze_ncm',
            script_path='prod/2_bronze/ncm.py'
        )
        roda_bronze_pais_bloco = create_bash_operator(
            task_id='run_python_script_bronze_pais_bloco',
            script_path='prod/2_bronze/pais_bloco.py'
        )
        roda_bronze_pais = create_bash_operator(
            task_id='run_python_script_bronze_pais',
            script_path='prod/2_bronze/pais.py'
        )
        roda_bronze_uf_mun = create_bash_operator(
            task_id='run_python_script_bronze_uf_mun',
            script_path='prod/2_bronze/uf_mun.py'
        )
        roda_bronze_uf = create_bash_operator(
            task_id='run_python_script_bronze_uf',
            script_path='prod/2_bronze/uf.py'
        )

    # Grupo Silver
    with TaskGroup("silver") as silver:
        roda_silver_exportacoes = create_bash_operator(
            task_id='run_python_script_silver_exportacoes',
            script_path='prod/3_silver/exportacoes.py'
        )
        roda_silver_importacoes = create_bash_operator(
            task_id='run_python_script_silver_importacoes',
            script_path='prod/3_silver/importacoes.py'
        )

    # Grupo Gold - Dimensões
    with TaskGroup("gold_dimensoes") as gold_dimensoes:
        roda_gold_dim_fator_agreg = create_bash_operator(
            task_id='run_python_script_gold_dim_fator_agreg',
            script_path='prod/4_gold/dim_fator_agreg.py'
        )
        roda_gold_dim_ncm = create_bash_operator(
            task_id='run_python_script_gold_dim_ncm',
            script_path='prod/4_gold/dim_ncm.py'
        )
        roda_gold_dim_pais = create_bash_operator(
            task_id='run_python_script_gold_dim_pais',
            script_path='prod/4_gold/dim_pais.py'
        )
        roda_gold_dim_tempo = create_bash_operator(
            task_id='run_python_script_gold_dim_tempo',
            script_path='prod/4_gold/dim_tempo.py'
        )
        roda_gold_dim_uf = create_bash_operator(
            task_id='run_python_script_gold_dim_uf',
            script_path='prod/4_gold/dim_uf.py'
        )

    # Grupo Gold - Fatos
    with TaskGroup("gold_fatos") as gold_fatos:
        roda_gold_fat_exportacoes = create_bash_operator(
            task_id='run_python_script_gold_fat_exportacoes',
            script_path='prod/4_gold/fat_exportacoes.py'
        )
        roda_gold_fat_importacoes = create_bash_operator(
            task_id='run_python_script_gold_fat_importacoes',
            script_path='prod/4_gold/fat_importacoes.py'
        )

    landing >> bronze >> silver >> gold_dimensoes >> gold_fatos
