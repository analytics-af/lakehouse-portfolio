from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),
    'retries': 1,
}

# Definir a DAG
with DAG(
    'execute_python_script',
    default_args=default_args,
    description='DAG para executar scripts Python com BashOperator',
    schedule_interval='@daily',
) as dag:

    # Task para rodar o script Python
    run_python_script = BashOperator(
        task_id='run_python_script',
        bash_command='python3 /usr/local/airflow/include/scripts_py/test/minio_duck.py',
    ),
    run_python_script_local_csv = BashOperator(
        task_id='run_python_script_local_csvs',
        bash_command='python3 /usr/local/airflow/include/scripts_py/test/minio_duck_csv_local.py')

    run_python_script >> run_python_script_local_csv
