from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Configuração dos argumentos padrões
default_args = {
    'owner': 'airflow',              # Indica o dono da DAG.
    'depends_on_past': False,        # Se as execuções futuras devem esperar que a execução anterior complete com sucesso.
    'email_on_failure': False,       # Se deve enviar email quando a tarefa falhar.
    'email_on_retry': False,         # Se deve enviar email quando houver retry.
    'retries': 1,                    # Número de vezes que a tarefa irá tentar em caso de falha.
    'retry_delay': timedelta(minutes=5),  # Tempo de espera entre tentativas.
}

def print_hello():
    print("Hello World!")

# Definindo a DAG
dag = DAG(
    'hello_world_dag',               # Nome da DAG
    default_args=default_args,        # Argumentos padrões que configuramos anteriormente.
    description='Minha primeira DAG no Airflow',  # Descrição da DAG
    schedule_interval=timedelta(days=1),  # Frequência de execução da DAG (a cada 1 dia neste exemplo).
    start_date=days_ago(1),           # Data de início da DAG.
    catchup=False,                    # Evita que execuções "perdidas" sejam realizadas.
)

#Exemplos de Schedules:
schedule_interval_cron='0 14 * * *',  # Todos os dias às 14h
schedule_interval_timedelta=timedelta(hours=1),  # A cada 1 hora
schedule_interval_cron='0 * * * *',  # A cada hora
schedule_interval_timedelta=timedelta(minutes=15),  # A cada 15 minutos
schedule_interval_cron='*/15 * * * *',  # A cada 15 minutos

# Dummy task (apenas uma tarefa de exemplo que não faz nada)
dummytask = DummyOperator(task_id='dummytask', dag=dag)

# Tarefa que imprime Hello World
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag
)

#ordem das tarefas
dummytask >> hello_task