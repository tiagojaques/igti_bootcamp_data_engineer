# Primeira DAG com Airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Argumentos Default

default_args = {
    'owner': 'Tiago Jaques - IGTI',
    'depends_on_past': False,
    'start_date': datetime(2020,11,27, 21),
    'email': ['tjaquespereira@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Definindo nossas DAGs
dag = DAG(
    'Treino-01',
    description='Basicos de Bash Operators e Python Operators',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

# Adicionando tarefas
hello_bash = BashOperator(
    task_id='Hello_Bash',
    bash_command='echo "Hello airflow from bash"',
    dag=dag
)

def say_hello():
    print('Hello airflow from Python')

hello_pyton = PythonOperator(
    task_id="Hello_Python",
    python_callable=say_hello,
    dag=dag
)

hello_bash >> hello_pyton