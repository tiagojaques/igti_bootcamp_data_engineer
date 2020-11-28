# DAG com Airflow - Analise dados titanic

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random 

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
    'Treino-03',
    description='Extrai dados do Titanic e calcula a idade média para mulheres ou homens',
    default_args=default_args,
    schedule_interval='*/2 * * * *'
)

get_data = BashOperator (
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /usr/local/airflow/data/train.csv',
    dag=dag
)

def sorteia_m_h():
    return random.choice(['male', 'female'])

escolhe_m_h = PythonOperator(
    task_id='escolhe_m_h',
    python_callable=sorteia_m_h,
    dag=dag
)

def FouM(**conext):
    value = conext['task_instance'].xcom_pull(task_ids='escolhe_m_h')
    if value == 'male':
        return 'branch_homem'
    if value == 'female':
        return 'branch_mulher'

male_female = BranchPythonOperator(
    task_id='condicional',
    python_callable=FouM,
    provide_context=True,
    dag=dag
)

def mean_homem():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df = df.loc[df.Sex=='male']
    print(f"Média de idade dos homens no Titanic: {df.Age.mean()}")

branch_homen = PythonOperator(
    task_id="branch_homem",
    python_callable=mean_homem,
    dag=dag
)

def mean_mulher():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df = df.loc[df.Sex=='female']
    print(f"Média de idade das mulheres no Titanic: {df.Age.mean()}")

branch_mulher = PythonOperator(
    task_id="branch_mulher",
    python_callable=mean_mulher,
    dag=dag
)

get_data >> escolhe_m_h >> male_female >> [branch_homen, branch_mulher]
