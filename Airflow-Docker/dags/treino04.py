# DAG com Airflow - Analise dados titanic

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile
import os.path

# Argumentos Default

default_args = {
    'owner': 'Tiago Jaques - IGTI',
    'depends_on_past': False,
    'start_date': datetime(2020,11,28, 6),
    'email': ['tjaquespereira@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Constantes
data_path = '/usr/local/airflow/data/microdados_enade_2019/2019/3.DADOS/'
arquivo = data_path + 'microdados_enade_2019.txt'

# Definindo nossas DAGs
dag = DAG(
    'Treino-04',
    description='ENADE 2019 - Paralelismo',
    default_args=default_args,
    schedule_interval='*/10 * * * *'
)

start_preprocessing = BashOperator(
    task_id='start_preprocessing',
    bash_command='echo Começando o JOB !!! VAI !!!',
    dag=dag
)

cmd_get_data = '[ ! -f /usr/local/airflow/data/microdados_enade_2019.zip ] && curl "http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip" -o "/usr/local/airflow/data/microdados_enade_2019.zip"'
get_data = BashOperator (
    task_id='get-data',
    bash_command=cmd_get_data,
    dag=dag
)

def unzip_file():
    if not os.path.exists(arquivo):
        with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
            zipped.extractall('/usr/local/airflow/data/')

unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_file,
    dag=dag
)

# Lendo os dados e aplicando os filtros
def aplica_filtros():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    enade = pd.read_csv(arquivo, sep=';', decimal=',', usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]

    enade.to_csv(data_path + 'enade_filtrado.csv', index=False)
    
task_aplica_filtros = PythonOperator(
    task_id='aplica_filtros',
    python_callable=aplica_filtros,
    dag=dag
)

# Idade centralizada na média
def constroi_idade_centralizada():
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade['NU_IDADE'] - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + 'idadecent.csv', index=False)

task_constroi_idade_centralizada = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag
)

# Idade centralizada ao quadrado
def constroi_idade_cent_quad():
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadecent['idade2'] = idadecent['NU_IDADE'] - idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)

task_constroi_idade_cent_quad = PythonOperator(
    task_id='constroi_idade_cent_quad',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)

def constroi_est_civil():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)

task_constroi_est_civil = PythonOperator(
    task_id='constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
)

def constroi_cor():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': '',
        ' ': ''
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)

task_constroi_cor = PythonOperator(
    task_id='constroi_cor',
    python_callable=constroi_cor,
    dag=dag
)

def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadequadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')

    final = pd.concat([
        filtro, idadecent, idadequadrado, estcivil, cor
    ],
    axis=1
    )

    final.to_csv(data_path + 'enade_tratado.csv', index=False)
    print(final)

task_join_data = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
)
start_preprocessing >> get_data >> unzip_data >> task_aplica_filtros
task_aplica_filtros >> [task_constroi_idade_centralizada, task_constroi_est_civil, task_constroi_cor]

task_constroi_idade_cent_quad.set_upstream(task_constroi_idade_centralizada)

task_join_data.set_upstream([
    task_constroi_est_civil, task_constroi_cor, task_constroi_idade_cent_quad
])