from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from script_coletor_v4 import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'coleta_sentimento_noticias_module',
    default_args=default_args,
    description='Coleta e análise de sentimento de notícias sobre o mercado financeiro em português (Brasil) e em inglês (EUA)',
    schedule_interval=timedelta(minutes=5),
    catchup=False
)

with dag:
    
    ### PT ###
    
    coleta_noticias_pt = PythonOperator(
        task_id='coleta_noticias_pt',
        python_callable=coleta_noticias_pt
    )

    analisa_sentimento_pt = PythonOperator(
        task_id='analisa_sentimento_pt',
        python_callable=analisa_sentimento_pt
    )

    ### EN ###

    coleta_noticias_en = PythonOperator(
        task_id='coleta_noticias_en',
        python_callable=coleta_noticias_en
    )

    analisa_sentimento_en = PythonOperator(
        task_id='analisa_sentimento_en',
        python_callable=analisa_sentimento_en
    )

    coleta_noticias_pt >> analisa_sentimento_pt >> coleta_noticias_en >> analisa_sentimento_en

