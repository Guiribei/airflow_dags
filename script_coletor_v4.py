import pandas as pd
import requests
import os, sys

### PT ###

def coleta_noticias_pt():
    api_key = "7d0a6cd469e6add6ee8400e95d605c11"

    params = {
        "q": "mercado financeiro",
        "lang": "pt",
        "country": "br",
        "max": 10,
        "token": api_key
    }

    url = "https://gnews.io/api/v4/search"

    res = requests.get(url, params=params)

    if res.status_code == 200:
        news = res.json()["articles"]
        df = pd.DataFrame(news, columns=["title"])
        if os.path.isfile('~/airflow/noticias_pt.parquet'):
            old_df = pd.read_parquet('~/airflow/noticias_pt.parquet')
        else:
            old_df = pd.DataFrame()
        concat_df = pd.concat([old_df, df])
        concat_df.to_parquet('~/airflow/noticias_pt.parquet')

    else:
        print("Falha ao obter notícias.")
        sys.exit()

def analisa_sentimento_pt():
    meaning_key = 'bc61619c8b3e480b0729d1d6601a4e92'

    noticias_df = pd.read_parquet('~/airflow/noticias_pt.parquet')

    def get_sentimento(titulo):
        url = 'https://api.meaningcloud.com/sentiment-2.1'
        payload = {'key': meaning_key, 'lang': 'pt', 'txt': titulo}
        res = requests.post(url, data=payload)
        if res.status_code == 200:
            result = res.json()
            if 'score_tag' in result:
                return result['score_tag']
        return None

    noticias_df['sentimento'] = noticias_df['title'].apply(get_sentimento)
    if os.path.isfile('~/airflow/sentimento_pt.parquet'):
        old_df = pd.read_parquet('~/airflow/sentimento_pt.parquet')
    else:
        old_df = pd.DataFrame()
    concat_df = pd.concat([old_df, noticias_df])
    concat_df.to_parquet('~/airflow/sentimento_pt.parquet', engine='pyarrow')

### EN ###

def coleta_noticias_en():
    api_key = "7d0a6cd469e6add6ee8400e95d605c11"

    params = {
        "q": "financial market",
        "lang": "en",
        "country": "us",
        "max": 10,
        "token": api_key
    }

    url = "https://gnews.io/api/v4/search"

    res = requests.get(url, params=params)

    if res.status_code == 200:
        news = res.json()["articles"]
        df = pd.DataFrame(news, columns=["title"])
        if os.path.isfile('~/airflow/noticias_en.parquet'):
            old_df = pd.read_parquet('~/airflow/noticias_en.parquet')
        else:
            old_df = pd.DataFrame()
        concat_df = pd.concat([old_df, df])
        concat_df.to_parquet('~/airflow/noticias_en.parquet')

    else:
        print("Falha ao obter notícias.")
        sys.exit()

def analisa_sentimento_en():
    meaning_key = 'bc61619c8b3e480b0729d1d6601a4e92'

    noticias_df = pd.read_parquet('~/airflow/noticias_en.parquet')

    def get_sentimento(titulo):
        url = 'https://api.meaningcloud.com/sentiment-2.1'
        payload = {'key': meaning_key, 'lang': 'en', 'txt': titulo}
        res = requests.post(url, data=payload)
        if res.status_code == 200:
            result = res.json()
            if 'score_tag' in result:
                return result['score_tag']
        return None

    noticias_df['sentimento'] = noticias_df['title'].apply(get_sentimento)
    if os.path.isfile('~/airflow/sentimento_en.parquet'):
        old_df = pd.read_parquet('~/airflow/sentimento_en.parquet')
    else:
        old_df = pd.DataFrame()
    concat_df = pd.concat([old_df, noticias_df])
    concat_df.to_parquet('~/airflow/sentimento_en.parquet', engine='pyarrow')
