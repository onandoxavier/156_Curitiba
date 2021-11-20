import pandas as pd
import requests
from datetime import date
from bs4 import BeautifulSoup
import os
from os import path

# Trecho para extrair o encoding correto de um arquivo
# with open('raw_data/2017-08-01_156_-_Base_de_Dados.csv') as f:
#    print(f)


if path.exists('pipeline_historic.csv'):
    print('Historico do pipiline ja existe, carregando...')
    pipeline_historic = pd.read_csv('pipeline_historic.csv', sep=';')
else:
    print('Criando novo arquivo de historico do pipeline')
    columns = ["link", "download_status", "download_date", "pre_processing_status", "pre_processing_date"]
    pipeline_historic = pd.DataFrame(columns=columns)

    if not path.exists('raw_data'):
        os.mkdir('raw_data')

bases_downloaded = pipeline_historic.loc[pipeline_historic['download_status'] == 'Completed']['link'].tolist()

url = 'http://dadosabertos.c3sl.ufpr.br/curitiba/156/'
try:
    req = requests.get(url)
    if req.status_code == 200:
        print('Requisição de listagem de bases bem sucedida!')
        content = req.content
except:
    exit()

soup = BeautifulSoup(content, 'html.parser')
links = soup.find_all("a")
new_links = []

for elem in links:
    elem = f'{elem.get("href")}'
    if elem.find('_156_-_Base_de_Dados.csv') > 0 and elem not in bases_downloaded and len(elem) < 35:
        new_links.append(elem)

#new_links = new_links[8:11]

print(f'{len(new_links)} novas bases estao disponiveis para download.')
print('Iniciando atualização da base')
#dadosAbertos_156
for uri in new_links:
    try:
        newurl = url+uri
        print('Nova URL:')
        print(newurl)
        df_raw = pd.read_csv(newurl, sep='delimiter', encoding="cp1252", engine='python', header=None)
        df = df_raw[0].to_frame()
        df.columns = df.iloc[0]
        df = df[1:]

        print('Download de nova base concluido')
        df.to_csv("raw_data/" + uri)
        print(f'{uri} - Foi salva no sistema')
        row = pipeline_historic.loc[pipeline_historic['link'] == uri]

        if row.empty:
            print('Insere nova linha ao historico')
            pipeline_historic = pipeline_historic.append({
                'link': uri,
                'download_status': 'Completed',
                'download_date': date.today(),
                'is_pre_processed': 0
            }, ignore_index=True)
        else:
            print(f'Atualiza linha {row.index} existente do historico')
            pipeline_historic.loc[row.index, 'download_status'] = 'Completed'
    except Exception as ex:
        print(ex)
        pipeline_historic = pipeline_historic.append({
            'link': uri,
            'download_status': ex,
            'download_date': date.today(),
            'is_pre_processed': 0
        }, ignore_index=True)

pipeline_historic.to_csv("pipeline_historic.csv", sep=';', index=False)
print('Historico de atualização salvo.')
print('Script finalizado.')
