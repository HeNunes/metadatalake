import pandas as pd
import couchdb
import os
from io import StringIO
from dotenv import load_dotenv
from versioning import download_files_from_github
import requests
import base64
import json

def create_db(couch):
    # db = couch.create('metadatalake')  # Ativar se necessário criar o DB
    db = couch['metadatalake']

    # URL base da API do GitHub
    api_url = f"https://api.github.com/repos/{os.getenv('REPO_OWNER')}/{os.getenv('REPO_NAME')}/contents/{os.getenv('FOLDER_PATH')}"
    headers = {
        "Authorization": f"token {os.getenv('GIT_TOKEN')}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Carregar e preparar o CSV
    df = pd.DataFrame(pd.read_csv('metadatalake.csv'))
    df.fillna("null", inplace=True)
    df_dict = [row.to_dict() for i, row in df.iterrows()]

    commit_hashes = []
    for doc in df_dict:
        # Remover ID antes de salvar
        doc.pop('ID', None)

        # Nome do arquivo no GitHub
        file_name = f"{doc['filename']}.json"  # Adicione extensão se necessário
        file_path = f"{api_url}/{file_name}"  # Construir o caminho do arquivo

        print(f"File path: {file_path}")

        # Converter o dicionário para JSON e codificar em Base64
        content = base64.b64encode(json.dumps(doc).encode('utf-8')).decode('utf-8')

        # Verificar se o arquivo já existe para obter o SHA
        response = requests.get(file_path, headers=headers)
        if response.status_code == 200:
            sha = response.json().get('sha')
            print(f"Arquivo '{file_name}' já existe. SHA encontrado: {sha}")
        elif response.status_code == 404:
            sha = None  # Arquivo não existe
            print(f"Arquivo '{file_name}' não encontrado. Criando novo.")
        else:
            print(f"Erro ao verificar existência do arquivo '{file_name}': {response.status_code}, {response.json()}")
            continue

        # Payload para criar ou atualizar o arquivo
        data = {
            "message": f"Add/Update {file_name}",
            "content": content,
            "branch": "main",
        }
        if sha:
            data['sha'] = sha

        # Fazer a solicitação PUT
        response = requests.put(file_path, headers=headers, json=data)

        if response.status_code in (200, 201):
            commit_hash = response.json().get('commit', {}).get('sha')
            commit_hashes.append(commit_hash)
            print(f"Arquivo '{file_name}' enviado com sucesso. Commit: {commit_hash}")
        else:
            print(f"Erro ao enviar '{file_name}': {response.status_code}, {response.json()}")

        # Salvar no CouchDB (se necessário)
        # db.save(doc=doc)

    print("Hashes dos commits realizados:", commit_hashes)
    print("DB criada com sucesso!")

def get_results(db, query):
    results = db.find(query)
    for result in results:
        print(result)

def source_only_1(couch):

   # downloaded_files = download_files_from_github(repo_owner=os.getenv('REPO_OWNER'), repo_name=os.getenv('REPO_NAME'), 
   #                            folder_path=os.getenv('FOLDER_PATH'), token=os.getenv('GIT_TOKEN'))
    
    metadata = couch['metadatalake']
    provider = couch['data_provider']

    print(metadata, provider)

   # added_ids = []
   # for file_name, file_content in downloaded_files.items():
   #     df = pd.read_csv(StringIO(file_content))
   #     df.fillna("null", inplace=True)
   #     df_dict = [row.to_dict() for i, row in df.iterrows()]
   #     for doc in df_dict:
   #         metadata.save(doc=doc)
   #         added_ids.append(doc['ID'])
    
    metadata_query = {
        "selector": {
            "ID": {"$gte": 0}
        },
        "fields": ["filename", "station"],
        "limit": 13900
    }
    provider_query = {
        "selector": {
            "Estado": "AL"
        },
        "fields": ["Estação"],
        "limit": 1
    }

    metadata_results = list(metadata.find(metadata_query))
    metadata_results = {(doc['filename'], doc['station']) for doc in metadata_results}

    provider_results = list(provider.find(provider_query))
    provider_results = {doc['Estação'] for doc in provider_results}

    metadata_results = [doc for doc in metadata_results if doc[1] in provider_results]

    i = 0
    for result in metadata_results:
        i += 1
    print(f"{i} resultados retornados")

        
def source_only_2(couch):
    metadata = couch['metadatalake']

    index_time = {
        "index": {
            "fields": ["endtime"]
        },
        "name": "endtime_index",
        "type": "json"
    }
    metadata.resource.post('_index', index_time)

    query = {
        "selector": {
            "station": "NBAN"
        },
        "fields": ["station", "filename", "endtime"],
        "sort": [{"endtime" : "desc"}],
        "limit": 1
    }
    results = metadata.find(query)

    for res in results:
        print(res)

if __name__ == '__main__':
    load_dotenv()
    couch = couchdb.Server('http://couchdb:couchdb123@localhost:5984')
        
    create_db(couch=couch)

    #source_only_1(couch=couch)