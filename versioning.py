import pandas as pd
import requests
import os
from dotenv import load_dotenv

def generate_metadata():
    df = pd.read_csv('metadatalake.csv')
    df['endtime'] = pd.to_datetime(df['endtime'], errors='coerce')
    df['starttime'] = pd.to_datetime(df['starttime'], errors='coerce')

    for y in range(1, 3):  

        df_adjusted = df.copy()
        df_adjusted['endtime'] = df_adjusted['endtime'].apply(lambda x: x.replace(year=x.year - y) if pd.notnull(x) else x)
        df_adjusted['starttime'] = df_adjusted['starttime'].apply(lambda x: x.replace(year=x.year - y) if pd.notnull(x) else x)

        df_adjusted.to_csv(f'metadata_versioning/metadatalake-{2022 - y}.csv', index=False)

def download_files_from_github(repo_owner, repo_name, folder_path, token, branch="main"):
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{folder_path}?ref={branch}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 404:
            print(f"Erro 404: O caminho '{folder_path}' não foi encontrado no repositório.")
            return {}
        elif response.status_code != 200:
            print(f"Erro ao acessar a API do GitHub: {response.json().get('message')}")
            return {}

        files = response.json()
        if not isinstance(files, list):
            print(f"Erro: O conteúdo da pasta '{folder_path}' não está acessível ou é vazio.")
            return {}

        file_contents = {}

        for file in files:
            if file['type'] == 'file':
                file_url = file['download_url']
                file_name = file['name']
                print(f"Baixando {file_name}...")

                file_response = requests.get(file_url, headers=headers)
                if file_response.status_code == 200:
                    file_contents[file_name] = file_response.content.decode('utf-8')
                else:
                    print(f"Erro ao baixar {file_name}: {file_response.status_code} - {file_response.reason}")

        print("Arquivos baixados com sucesso e armazenados em memória.")
        return file_contents

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return {}

if __name__ == '__main__':
    load_dotenv()

    download_files_from_github(repo_owner=os.getenv('REPO_OWNER'), repo_name=os.getenv('REPO_NAME'), 
                               folder_path=os.getenv('FOLDER_PATH'), token=os.getenv('GIT_TOKEN'))