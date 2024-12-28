import pandas as pd

# Carregar o arquivo CSV no DataFrame
df = pd.read_csv('metadatalake_backup.csv')

# Processar a coluna 'filename' para atualizar os valores
df['filename'] = df['filename'].apply(lambda x: x.rstrip("/").split("/")[-1].replace(":", "-"))

# Salvar o DataFrame modificado de volta em um arquivo CSV
df.to_csv('metadatalake.csv', index=False)
