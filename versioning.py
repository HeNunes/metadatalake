import pandas as pd

def generate_metadata():
    df = pd.read_csv('metadatalake.csv')
    df['endtime'] = pd.to_datetime(df['endtime'], errors='coerce')
    df['starttime'] = pd.to_datetime(df['starttime'], errors='coerce')

    for y in range(1, 3):  

        df_adjusted = df.copy()
        df_adjusted['endtime'] = df_adjusted['endtime'].apply(lambda x: x.replace(year=x.year - y) if pd.notnull(x) else x)
        df_adjusted['starttime'] = df_adjusted['starttime'].apply(lambda x: x.replace(year=x.year - y) if pd.notnull(x) else x)

        df_adjusted.to_csv(f'metadata_versioning/metadatalake-{2022 - y}.csv', index=False)

def get_metadata():
    

if __name__ == '__main__':
    generate_metadata()