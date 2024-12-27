import pandas as pd

metadatalake = pd.DataFrame(pd.read_parquet('metadatalake.parquet'))

metadatalake.fillna("null", inplace=True)  # Substituindo NaN por "null"

metadatalake.to_csv('metadatalake.csv')