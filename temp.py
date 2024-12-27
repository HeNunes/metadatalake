import pandas as pd

parquet_df = pd.DataFrame(pd.read_parquet('metadatalake.parquet'))
print(parquet_df.columns)
parquet_df.to_csv('metadatalake.csv')
