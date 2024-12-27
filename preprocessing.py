import pandas as pd

data_object_path     = 'metadata/dim_dataobject.parquet'
data_provider_path   = 'metadata/dim_dataprovider.parquet'
data_repository_path = 'metadata/dim_datarepository.parquet'
data_date_path       = 'metadata/dim_date.parquet'
data_time_path       = 'metadata/dim_time.parquet'
fact_seismology_path = 'metadata/fact_seismology.parquet'

object_df     = pd.read_parquet(data_object_path)
provider_df   = pd.read_parquet(data_provider_path)
repository_df = pd.read_parquet(data_repository_path)
date_df       = pd.read_parquet(data_date_path)
time_df       = pd.read_parquet(data_time_path)
fact_df       = pd.read_parquet(fact_seismology_path)

merged = pd.merge(fact_df, provider_df,   on='sk_dataprovider',   how='inner')
merged = pd.merge(merged,  object_df,     on='sk_dataobject',     how='inner')
merged = pd.merge(merged,  repository_df, on='sk_datarepository', how='inner')

merged.rename(columns={'sk_end_date_seism' : 'sk_date'}, inplace=True)
merged.rename(columns={'sk_end_time_seism' : 'sk_time'}, inplace=True)

merged = pd.merge(merged, date_df, on='sk_date', how='inner')
merged = pd.merge(merged, time_df, on='sk_time', how='inner')

print(merged.columns)
print(merged)

merged.index.name = 'ID'
merged.to_csv('metadata.csv', index=True)