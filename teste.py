import pandas as pd
import fastparquet
import obspy as obs

parquet_path = 'metadata/dim_dataobject.parquet'
sac_path = 'NBAN/NB.NBAN..HHE.D.2022,1,0-0-0.SAC'

df = fastparquet.ParquetFile(parquet_path).to_pandas()

stream = obs.read(sac_path)

print(stream[0].stats)
stream.plot()