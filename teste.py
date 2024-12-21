import pandas as pd
import fastparquet
from obspy import read
import boto3
from botocore.client import Config
import io
from pyspark.sql import SparkSession
from MinioUtils import bucket_download, bucket_upload, get_sac_from_minio

def query_1(spark, minio_client):
    query = """
    SELECT DISTINCT dim_dataobject.unique_source_id, 
           dim_datarepository.connection_username
    FROM fact_seismology
    INNER JOIN dim_dataprovider
    ON fact_seismology.sk_dataprovider = dim_dataprovider.sk_dataprovider
    INNER JOIN dim_datarepository
    ON fact_seismology.sk_datarepository = dim_datarepository.sk_datarepository
    INNER JOIN dim_dataobject
    ON fact_seismology.sk_dataobject = dim_dataobject.sk_dataobject
    WHERE dim_dataprovider.station_state = 'Alagoas'
    """

    result_df = spark.sql(query)
    file_result = result_df.collect()

    # Itera sobre resultados
    # Calcula amplitude e compara com max amplitude
    # Armazena id de max_amplitude

    i = 0
    max_id = 0 
    max_amplitude = 0
    for file in file_result:
        obs_file = get_sac_from_minio(bucket_name="bucket-teste", object_name=file['unique_source_id'], 
                        station_name=(file['connection_username']).upper(), minio_client=minio_client)
        
        obs = read(obs_file)
        amplitude = obs[0].data.max() - obs[0].data.min()
        if amplitude > max_amplitude:
            max_amplitude = amplitude
            max_id = i
        i += 1
    
    print(max_id, max_amplitude)

    # Obtendo o arquivo correspondente
    obs = get_sac_from_minio(bucket_name="bucket-teste", object_name=file_result[max_id]['unique_source_id'], 
                        station_name=(file_result[max_id]['connection_username']).upper(), minio_client=minio_client)

    print("Writing result in Minio")
    file = io.BytesIO()
    read(obs).write(file, format="SAC")
    file.seek(0) 
    minio_client.put_object(Bucket="bucket-teste", Key="result-1", Body=file, ContentLength=file.getbuffer().nbytes)


    

def query_2(spark, minio_client):
    query = """
    SELECT dim_dataobject.unique_source_id,
           dim_datarepository.connection_username
    FROM fact_seismology
    INNER JOIN dim_dataprovider
    ON fact_seismology.sk_dataprovider = dim_dataprovider.sk_dataprovider
    INNER JOIN dim_datarepository
    ON fact_seismology.sk_datarepository = dim_datarepository.sk_datarepository
    INNER JOIN dim_dataobject
    ON fact_seismology.sk_dataobject = dim_dataobject.sk_dataobject
    INNER JOIN dim_date
    ON fact_seismology.sk_end_date_seism = dim_date.sk_date
    INNER JOIN dim_time
    ON fact_seismology.sk_end_time_seism = dim_time.sk_time
    WHERE dim_dataprovider.station_name = 'NBAN'
    ORDER BY dim_date.date DESC, dim_time.timestamp DESC
    LIMIT 1
    """

    result_df = spark.sql(query)
    file_result = result_df.collect()

    result_df.show()
    print(f"Unique source id: {file_result[0]['unique_source_id']}")
    print(f"Provider: {(file_result[0]['connection_username']).upper()}")

    print("Downloading from Minio")
    obs = get_sac_from_minio(bucket_name="bucket-teste", object_name=file_result[0]['unique_source_id'], 
                        station_name=(file_result[0]['connection_username']).upper(), minio_client=minio_client)

    print("Writing result in Minio")
    file = io.BytesIO()
    read(obs).write(file, format="SAC")
    file.seek(0) 
    minio_client.put_object(Bucket="bucket-teste", Key="result-2", Body=file, ContentLength=file.getbuffer().nbytes)

if __name__ == '__main__':
    
    minio_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='zR25x6sniwJTKnCRs9P2',
        aws_secret_access_key='BtsJ4MwXMjSXroaNnCgTOFa2LjrZNSBgaEwu3Zed',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    spark = SparkSession.builder.appName("Query test").getOrCreate()

    metadata_path = 'metadata'

    fact_seismology_df = spark.read.parquet(f"{metadata_path}/fact_seismology.parquet")
    fact_seismology_df.createOrReplaceTempView("fact_seismology")

    dim_dataprovider_df = spark.read.parquet(f"{metadata_path}/dim_dataprovider.parquet")
    dim_dataprovider_df.createOrReplaceTempView("dim_dataprovider")

    dim_datarepository_df = spark.read.parquet(f"{metadata_path}/dim_datarepository.parquet")
    dim_datarepository_df.createOrReplaceTempView("dim_datarepository")

    dim_dataobject_df = spark.read.parquet(f"{metadata_path}/dim_dataobject.parquet")
    dim_dataobject_df.createOrReplaceTempView("dim_dataobject")

    dim_date_df = spark.read.parquet(f"{metadata_path}/dim_date.parquet")
    dim_date_df.createOrReplaceTempView("dim_date")

    dim_time_df = spark.read.parquet(f"{metadata_path}/dim_time.parquet")
    dim_time_df.createOrReplaceTempView("dim_time")

    query_1(spark=spark, minio_client=minio_client)