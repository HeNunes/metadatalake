import pandas as pd
from obspy import read
import boto3
from botocore.client import Config
import io
import couchdb
from MinioUtils import get_sac_from_minio

def query_1(db, minio_client):
    query = {
        "selector": {
            "station_state": "Alagoas"
        },
        "fields": ["ID", "unique_source_id", "connection_username"]
    }
    results = db.find(query)

    max_amplitude = 0
    file_result = None

    for result in results:
        obs_file = get_sac_from_minio(bucket_name="bucket-teste", object_name=result['unique_source_id'], 
                        station_name=(result['connection_username']).upper(), minio_client=minio_client)
        
        obs = read(obs_file)
        amplitude = obs[0].data.max() - obs[0].data.min()

        print(f"ID: {result['ID']} | AMPLITUDE: {amplitude}")

        if amplitude > max_amplitude:
            max_amplitude = amplitude
            file_result = result
    
    print(f"Result:\n ID: {file_result['ID']} Source:{file_result['unique_source_id']} Amplitude:{max_amplitude}")

    obs = get_sac_from_minio(bucket_name="bucket-teste", object_name=file_result['unique_source_id'], 
                        station_name=(file_result['connection_username']).upper(), minio_client=minio_client)

    print("Writing result in Minio")
    file = io.BytesIO()
    read(obs).write(file, format="SAC")
    file.seek(0) 
    minio_client.put_object(Bucket="bucket-teste", Key="result-1", Body=file, ContentLength=file.getbuffer().nbytes)

def query_2(db, minio_client):
    index_time = {
        "index": {
            "fields": ["date", "timestamp"]
        },
        "name": "date_timestamp_index",
        "type": "json"
    }
    db.resource.post('_index', index_time)

    query = {
        "selector": {
            "station_name": "NBAN"
        },
        "fields": ["ID", "unique_source_id", "connection_username"],
        "sort": [
            {"date": "desc"}, 
            {"timestamp": "desc"}
        ],
        "limit": 1
    }
    results = db.find(query)
    results = list(results)
    result = results[0]

    print(f"Unique source id: {result['unique_source_id']}")
    print(f"Provider: {(result['connection_username']).upper()}")

    print("Downloading from Minio")
    obs = get_sac_from_minio(bucket_name="bucket-teste", object_name=result['unique_source_id'], 
                        station_name=(result['connection_username']).upper(), minio_client=minio_client)

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

    couch = couchdb.Server('http://couchdb:couchdb123@localhost:5984')
        
    db = couch['metadatalake']
    print(db)

    query_2(db=db, minio_client=minio_client)