import pandas as pd
from obspy import read
import boto3
from botocore.client import Config
import io
import couchdb
from MinioUtils import get_sac_from_minio

def query_1(couch, minio_client):
    metadata = couch['metadatalake']
    provider = couch['data_provider']

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
        "limit": 105
    }

    metadata_results = list(metadata.find(metadata_query))
    metadata_results = {(doc['filename'], doc['station']) for doc in metadata_results}

    provider_results = list(provider.find(provider_query))
    provider_results = {doc['Estação'] for doc in provider_results}

    metadata_results = [doc for doc in metadata_results if doc[1] in provider_results]

    max_amplitude = 0
    file_result = None
    for result in metadata_results:

        obs_file = get_sac_from_minio(bucket_name="bucket-teste", object_name=result[0], 
                        station_name=(result[1]), minio_client=minio_client)
        
        obs = read(obs_file)
        amplitude = obs[0].data.max() - obs[0].data.min()

        if amplitude > max_amplitude:
            max_amplitude = amplitude
            file_result = result
    
    print(f"Result:\nSource: {file_result[0]} Amplitude: {max_amplitude}")

    obs = get_sac_from_minio(bucket_name="bucket-teste", object_name=file_result[0], 
                        station_name=(file_result[1]).upper(), minio_client=minio_client)

    print("Writing result in Minio")
    file = io.BytesIO()
    read(obs).write(file, format="SAC")
    file.seek(0) 
    minio_client.put_object(Bucket="bucket-teste", Key="result-1-new", Body=file, ContentLength=file.getbuffer().nbytes)

def query_2(couch, minio_client):
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
    result = list(results)[0]

    print("Downloading from Minio")
    obs = get_sac_from_minio(bucket_name="bucket-teste", object_name=result['filename'], 
                        station_name=(result['station']).upper(), minio_client=minio_client)

    print("Writing result in Minio")
    file = io.BytesIO()
    read(obs).write(file, format="SAC")
    file.seek(0) 
    minio_client.put_object(Bucket="bucket-teste", Key="result-2-new", Body=file, ContentLength=file.getbuffer().nbytes)

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

    query_2(couch=couch, minio_client=minio_client)