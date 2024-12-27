from io import BytesIO
import boto3
from botocore.client import Config

# Access key: zR25x6sniwJTKnCRs9P2
# Secret key: BtsJ4MwXMjSXroaNnCgTOFa2LjrZNSBgaEwu3Zed

minio_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='zR25x6sniwJTKnCRs9P2',
    aws_secret_access_key='BtsJ4MwXMjSXroaNnCgTOFa2LjrZNSBgaEwu3Zed',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

def bucket_get(bucket_name):
    response = minio_client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
        print(f"Encontrado: {obj['Key']}")

def bucket_upload(file_path, bucket_name, object_name):
    minio_client.upload_file(file_path, bucket_name, object_name)
    print(f"Arquivo {file_path} enviado para o bucket {bucket_name} como {object_name}.")

def bucket_download(bucket_name, object_name, station_name):
    object_path = f"{station_name}/{object_name}"
    download_path = f"dest/{object_name}"

    minio_client.download_file(bucket_name, object_path, download_path)
    print(f"Arquivo {object_name} baixado para {download_path}.")

def get_sac_from_minio(bucket_name, object_name, station_name, minio_client):
    object_path = f"{station_name}/{object_name}"
    
    response = minio_client.get_object(Bucket=bucket_name, Key=object_path)
    sac_file = BytesIO(response['Body'].read())
    return sac_file

if __name__ == '__main__':
    bucket_name = 'bucket-teste'
    parquet_path = 'metadata/dim_dataobject.parquet'
    station_name = 'NBAN'
    sac_path = 'NB.NBAN..HHE.D.2022,1,8-20-29.SAC'
    file_path = f"{station_name}/{sac_path}"

    bucket_download(bucket_name=bucket_name, object_name=sac_path, station_name=station_name)
    bucket_upload(file_path=file_path, bucket_name=bucket_name, object_name="teste")