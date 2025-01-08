import pandas as pd
from obspy import read
import boto3
from botocore.client import Config
import io
import couchdb
import requests
from MinioUtils import get_sac_from_minio

def metadata_only_1(couchdb_url, database_name, design_doc_name, view_name, minio_client):
    map_function = '''
        function (doc) {
            var date = new Date(doc.endtime);
            var yearMonthInt = (date.getUTCMonth() + 1); // O mês é baseado em zero
            if (!isNaN(date.getTime())) {
                emit(yearMonthInt, { npts: doc.npts, data_size: doc.npts * 32 });
            }
        }
    '''

    reduce_function = '''
        function (keys, values, rereduce) {
            var result = { npts: 0, data_size: 0 };
            
            // Se for o processo de reredução, agregue os resultados
            if (rereduce) {
                values.forEach(function(value) {
                    result.npts += value.npts;
                    result.data_size += value.data_size;
                });
            } else {
                // Se for a primeira redução, some os valores
                values.forEach(function(value) {
                    result.npts += value.npts;
                    result.data_size += value.data_size;
                });
            }
            
            return result;
        }
    '''

    design_doc = {
        "_id": "_design/{}".format(design_doc_name),
        "views": {
            view_name: {
                "map": map_function,
                "reduce": reduce_function
            }
        }
    }

    url = f'{couchdb_url}/{database_name}/{design_doc["_id"]}'
    response = requests.put(url, json=design_doc)

    if response.status_code not in [201, 202]:
        print(f"Falha ao subir o design doc: {response.status_code}, {response.text}")

    view_url = f'{couchdb_url}/{database_name}/_design/{design_doc_name}/_view/{view_name}?group=true'
    response = requests.get(view_url)
    results = response.json()
    print(results)
    rows = results.get('rows', [])
    
    data = []
    for row in rows:
        month = row['key']
        npts = row['value']['npts']
        data_size = row['value']['data_size']
        data.append([month, npts, data_size])

    results_df = pd.DataFrame(data, columns=['month', 'npts', 'data_size'])
    aggregated_df = results_df.groupby('month').agg({
        'npts': 'sum',
        'data_size': 'sum'
    }).reset_index()

    file = io.BytesIO()
    aggregated_df.to_csv(file, index=False)
    file.seek(0)

    print("Escrevendo resultado")
    minio_client.put_object(Bucket="bucket-teste", Key="metadata_only_1", Body=file, ContentLength=file.getbuffer().nbytes)  

def metadata_only_2(couchdb_url, metadata_db_name, design_doc_name, view_name, minio_client):
    map_function = '''
    function (doc) {
        if (doc.station) {
            emit(doc.station, { type: "metadata", filename: doc.filename });
        } else if (doc.Estação) {
            emit(doc.Estação, { type: "provider", state: doc.Estado });
        } else {
            emit("undefined_station", { type: "unknown", message: "station or Estação not found" });
        }
    }
    '''

    reduce_function = '''
    function (keys, values, rereduce) {
        var result = { metadata: 0, state: null };
        if (rereduce) {
            values.forEach(function(value) {
                if (value.metadata) {
                    result.metadata += value.metadata;
                }
                if (value.state) {
                    result.state = value.state;
                }
            });
        } else {
            values.forEach(function(value) {
                if (value.type === "metadata") {
                    result.metadata += 1;
                } else if (value.type === "provider") {
                    result.state = value.state;
                }
            });
        }
        return result;
    }
    '''

    design_doc_metadata = {
        "_id": f"_design/{design_doc_name}",
        "views": {
            view_name: {
                "map": map_function,
                "reduce": reduce_function
            }
        }
    }
    
    url = f"{couchdb_url}/{metadata_db_name}/{design_doc_metadata['_id']}"
    response = requests.delete(url)

    response = requests.put(url, json=design_doc_metadata)
    if response.status_code not in [201, 202]:
        print(f"Failed to upload design doc to {metadata_db_name}: {response.status_code} {response.text}")

    view_url = f'{couchdb_url}/{metadata_db_name}/_design/{design_doc_name}/_view/{view_name}?group=true'
    response = requests.get(view_url)

    results = response.json()
    rows = results.get("rows", [])
    data = [
        {
            "Station": row["key"],
            "State": row["value"].get("state", None),
            "Files": row["value"].get("metadata", [])
        }
        for row in rows
    ]
    df = pd.DataFrame(data)
    
    file = io.BytesIO()
    df.to_csv(file, index=False)
    file.seek(0)

    print("Escrevendo resultado")
    minio_client.put_object(Bucket="bucket-teste", Key="metadata_only_2", Body=file, ContentLength=file.getbuffer().nbytes)

def metadata_only_3(couchdb_url, database_name, design_doc_name, view_name, minio_client):
    map_function = '''
        function (doc) {
            if (doc.filename && doc.layer) {
                emit(doc.layer, { filename: doc.filename, npts: doc.npts, data_size: doc.npts * 32 });
            }
        }
    '''

    reduce_function = '''
        function (keys, values, rereduce) {
            var result = { files: 0, npts: 0, data_size: 0 };

            if (rereduce) {
                // Durante a re-redução, agregue os valores
                values.forEach(function(value) {
                    result.files += value.files; // Somar o número de arquivos
                    result.npts += value.npts;   // Somar os npts
                    result.data_size += value.data_size; // Somar o data_size
                });
            } else {
                // Durante a primeira redução, apenas agregue os valores
                result.files = values.length;  // Contar o número de arquivos distintos (um para cada entry)
                result.npts = 0;
                result.data_size = 0;

                values.forEach(function(value) {
                    result.npts += value.npts;  // Somar os npts
                    result.data_size += value.data_size;  // Somar o data_size
                });
            }

            return result;
        }
    '''

    design_doc = {
        "_id": "_design/{}".format(design_doc_name),
        "views": {
            view_name: {
                "map": map_function,
                "reduce": reduce_function
            }
        }
    }

    url = f'{couchdb_url}/{database_name}/{design_doc["_id"]}'
    response = requests.put(url, json=design_doc)

    if response.status_code not in [201, 202]:
        print(f"Falha ao subir o design doc: {response.status_code}, {response.text}")

    view_url = f'{couchdb_url}/{database_name}/_design/{design_doc_name}/_view/{view_name}?group=true'
    response = requests.get(view_url)
    results = response.json()
    
    rows = results.get("rows", [])
    data = [
        {
            "Layer": row["key"],
            "Files": row["value"].get("files", None),
            "Points": row["value"].get("npts", None),
            "Data Size": row["value"].get("data_size", None)
        }
        for row in rows
    ]
    df = pd.DataFrame(data)

    file = io.BytesIO()
    df.to_csv(file, index=False)
    file.seek(0)

    print("Escrevendo resultado")
    minio_client.put_object(Bucket="bucket-teste", Key="metadata_only_3", Body=file, ContentLength=file.getbuffer().nbytes)

def source_only_1(couch, minio_client):
    metadata = couch['metadatalake']
    provider = couch['data_provider']

    provider_query = {
        "selector": {
            "Estado": "AL"
        },
        "fields": ["Estação"],
        "limit": 105
    }
    provider_results = {doc['Estação'] for doc in provider.find(provider_query)}

    metadata_query = {
        "selector": {
            "station": {"$in": list(provider_results)},
            "ID": {"$gte": 0}
        },
        "fields": ["filename", "station"],
        "limit": 50000
    }
    metadata_results = ((doc['filename'], doc['station']) for doc in metadata.find(metadata_query))

    print("Analisando os arquivos")

    max_amplitude = 0
    for result in metadata_results:
        obs_file = get_sac_from_minio(bucket_name="bucket-teste", object_name=result[0], 
                                    station_name=result[1], minio_client=minio_client)
        obs = read(obs_file)
        amplitude = obs[0].data.max() - obs[0].data.min()

        if amplitude > max_amplitude:
            max_amplitude = amplitude
            file_result = result

    print(max_amplitude)

    obs = get_sac_from_minio(bucket_name="bucket-teste", object_name=file_result[0], 
                            station_name=file_result[1].upper(), minio_client=minio_client)

    print("Escrevendo resultado")
    file = io.BytesIO()
    read(obs).write(file, format="SAC")
    file.seek(0)
    minio_client.put_object(Bucket="bucket-teste", Key="source-only-1", Body=file, ContentLength=file.getbuffer().nbytes)

def source_only_2(couch, minio_client):
    metadata = couch['metadatalake']
    
    index_time = {
        "index": {
            "fields": ["endtime"]
        },
        "name": "endtime_index",
        "type": "json"
    }
    print("Metadatalake query")
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

    print("Baixando arquivo")
    obs = get_sac_from_minio(bucket_name="bucket-teste", object_name=result['filename'], 
                        station_name=(result['station']).upper(), minio_client=minio_client)

    print("Escrevendo resultado")
    file = io.BytesIO()
    read(obs).write(file, format="SAC")
    file.seek(0) 
    minio_client.put_object(Bucket="bucket-teste", Key="source_only_2", Body=file, ContentLength=file.getbuffer().nbytes)

def full_1(couchdb_url, database_name, design_doc_name, view_name, minio_client):
    map_function = '''
        function (doc) {
            if(doc.station) {
                emit(doc.station, { filename: doc.filename, npts: doc.npts, data_size: doc.npts * 32 });    
            }
        }
    '''

    reduce_function = '''
        function (keys, values, rereduce) {
            var result = {};

            if (rereduce) {
                values.forEach(function (partialResult) {
                    for (var station in partialResult) {
                        if (!result[station] || partialResult[station].data_size > result[station].data_size) {
                            result[station] = partialResult[station];
                        }
                    }
                });
            } else {
                keys.forEach(function (key, index) {
                    var station = key[0]; // A chave é o nome da estação
                    var value = values[index];
                    if (!result[station] || value.data_size > result[station].data_size) {
                        result[station] = value;
                    }
                });
            }
            return result;
        }
    '''

    design_doc = {
        "_id": "_design/{}".format(design_doc_name),
        "views": {
            view_name: {
                "map": map_function,
                "reduce": reduce_function
            }
        }
    }

    url = f'{couchdb_url}/{database_name}/{design_doc["_id"]}'
    response = requests.put(url, json=design_doc)
    
    if response.status_code not in [201, 202]:
        print(f"Falha ao subir o design doc: {response.status_code}, {response.text}")

    view_url = f'{couchdb_url}/{database_name}/_design/{design_doc_name}/_view/{view_name}'
    response = requests.get(view_url)
    results = response.json()

    rows = results.get('rows', [])
    data = []

    for row in rows:
        value = row['value']
        
        for station, station_data in value.items():
            filename = station_data.get('filename')
            npts = station_data.get('npts')
            data_size = station_data.get('data_size')
            
            data.append([station, filename, npts, data_size])

    results_df = pd.DataFrame(data, columns=['station', 'filename', 'npts', 'data_size'])

    print(results_df)

    # Calculando a amplitude e adicionando no dataset
    amplitudes = []
    for index, row in results_df.iterrows():
        try:
            obs_file = get_sac_from_minio(bucket_name="bucket-teste", object_name=row['filename'], 
                                        station_name=row['station'], minio_client=minio_client)
            obs = read(obs_file)
            amplitude = obs[0].data.max() - obs[0].data.min()

            amplitudes.append(amplitude)
            print(amplitude)
        except:
            print("Arquivo não encontrado no Minio")
            amplitudes.append(0)
    results_df['amplitude'] = amplitudes

    print(results_df)

    file = io.BytesIO()
    results_df.to_csv(file, index=False)
    file.seek(0)

    print("Escrevendo resultado")
    minio_client.put_object(Bucket="bucket-teste", Key="full_1", Body=file, ContentLength=file.getbuffer().nbytes) 

def full_2(couchdb_url, database_name, design_doc_name, view_name, minio_client):
    map_function = '''
        function (doc) {
            if (doc.station && doc.filename) {

                var date = new Date(doc.endtime);
                var Year = date.getUTCFullYear();

                if (Year === 2022 && !isNaN(date.getTime())) {
                    var yearMonthInt = date.getUTCMonth() + 1; // O mês é baseado em zero
                    emit([doc.station, yearMonthInt], { filename: doc.filename });
                }
            }
        }
    '''

    design_doc = {
        "_id": "_design/{}".format(design_doc_name),
        "views": {
            view_name: {
                "map": map_function
            }
        }
    }

    url = f'{couchdb_url}/{database_name}/{design_doc["_id"]}'
    response = requests.put(url, json=design_doc)
    
    if response.status_code not in [201, 202]:
        print(f"Falha ao subir o design doc: {response.status_code}, {response.text}")

    view_url = f'{couchdb_url}/{database_name}/_design/{design_doc_name}/_view/{view_name}'
    response = requests.get(view_url)
    results = response.json()

    stations = []
    months = []
    filenames = []

    for row in results['rows']:
        stations.append(row['key'][0])
        months.append(row['key'][1])
        filenames.append(row['value']['filename'])

    df = pd.DataFrame({
        'station' : stations,
        'month' : months,
        'filename' : filenames 
    })

    agg_df = df.groupby(['station', 'month'])['filename'].apply(list).reset_index()

    amplitudes = []
    for index, row in agg_df.iterrows():
        total_amplitude = 0
        count = 0

        for file in row['filename']:
            try:
                obs_file = get_sac_from_minio(bucket_name="bucket-teste", object_name=file, 
                                            station_name=row['station'], minio_client=minio_client)
                obs = read(obs_file)
                
                amplitude = obs[0].data.max() - obs[0].data.min()
                total_amplitude += amplitude
                count += 1
            except Exception as e:
                pass

        if count > 0:
            average_amplitude = total_amplitude / count
        else:
            average_amplitude = 0

        amplitudes.append(average_amplitude)
        print(f"Amplitude média para {row['station']} no mês {row['month']}: {average_amplitude}")

    agg_df['average_amplitude'] = amplitudes
    print(agg_df)

    file = io.BytesIO()
    agg_df.to_csv(file, index=False)
    file.seek(0)

    print("Escrevendo resultado")
    minio_client.put_object(Bucket="bucket-teste", Key="full_2", Body=file, ContentLength=file.getbuffer().nbytes) 

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

    source_only_1(couch=couch, minio_client=minio_client)
   
   # source_only_2(couch=couch, minio_client=minio_client)

   # metadata_only_1(couchdb_url='http://couchdb:couchdb123@localhost:5984', database_name='metadatalake', 
    #                design_doc_name='monthly_aggregation', view_name='monthly_data', minio_client=minio_client)
    
    #metadata_only_2(couchdb_url='http://couchdb:couchdb123@localhost:5984', metadata_db_name='metadatalake',
    #                design_doc_name='count_per_station', view_name='statio_count', minio_client=minio_client)

   # metadata_only_3(couchdb_url='http://couchdb:couchdb123@localhost:5984', database_name='metadatalake', 
    #                design_doc_name='layer_aggregation', view_name='layer_agg', minio_client=minio_client)
     
    
   #full_1(couchdb_url='http://couchdb:couchdb123@localhost:5984', database_name='metadatalake', 
    #                design_doc_name='size_per_station', view_name='data_size_agg', minio_client=minio_client)

    full_2(couchdb_url='http://couchdb:couchdb123@localhost:5984', database_name='metadatalake', 
                    design_doc_name='month_station_aggregation', view_name='month_station_agg', minio_client=minio_client)

    """
    curl -X GET http://couchdb:couchdb123@localhost:5984/metadatalake/_design/monthly_aggregation/_view/monthly_data
    """