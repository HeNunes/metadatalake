import pandas as pd
import couchdb

def create_db(couch):
    couch.delete('metadatalake')
    db = couch.create('metadatalake')

    df = pd.DataFrame(pd.read_csv('metadatalake.csv'))
    df.fillna("null", inplace=True)
    df_dict = [row.to_dict() for i, row in df.iterrows()]

    for doc in df_dict:
        db.save(doc=doc)

    print('DB criada com sucesso!')

def get_results(db, query):
    results = db.find(query)
    for result in results:
        print(result)

def source_only_1(couch):
    metadata = couch['metadatalake']
    provider = couch['data_provider']

    print(metadata, provider)

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
        "limit": 1
    }

    metadata_results = list(metadata.find(metadata_query))
    metadata_results = {(doc['filename'], doc['station']) for doc in metadata_results}

    provider_results = list(provider.find(provider_query))
    provider_results = {doc['Estação'] for doc in provider_results}

    metadata_results = [doc for doc in metadata_results if doc[1] in provider_results]

    for result in metadata_results:
        print(result)
        
def source_only_2(couch):
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

    for res in results:
        print(res)

if __name__ == '__main__':
    couch = couchdb.Server('http://couchdb:couchdb123@localhost:5984')
        
    #create_db(couch=couch)

    source_only_2(couch=couch)