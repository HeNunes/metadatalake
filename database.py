import pandas as pd
import couchdb

def create_db(couch):
    couch.delete('metadatalake')
    db = couch.create('metadatalake')

    df = pd.DataFrame(pd.read_csv('metadata.csv'))
    df_dict = [row.to_dict() for i, row in df.iterrows()]

    for doc in df_dict:
        db.save(doc=doc)

    print('DB criada com sucesso!')

def get_results(db, query):
    results = db.find(query)
    for result in results:
        print(result)

def source_only_1(db):
    query = {
        "selector": {
            "station_state": "Alagoas"
        },
        "fields": ["_id", "ID", "connection_username"]
    }
    get_results(db, query)

def source_only_2(db):
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
            "station_state": "Alagoas"
        },
        "fields": ["_id", "ID", "connection_username"],
        "sort": [
            {"date": "desc"}, 
            {"timestamp": "desc"}
        ],
        "limit": 1
    }
    get_results(db, query)

if __name__ == '__main__':
    couch = couchdb.Server('http://couchdb:couchdb123@localhost:5984')
        
    db = couch['metadatalake']
    print(db)

    source_only_2(db=db)
