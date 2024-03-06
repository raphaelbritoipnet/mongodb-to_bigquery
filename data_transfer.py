from pymongo import MongoClient
from google.cloud import bigquery
from bson import ObjectId
import json
from datetime import datetime

class DataMigrator:
    def __init__(self, mongo_uri, mongo_db, mongo_collection, bigquery_project_id, bigquery_dataset_id, bigquery_table_id):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.bigquery_project_id = bigquery_project_id
        self.bigquery_dataset_id = bigquery_dataset_id
        self.bigquery_table_id = bigquery_table_id
        self.field_mapping = {}

    def connect_to_mongodb(self):
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db]
        self.collection = self.db[self.mongo_collection]

    def connect_to_bigquery(self):
        self.bq_client = bigquery.Client(project=self.bigquery_project_id)
        self.dataset_ref = self.bq_client.dataset(self.bigquery_dataset_id)
        self.table_ref = self.dataset_ref.table(self.bigquery_table_id)
        self.table = self.bq_client.get_table(self.table_ref)

    def generate_field_list(self):
        self.connect_to_mongodb()
        sample_document = self.collection.find_one()
        if sample_document:
            return list(sample_document.keys())
        else:
            return [] 

    def convert_document_recursive(self, document):
        converted_doc = {}
        for key, value in document.items():
            if isinstance(value, dict):
                converted_doc[key] = self.convert_document_recursive(value)
            elif isinstance(value, list):
                converted_doc[key] = [self.convert_document_recursive(item) if isinstance(item, dict) else item for item in value]
            elif isinstance(value, ObjectId):
                converted_doc[key] = str(value)
            #elif isinstance(value, bool):
            #    converted_doc[key] = str(value)   
            elif isinstance(value, bool):
                if value:
                    converted_doc[key] = "True" 
                else:
                    converted_doc[key] = "False"
            elif isinstance(value, int):
                converted_doc[key] = str(value)    
            elif isinstance(value, datetime):
                converted_doc[key] = value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                converted_doc[key] = value
        return converted_doc
    
    def convert_document(self, obj):
        if isinstance(obj, bson.objectid.ObjectId):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, list) and all(isinstance(item, str) for item in obj):
            return obj
        elif isinstance(obj, dict):
            cleaned_dict = {}
            for k, v in obj.items():
                if isinstance(v, bson.objectid.ObjectId):
                    cleaned_dict[k] = str(v)
                else:
                    cleaned_dict[k] = v
            return cleaned_dict
        return obj


    def migrate_data(self):
        self.connect_to_mongodb()
        data_from_mongodb = self.collection.find()

        num_documents = sum(1 for _ in data_from_mongodb)
        data_from_mongodb.rewind()

        print(f"Total de documentos encontrados: {num_documents}")

        self.connect_to_bigquery()

        fields = self.generate_field_list()
        batch_size = 1000
        rows_to_insert = []
        for document in data_from_mongodb:
            converted_document = self.convert_document_recursive(document)
            for key, value in converted_document.items():
                if key == 'statuses':
                    converted_document[key] = [status['name'] for status in value] if isinstance(value, list) else None
                if isinstance(value, dict):
                    converted_document[key] = json.dumps(value)
            row = [converted_document.get(field, None) for field in fields]   
            rows_to_insert.append(row)

            if len(rows_to_insert) >= batch_size:
                errors = self.bq_client.insert_rows(self.table, rows_to_insert)
                if errors:
                    print('Ocorreram erros durante a inserção:', errors)
                rows_to_insert = [] 

        if rows_to_insert:
            errors = self.bq_client.insert_rows(self.table, rows_to_insert)
            if errors:
                print('Ocorreram erros durante a inserção:', errors)

        print('Processo de inserção concluído!')

