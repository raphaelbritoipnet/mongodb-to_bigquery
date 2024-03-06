from pymongo import MongoClient
from google.cloud import bigquery
from bson import ObjectId
import datetime
import json
import time

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, datetime.datetime):
            return o.isoformat()
        return super().default(o)

class DataMigrator:
    def __init__(self, mongo_uri, bigquery_project_id, bigquery_dataset_id, bigquery_table_id):
        self.mongo_uri = mongo_uri
        self.bigquery_project_id = bigquery_project_id
        self.bigquery_dataset_id = bigquery_dataset_id
        self.bigquery_table_id = bigquery_table_id
        self.client = MongoClient(mongo_uri)
        self.bq_client = bigquery.Client(project=bigquery_project_id)
        self.dataset_ref = self.bq_client.dataset(bigquery_dataset_id)
        self.table_ref = self.dataset_ref.table(bigquery_table_id)
    
    # Função para converter dados do documento MongoDB para formato compatível com BigQuery
    def prepare_document(self, document):
        # Função para converter ObjectId e outros tipos não padrão
        def convert_value(value):
            if isinstance(value, ObjectId):
                return str(value)
            if isinstance(value, datetime.datetime):
                return value.isoformat()
            if isinstance(value, (list, dict)):
                return json.dumps(value, default=str)  # Usamos json.dumps para listas e dicionários
            return value
        return {k: convert_value(v) for k, v in document.items()}
    
    # Função para inferir o esquema do BigQuery a partir de um documento MongoDB
    def infer_schema_from_document(self, collection):
        
        # Obtém uma amostra de documento para inferir o esquema
        sample_document = collection.find_one()
        if sample_document is None:
            return
        
        # Cria a tabela no BigQuery com base no esquema inferido
        schema = []
        for field_name, value in sample_document.items():
            field_type = 'STRING'  # Tipo padrão como STRING
            if type(value) == ObjectId:
                field_type = 'STRING'  # Converte ObjectId para STRING
            elif type(value) == int:
                field_type = 'INTEGER'
            elif type(value) == float:
                field_type = 'FLOAT'
            elif type(value) == bool:
                field_type = 'BOOLEAN'
            elif type(value) == datetime.datetime:
                field_type = 'TIMESTAMP'
            elif type(value) == datetime.date:
                field_type = 'DATE'
            elif type(value) == datetime.time:
                field_type = 'TIME'
            elif type(value) == bytes:
                field_type = 'BYTES'
            schema.append(bigquery.SchemaField(field_name, field_type))
        return schema
    
    def create_table(self, schema) -> None:
        try:
            table = bigquery.Table(self.table_ref, schema=schema)
            table = self.bq_client.create_table(table)
            print(f"Tabela {self.bigquery_table_id} criada com sucesso no BigQuery")
        except Exception as e:
            print(f"A tabela {self.bigquery_table_id} já existe no BigQuery.")

    def migrate_collection(self, db_name, collection_name, batch_size=1000):
        db = self.client[db_name]
        collection = db[collection_name]
        
        # Carrega os dados do MongoDB para o BigQuery em lotes
        total_docs = collection.count_documents({})
        num_batches = (total_docs // batch_size) + 1
        for i in range(num_batches):
            offset = i * batch_size

            rows_to_insert = [self.prepare_document(doc) for doc in collection.find().skip(offset).limit(batch_size)]
        
            job_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                #schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
            )

            job = self.bq_client.load_table_from_json(rows_to_insert, self.table_ref, job_config=job_config)
            job.result()
            print(f"Bloco {i+1}/{num_batches} carregado com sucesso para a tabela {self.bigquery_table_id} no BigQuery")
            time.sleep(1)  # Aguarda 1 segundo entre os lotes para não sobrecarregar o BigQuery