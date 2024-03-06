import functions_framework
from data_transfer import DataMigrator

MONGO_URI = 'mongodb+srv://ipnet:cuw5xkg_mtc*ftb1TGV@production.sv33w.mongodb.net/'
MONGO_DB = ['booking']
MONGO_COLLECTION = ['guests']
BIGQUERY_PROJECT_ID = 'housi-dados'
BIGQUERY_DATASET_ID = 'MONGODB_RAW_ZONE'
BIGQUERY_TABLE_ID = ['guests_raw']

@functions_framework.http
def mongo_to_db(request):
    for item in range(len(MONGO_DB)):
        migrator = DataMigrator(MONGO_URI, MONGO_DB[item], MONGO_COLLECTION[item], BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID[item])
        migrator.migrate_data()
    return 'Processamento Finalizado com Sucesso.'