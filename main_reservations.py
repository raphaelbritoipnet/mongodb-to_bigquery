import functions_framework
from data_transfer_reservations import DataMigrator

mongo_uri = 'mongodb+srv://ipnet:cuw5xkg_mtc*ftb1TGV@production.sv33w.mongodb.net/'

db_name = 'booking'
collection_name= 'reservations'

bigquery_project_id = 'housi-dados'
bigquery_dataset_id = 'MONGODB_RAW_ZONE'
bigquery_table_id = 'reservations_raw'

@functions_framework.http
def mongo_to_db(request):
    migrator = DataMigrator(mongo_uri, bigquery_project_id, bigquery_dataset_id, bigquery_table_id)
    migrator.migrate_collection(db_name, collection_name, batch_size=5000)
    return 'Processamento Finalizado com Sucesso.'