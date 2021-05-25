import os.path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dailyBenchamarking import downloadDatasets,convertDatasetsToJson,\
    setupPostgresDatabase,setupMongodbDatabase,setupNeo4jDatabase,singleReadQuery,singleWriteQuery,\
    aggregateQuery,neighborsQuery,neighbors2Query,shortestpathQuery,deleteDatabasesAndDataset,neighbors2dataQuery


def download_And_Extract_Dataset():
    downloadDatasets()

def convert_datasets_to_json():
    convertDatasetsToJson()

def setup_Postgres_Database():
    setupPostgresDatabase()

def setup_Mongodb_Database():
    setupMongodbDatabase()

def setup_Neo4j_Database():
    setupNeo4jDatabase()

def single_Read_Query():
    singleReadQuery()

def single_Write_Query():
    singleWriteQuery()

def aggregate_Query():
    aggregateQuery()

def neighbors_Query():
    neighborsQuery()

def neighbors2_Query():
    neighbors2Query()

def neighbors2_data_Query():
    neighbors2dataQuery()

def shortest_path_Query():
    shortestpathQuery()

def delete_Databases_And_Dataset():
     deleteDatabasesAndDataset()



with DAG(dag_id="benchmarking_dag",
        schedule_interval='@daily',
        default_args={
            "owner": "airflow",
            "retries": 0,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1)
        },catchup=False) as dag:

        download_And_Extract_Dataset = PythonOperator(
            task_id="download_And_Extract_Dataset",
            provide_context=False,
            python_callable=download_And_Extract_Dataset
        )

        convert_datasets_to_json = PythonOperator(
            task_id="convert_datasets_to_json",
            provide_context=False,
            python_callable=convert_datasets_to_json
        )

        setup_Postgres_Database = PythonOperator(
            task_id="setup_Postgres_Database",
            provide_context=False,
            python_callable=setup_Postgres_Database
        )

        setup_Mongodb_Database = PythonOperator(
            task_id="setup_Mongodb_Database",
            provide_context=False,
            python_callable=setup_Mongodb_Database
        )

        setup_Neo4j_Database = PythonOperator(
            task_id="setup_Neo4j_Database",
            provide_context=False,
            python_callable=setup_Neo4j_Database
        )

        single_Read_Query = PythonOperator(
            task_id="single_Read_Query",
            provide_context=False,
            python_callable=single_Read_Query
        )

        single_Write_Query = PythonOperator(
            task_id="single_Write_Query",
            provide_context=False,
            python_callable=single_Write_Query
        )

        aggregate_Query = PythonOperator(
            task_id="aggregate_Query",
            provide_context=False,
            python_callable=aggregate_Query
        )

        neighbors_Query = PythonOperator(
            task_id="neighbors_Query",
            provide_context=False,
            python_callable=neighbors_Query
        )

        neighbors2_Query = PythonOperator(
            task_id="neighbors2_Query",
            provide_context=False,
            python_callable=neighbors2_Query
        )

        neighbors2_data_Query = PythonOperator(
            task_id="neighbors2_data_Query",
            provide_context=False,
            python_callable=neighbors2_data_Query
        )

        shortest_path_Query = PythonOperator(
            task_id="shortest_path_Query",
            provide_context=False,
            python_callable=shortest_path_Query
        )

        delete_Databases_And_Dataset = PythonOperator(
            task_id="delete_Databases_And_Dataset",
            provide_context=False,
            trigger_rule='all_done',
            python_callable=delete_Databases_And_Dataset
        )

download_And_Extract_Dataset >> convert_datasets_to_json >> [setup_Postgres_Database, setup_Mongodb_Database, setup_Neo4j_Database]
setup_Postgres_Database >> [single_Read_Query,single_Write_Query,aggregate_Query,neighbors_Query,neighbors2_Query,neighbors2_data_Query,shortest_path_Query] >> delete_Databases_And_Dataset
setup_Mongodb_Database >> [single_Read_Query,single_Write_Query,aggregate_Query,neighbors_Query,neighbors2_Query,neighbors2_data_Query,shortest_path_Query]
setup_Neo4j_Database >> [single_Read_Query,single_Write_Query,aggregate_Query,neighbors_Query,neighbors2_Query,neighbors2_data_Query,shortest_path_Query]



####-------following lines runs queries sequentially----#######

# download_And_Extract_Dataset >> convert_datasets_to_json >> [setup_Postgres_Database, setup_Mongodb_Database, setup_Neo4j_Database] >> single_Read_Query >> single_Write_Query >> aggregate_Query \
# >> neighbors_Query >> neighbors2_Query >> neighbors2_data_Query >> shortest_path_Query >> delete_Databases_And_Dataset
