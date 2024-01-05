from connectors.database_connector import MSSQLDatabaseConnector, PostgresDatabaseConnector
from connectors.api_connector import APIConnector
from processors.data_processor import DataProcessor
from config import API_URL

def run_data_ingestion():
    # Fetch data from MSSQL
    mssql_connector = MSSQLDatabaseConnector()
    source_data = mssql_connector.fetch_data("SELECT * FROM [source].dbo.escolas_inep")

    # Insert data into PostgreSQL
    postgres_connector = PostgresDatabaseConnector()
    postgres_connector.check_and_create_database("source", "landing")
    postgres_connector.insert_data(source_data, "source.landing.escolas_inep")
    postgres_connector.close_connection()

    # API data ingestion
    api_connector = APIConnector(API_URL)
    api_data = api_connector.fetch_data()

    # Process and insert API data
    processor = DataProcessor()
    processor.process_api_data(api_data)

if __name__ == "__main__":
    run_data_ingestion()
