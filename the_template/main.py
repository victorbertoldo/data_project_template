from connectors.database_connector import MSSQLDatabaseConnector, PostgresDatabaseConnector
from connectors.api_connector import APIConnector
from processors.data_processor import DataProcessor
from config import API_URL
from utils.logger import setup_logger

# Initialize the logger
logger = setup_logger('data_ingestion', 'data_ingestion.log')


def run_data_ingestion():
    """
    Run the data ingestion process.

    This function fetches data from an MSSQL database, inserts it into a PostgreSQL database,
    fetches data from an API, and processes and inserts the API data. It logs the start and end
    of the data ingestion process.

    Parameters:
    None

    Returns:
    None

    Raises:
    Any exception that occurs during the data ingestion process.

    """
    

    logger.info("Data ingestion started.")

    try:
        # Fetch data from MSSQL
        mssql_connector = MSSQLDatabaseConnector()
        source_data = mssql_connector.fetch_data("SELECT * FROM [source].dbo.escolas_inep")

        # Insert data into PostgreSQL
        postgres_connector = PostgresDatabaseConnector()
        postgres_connector.check_and_create_database("test", "landing")
        postgres_connector.insert_data(source_data, "test.landing.escolas_inep")
        postgres_connector.close_connection()

        # API data ingestion
        api_connector = APIConnector(API_URL)
        api_data = api_connector.fetch_data()

        # Process and insert API data
        processor = DataProcessor()
        processor.process_api_data(api_data)
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        raise

    logger.info("Data ingestion completed successfully.")
    

if __name__ == "__main__":
    run_data_ingestion()
