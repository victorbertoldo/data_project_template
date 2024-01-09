from connectors.database_connector import MSSQLDatabaseConnector, PostgresDatabaseConnector
from connectors.api_connector import APIConnector
from processors.data_processor import DataProcessor
from connectors.file_connector import FileConnector
from config import API_URL
from config import FILE_PATH  # Assuming the path to your file is stored in an environment variable
from utils.logger import setup_logger
from processors.transform import dynamically_flatten_dataframes

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

        logger.info(f"Starting Getting data from an API")
        # API data ingestion
        api_connector = APIConnector(API_URL)
        api_data = api_connector.fetch_data()

        # Process and insert API data
        processor = DataProcessor()
        processor.process_api_data(api_data)

        logger.info(f"End of Getting data from an API")

        logger.info(f"Starting Getting data from a File")

        file_connector = FileConnector()
        file_data_list = file_connector.process_files()

        postgres_connector = PostgresDatabaseConnector()
        for dataframe, table_name in file_data_list:
            # Dynamically flatten the DataFrame if it contains nested structures
            dataframe = dynamically_flatten_dataframes(dataframe)
            postgres_connector.check_and_create_database("test", "landing")
            full_table_name = f"landing.{table_name}"  # Schema and table name
            postgres_connector.insert_dataframe(dataframe, full_table_name)

        # # File data ingestion
        # file_connector = FileConnector()
        # postgres_connector = PostgresDatabaseConnector()  # Assuming the PostgresDatabaseConnector is already set up         

        # # Read the file into a DataFrame
        # dataframe = file_connector.read_file()  # Use the read_file method 

        # logger.info(f"End of Getting data from a File")

        # logger.info(f"Starting Inserting data (file or api) into PostgreSQL")
        # # Insert the DataFrame into PostgreSQL
        # if dataframe is not None:
        #     postgres_connector.insert_dataframe(dataframe, "file_products")
        #     print("Data inserted into PostgreSQL successfully.")
        # else:
        #     print("Failed to read data from the file.")

        # logger.info(f"End of Inserting data (file or api) into PostgreSQL")

        logger.info(f"Starting Data Extraction from a MSSQL Server")
        # Fetch data from MSSQL
        mssql_connector = MSSQLDatabaseConnector()
        source_data = mssql_connector.fetch_data("SELECT * FROM [source].dbo.escolas_inep")

        logger.info(f"End of Data Extraction from a MSSQL Server")

        logger.info(f"Starting Ingest Data into Postgres Database")
        # Insert data into PostgreSQL
        postgres_connector = PostgresDatabaseConnector()
        postgres_connector.check_and_create_database("test", "landing")
        postgres_connector.insert_data(source_data, "test.landing.escolas_inep")
        postgres_connector.close_connection()

        logger.info(f"End of Ingest Data into Postgres Database")

        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        raise

    logger.info("Data ingestion completed successfully.")
    

if __name__ == "__main__":
    run_data_ingestion()
