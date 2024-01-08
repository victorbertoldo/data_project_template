import pandas as pd
from connectors.database_connector import PostgresDatabaseConnector

class DataProcessor:
    def process_api_data(self, data):
        """
        Processes the API data by converting it to a DataFrame and inserting it into the PostgreSQL database.

        Parameters:
        - data (list or dict): The data to be processed.

        Returns:
        None
        """
        # Convert the data to a DataFrame
        df = pd.DataFrame(data)

        # Insert the DataFrame into the PostgreSQL database
        self.insert_into_postgres(df, "landing.dummy_posts_comments")

    def insert_into_postgres(self, dataframe, table_name):
        """
        Insert a DataFrame into a Postgres table.

        Parameters:
            dataframe (DataFrame): The DataFrame to be inserted.
            table_name (str): The name of the table in which to insert the DataFrame.

        Returns:
            None
        """
        # Assuming the PostgresDatabaseConnector is already set up to insert DataFrames
        postgres_connector = PostgresDatabaseConnector()
        postgres_connector.insert_dataframe(dataframe, table_name)

    # ... other methods ...
