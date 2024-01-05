import pandas as pd
from connectors.database_connector import PostgresDatabaseConnector

class DataProcessor:
    def process_api_data(self, data):
        # Convert the data to a DataFrame
        df = pd.DataFrame(data)

        # Insert the DataFrame into the PostgreSQL database
        self.insert_into_postgres(df, "landing.dummy_posts_comments")

    def insert_into_postgres(self, dataframe, table_name):
        # Assuming the PostgresDatabaseConnector is already set up to insert DataFrames
        postgres_connector = PostgresDatabaseConnector()
        postgres_connector.insert_dataframe(dataframe, table_name)

    # ... other methods ...
