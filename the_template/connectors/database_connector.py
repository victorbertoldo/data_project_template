import psycopg2
import pymssql
import sqlalchemy
from config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_SERVER, POSTGRES_PORT, POSTGRES_DB
from config import MSSQL_SERVER, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE

from processors.transform import sanitize_column_names

class MSSQLDatabaseConnector:
    def __init__(self):
        self.connection = pymssql.connect(server=MSSQL_SERVER, user=MSSQL_USER, 
                                          password=MSSQL_PASSWORD, database=MSSQL_DATABASE)

    def fetch_data(self, query):
        cursor = self.connection.cursor(as_dict=True)
        cursor.execute(query)
        return cursor.fetchall()   
        

class PostgresDatabaseConnector:
    def __init__(self):
        self.connection = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_SERVER,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        )

    def close_connection(self):
        if self.connection:
            self.connection.close()

    def check_and_create_database(self, database_name, schema_name):
        # Connect to the default database to create a new database
        conn = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_SERVER,
            port=POSTGRES_PORT,
            database="postgres"  # Default system database
        )
        conn.autocommit = True

        # Create database if it doesn't exist
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {database_name}")
        cursor.close()

        # Close the connection to the default database
        conn.close()

        # Connect to the newly created or existing database to create schema
        self.connection = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_SERVER,
            port=POSTGRES_PORT,
            database=database_name
        )
        self.connection.autocommit = True

        # Check and create schema
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE SCHEMA {schema_name}")
        cursor.close()

    def ensure_table_exists(self, data, table_name):
        cursor = self.connection.cursor()

        # Check if table exists
        cursor.execute(f"SELECT to_regclass('{table_name}')")
        if cursor.fetchone()[0] is None:
            print(f"Table {table_name} not found. Creating table.")

            table_parts = table_name.split('.')
            modified_table_name = '.'.join(table_parts[-2:]) if len(table_parts) > 2 else table_name


            # Get column definitions from data
            column_definitions = ', '.join([f"{key.replace(' ', '_')} TEXT" for key in data[0].keys()])
            create_table_query = f"CREATE TABLE {modified_table_name} ({column_definitions}, load_dts TIMESTAMP)"
            cursor.execute(create_table_query)
            self.connection.commit()
            print(f"Table {table_name} created.")

        cursor.close()        

    def insert_data(self, data, table_name):
        cursor = self.connection.cursor()
        
        # Ensure the table exists
        self.ensure_table_exists(data, table_name)

        # Remove database part if present
        table_parts = table_name.split('.')
        modified_table_name = '.'.join(table_parts[-2:]) if len(table_parts) > 2 else table_name

         
        for record in data:

            # Sanitize column names
            sanitized_record = sanitize_column_names(record)
            columns = ', '.join(sanitized_record.keys())
            placeholders = ', '.join(['%s'] * len(sanitized_record))
            query = f"INSERT INTO {modified_table_name} ({columns}, load_dts) VALUES ({placeholders}, CURRENT_TIMESTAMP)"
            cursor.execute(query, list(sanitized_record.values()))
        self.connection.commit()

    def close_connection(self):
        if self.connection:
            self.connection.close()

    def insert_dataframe(self, dataframe, table_name):
        # Construct the connection string for SQLAlchemy engine
        engine_connection_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = sqlalchemy.create_engine(engine_connection_string)
        
        # Use 'to_sql' to insert the dataframe
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
