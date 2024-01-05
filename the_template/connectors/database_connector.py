import psycopg2
import pyodbc
import sqlalchemy
from config import MSSQL_DATABASE_URI, POSTGRES_DATABASE_URI

class MSSQLDatabaseConnector:
    def __init__(self):
        self.connection = pyodbc.connect(MSSQL_DATABASE_URI)

    def fetch_data(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return data

class PostgresDatabaseConnector:
    def __init__(self):
        self.connection = psycopg2.connect(POSTGRES_DATABASE_URI)
        self.connection.autocommit = True

    def check_and_create_database(self, database_name, schema_name):
            # Check and create database
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            if not cursor.fetchone():
                cursor.execute(f"CREATE DATABASE {database_name}")

            # Close the current connection and connect to the new database
            cursor.close()
            self.connection.close()
            self.connection = psycopg2.connect(POSTGRES_DATABASE_URI + f" dbname={database_name}")
            self.connection.autocommit = True

            # Check and create schema
            cursor = self.connection.cursor()
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema_name,))
            if not cursor.fetchone():
                cursor.execute(f"CREATE SCHEMA {schema_name}")

            cursor.close()

    def insert_data(self, data, table_name):
        cursor = self.connection.cursor()
        for record in data:
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['%s'] * len(record))
            query = f"INSERT INTO {table_name} ({columns}, load_dts) VALUES ({placeholders}, CURRENT_TIMESTAMP)"
            cursor.execute(query, list(record.values()))
        self.connection.commit()

    def close_connection(self):
        if self.connection:
            self.connection.close()

    def insert_dataframe(self, dataframe, table_name):
        engine = sqlalchemy.create_engine(POSTGRES_DATABASE_URI)
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)            
