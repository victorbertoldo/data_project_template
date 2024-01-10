import psycopg2
import pymssql
import sqlalchemy
from config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_SERVER, POSTGRES_PORT, POSTGRES_DB, POSTGRES_DB_CONN
from config import MSSQL_SERVER, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE, MSSQL_PORT

from processors.transform import sanitize_column_names

class MSSQLDatabaseConnector:
    def __init__(self):
        """
        Initialize the object by creating a connection to the MSSQL database.

        Parameters:
        None

        Returns:
        None
        """
        self.connection = pymssql.connect(server=MSSQL_SERVER, user=MSSQL_USER, 
                                          password=MSSQL_PASSWORD, database=MSSQL_DATABASE, port=MSSQL_PORT)

    def fetch_data(self, query):
        """
        Fetches data from the database using the provided query.

        Parameters:
            query (str): The SQL query to fetch data from the database.

        Returns:
            list: A list of dictionaries representing the fetched data.
        """
        cursor = self.connection.cursor(as_dict=True)
        cursor.execute(query)
        return cursor.fetchall()   
        

class PostgresDatabaseConnector:
    def __init__(self):
        """
        Initializes a new instance of the class.

        Parameters:
            None

        Returns:
            None
        """
        self.connection = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_SERVER,
            port=POSTGRES_PORT,
            database=POSTGRES_DB_CONN
        )

    def close_connection(self):
        """
        Close the connection to the database.

        This function closes the connection to the database if it is open.

        Parameters:
            self (obj): The instance of the class.

        Returns:
            None
        """
        if self.connection:
            self.connection.close()

    def check_and_create_database(self, database_name, schema_name):
        """
        Check if a database exists and create it if it doesn't.
        
        Parameters:
            database_name (str): The name of the database to check and create.
            schema_name (str): The name of the schema to check and create.
        
        Returns:
            None
        """
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
        print(f"Database {database_name} not found. Creating database...")
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
        """
        Ensure the specified table exists in the database.

        Args:
            data (list[dict]): The data used to define the columns of the table.
            table_name (str): The name of the table to ensure exists.

        Returns:
            None
        """
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
        """
        Inserts data into the specified table.

        Args:
            data (list[dict]): The data to be inserted into the table.
            table_name (str): The name of the table.

        Returns:
            None
        """
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
        """
        Closes the connection to the database if it is currently open.

        This function checks if the `connection` attribute of the object is not None.
        If it is not None, it calls the `close()` method on the `connection` object
        to close the connection.

        Parameters:
            self (object): The object instance.

        Returns:
            None
        """
        if self.connection:
            self.connection.close()

    def insert_dataframe(self, dataframe, table_name):
        """
        Insert a Pandas DataFrame into a PostgreSQL table.

        Args:
            dataframe (pandas.DataFrame): The DataFrame to be inserted.
            table_name (str): The name of the table in which the DataFrame will be inserted.

        Returns:
            None
        """
        # Construct the connection string for SQLAlchemy engine
        engine_connection_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = sqlalchemy.create_engine(engine_connection_string)
        
        # Use 'to_sql' to insert the dataframe
        # TO A SPECIFIC SCHEMA
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False, schema='landing')  
