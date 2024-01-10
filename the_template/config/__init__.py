from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Accessing variables
MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_USER = os.getenv("MSSQL_USER")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_PORT = os.getenv("MSSQL_PORT")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_SERVER = os.getenv("POSTGRES_SERVER")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_DB_CONN = os.getenv("POSTGRES_DB_CONN")

API_URL = os.getenv("API_URL")
FILE_PATH = os.getenv("FILE_PATH")
