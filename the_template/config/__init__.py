from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Accessing variables
DATABASE_URI = os.getenv("DATABASE_URI")
API_URL = os.getenv("API_URL")
FILE_PATH = os.getenv("FILE_PATH")
