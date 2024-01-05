import pandas as pd
from config.settings import FILE_PATH

class FileConnector:
    def __init__(self):
        self.file_path = FILE_PATH

    def read_file(self):
        return pd.read_csv(self.file_path)
