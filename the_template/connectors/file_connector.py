import pandas as pd
from config import FILE_PATH

class FileConnector:
    def __init__(self):
        """
        Initializes a new instance of the class.

        Parameters:
            self: The object itself.

        Returns:
            None
        """
        self.file_path = FILE_PATH

    def read_file(self):
        """
        Reads a file and returns its content as a pandas DataFrame.

        Returns:
            pandas.DataFrame: The content of the file as a DataFrame.
        """
        return pd.read_csv(self.file_path)
