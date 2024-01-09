import os
import pandas as pd
from config import FILE_PATH
from utils.cli import print_success, print_failure, print_info, print_warning, progress_bar

class FileConnector:
    def __init__(self):
        """
        Initializes a new instance of the class.
        """
        self.directory_path = FILE_PATH

    def process_files(self):
        """
        Process the files in the specified directory and return a list of processed files.

        Returns:
            list: A list of tuples containing the processed dataframes and their corresponding table names.
        """
        processed_files = []
        for filename in os.listdir(self.directory_path):
            print_info(f"Starting to process the file:{filename}", highlight=filename)
            progress_bar(f"Processing {filename}", total_steps=10)

            try:

                file_path = os.path.join(self.directory_path, filename)
                table_name = f"file_{os.path.splitext(filename)[0]}"  # table name format: "file_{file_name}"
                dataframe = None

                if filename.endswith(('.csv', '.CSV')):
                    dataframe = self.read_csv(file_path)
                elif filename.endswith(('.xls', '.xlsx')):
                    dataframe = self.read_excel(file_path)
                elif filename.endswith('.json'):
                    dataframe = self.read_json(file_path)

                if dataframe is not None:
                    processed_files.append((dataframe, table_name))
                    print_success("Success!")
                else:
                    print_warning(f"The dataframe from table: {table_name} is empty!", highlight="empty")

            except Exception as e:
                print_failure(f"Failed! Error: {e}", highlight="Error")
                print_failure("Task failed due to an error.", highlight="failed")
            
            print_info("Processing the next item.", highlight="next")

        print_success("Task `load files` completed successfully!", highlight="successfully")

        return processed_files
        

            # Additional processing for the dataframe can be done here

    def read_csv(self, file_path):
        """
        Read a CSV file and return a pandas DataFrame.

        Parameters:
            file_path (str): The path to the CSV file.

        Returns:
            pd.DataFrame: The DataFrame containing the data from the CSV file.
            None: If there was an error reading the CSV file.
        """
        try:
            return pd.read_csv(file_path, sep=";", index_col=False, encoding= 'ISO-8859-1')
        except Exception as e:
            print(f"Error reading CSV file {file_path}: {e}")
            return None

    def read_excel(self, file_path):
        """
        Read an Excel file and return its contents as a pandas DataFrame.

        Parameters:
            file_path (str): The path to the Excel file.

        Returns:
            pd.DataFrame or None: The contents of the Excel file as a pandas DataFrame if successful,
                                  None if there was an error reading the file.
        """
        try:
            return pd.read_excel(file_path, index_col=False)
        except Exception as e:
            print(f"Error reading Excel file {file_path}: {e}")
            return None

    def read_json(self, file_path):
        """
        Reads a JSON file and returns a pandas DataFrame.

        Parameters:
            file_path (str): The path to the JSON file.

        Returns:
            df (pandas.DataFrame): The DataFrame containing the data from the JSON file.
                Returns None if there is an error reading the file.
        """
        try:
            df = pd.read_json(file_path,  orient='records', encoding= 'ISO-8859-1')
            # Additional processing for JSON files can be done here
            return df
        except Exception as e:
            print(f"Error reading JSON file {file_path}: {e}")
            return None
