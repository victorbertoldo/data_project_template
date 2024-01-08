import requests

class APIConnector:
    def __init__(self, url):
        """
        Initializes an instance of the class with the given URL.

        Parameters:
            url (str): The URL to be assigned to the `url` attribute.

        Returns:
            None
        """
        self.url = url

    def fetch_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
