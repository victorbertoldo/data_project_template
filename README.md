# The Data Project Template

To init the project, first you'll have to run the command `chmod +x init_project.sh` to ensure that the script will create the project structure.

The structure of the project is just a skelethon, but don't worrie, go to `the_template` dir and see an example of a data engineering project.

This is the Project Structure:

```shell
my_project/
│
├── .env                  # Environment variables (not tracked by Git)
│
├── config/               # Configuration initialization
│   └── __init__.py
│
├── connectors/           # Modules to connect to various data sources
│   ├── __init__.py
│   ├── database_connector.py
│   ├── file_connector.py
│   └── api_connector.py
│
├── processors/           # Data processing modules
│   ├── __init__.py
│   ├── data_processor.py
│   └── transform.py
│
├── utils/                # Utility functions and classes
│   ├── __init__.py
│   └── logger.py
│
├── tests/                # Unit and integration tests
│   ├── __init__.py
│   ├── test_database_connector.py
│   ├── test_file_connector.py
│   └── test_api_connector.py
│
├── main.py               # Main script to run the data ingestion processes
├── pyproject.toml        # Poetry dependency file
└── README.md

```

To generate it structure you need to run this:

```shell
./init_project.sh <project_name>
```
