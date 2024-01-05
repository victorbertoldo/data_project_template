#!/bin/bash

# Check if a project name was provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <project_name>"
    exit 1
fi

# Create the project directory
PROJECT_NAME=$1
mkdir $PROJECT_NAME
cd $PROJECT_NAME

# Create the directory structure
mkdir config connectors processors utils tests

# Create empty Python files in each directory
touch config/__init__.py config/settings.py
touch connectors/__init__.py connectors/database_connector.py connectors/file_connector.py connectors/api_connector.py
touch processors/__init__.py processors/data_processor.py processors/transform.py
touch utils/__init__.py utils/logger.py
touch tests/__init__.py tests/test_database_connector.py tests/test_file_connector.py tests/test_api_connector.py

# Create main.py and requirements.txt at the root of the project
touch main.py
touch requirements.txt

echo "Project $PROJECT_NAME created successfully."
