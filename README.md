# HubSpot Contacts and Deals ETL Process
This repository contains the code and documentation for an ETL (Extract, Transform, Load) process designed to fetch data from HubSpot's Contacts and Deals APIs, perform transformations on the data, and load it into a locally hosted MySQL server.

## Overview
The ETL process consists of the following steps:

1. Extraction: Data is fetched from HubSpot's Contacts and Deals APIs using an access token, which can be generated directly from the HubSpot settings, google analytics [sample data](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset), and facebook [sample data](https://data.world/datasets/facebook-ads)
2. Transformation: The extracted data is transformed to retain only the required columns. This step may include data cleaning, formatting, and any necessary adjustments to prepare the data for loading.
3. Load: The transformed data is loaded into a locally hosted MySQL server. This involves establishing a connection to the MySQL database and inserting the data into the appropriate tables.

## Usage
### Prerequisites
1. Python 3 latest version will work.
2. Required Python packages installed. You can install them using pip install -r requirements.txt. (Or use the venv environment)
3. Access token generated from HubSpot settings.
4. MYSQL server setup
5. Docker desktop
6. WSL
   
## Configuration
Before running the ETL process, ensure that you have configured the following settings:

1. HubSpot API Credentials: Obtain an access token from HubSpot's settings and update the config.yml ile with your HubSpot API key.
2. MySQL Database: Ensure that you have a MySQL database set up and running locally. Update the config.yml file with your MySQL database credentials.
3. Running the ETL Process
4. Clone this repository to your local machine:
```
foo@bar:~$ git clone https://github.com/SyedHamza0196/ETL.git
```
5. initiate the environment (or create your own env and install dependencies from the requirements.txt)
  ```
    .venv\Scripts\ativate
  ```
Navigate to the project directory where app.py resides

Run the ETL process:
1. Open a terminal in the directory where app.py resides
2. Run the flask server
```
foo@bar:~$ ETL-main/flask run
```
This command will initiate a server on local host 5000
On the server change the url to
```
http://127.0.0.1:5000/run_etl
```
This command will execute the entire ETL process, including extraction, transformation, and loading of data from hubspo.

## Files and Directory Structure
_No such directory structure is followed. All the files are in root directory_
1. config.py: Configuration file containing API credentials and database connection settings.
2. etl.py: Main script for executing the ETL process.
3. pipeline.py: Orchestrates the extract transform and load process.
4. hubspot.py: Module for extracting data from HubSpot's Contacts and Deals APIs.
5. ga_4.py: Module for extracting data from Google analytics 4.
6. facebook.py: Module for extracting data from facebook.
7. requirements.txt: List of Python packages required for the project.
8. README.md: Documentation file providing an overview of the project and usage instructions.
