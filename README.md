# HubSpot Contacts and Deals ETL Process
This repository contains the code and documentation for an ETL (Extract, Transform, Load) process designed to fetch data from HubSpot's Contacts and Deals APIs, perform transformations on the data, and load it into a locally hosted MySQL server.

## Overview
The ETL process consists of the following steps:

1. Extraction: Data is fetched from HubSpot's Contacts and Deals APIs using an access token, which can be generated directly from the HubSpot settings, google analytics [sample data](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset), and facebook [sample data](https://data.world/datasets/facebook-ads)
2. Transformation: The extracted data is transformed to retain only the required columns. This step may include data cleaning, formatting, and any necessary adjustments to prepare the data for loading.
3. Load: The transformed data is loaded into a locally hosted MySQL server. This involves establishing a connection to the MySQL database and inserting the data into the appropriate tables.
4. Apache Airflow: Setup apache airflow to automate the processes mentioned above

## Usage
### Prerequisites Apache Airflow
1. Install WSL
2. Run bellow bash commands to install python 3 and pip.
   ```
   sudo apt install python3
   sudo apt install python3-pip
   ```
3. For Airflow to work you need to give it a folder where it can install and store its files.
   ```
   export AIRFLOW_HOME=~/apache-airflow
   ```
4. Run bellow bash commands to install Airflo using the constraint file
   ```
   AIRFLOW_VERSION=2.4.3
   PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
   CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
   pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
   ```
5. Try running `airflow` in the terminal now. In case that doesn’t work, because the command can’t be found, you will first need to add airflow to your PATH variable by running the command below
   ```
   PATH="$PATH:$HOME/.local/bin"
   ```
4. Install the latest version of Airflow from [PyPi](https://pypi.org/project/apache-airflow/)
5. Run the bash command bellow to start all aitflow components simultaneously.
   ```
   airflow standalone
   ```
6. As an output you should receive the logs followed by something similar to the output below. (This should take around 20–60 seconds.)
   ```
   standalone | Airflow is ready
   standalone | Login with username: admin  password: *****************
   ```
7. Access the airflow interface on `localhost` port `8080`
8. create a DAG (Directed Acyclic Graph) file. It can be a python script. You can also create it using vim
9. You can now run this file by running python3 `<fileame>.py`. You should then be able to see the DAG in the web interface.
### Prerequisites Pipline Code
1. Python 3 latest version will work.
2. Clone this repository to your local machine:
```
foo@bar:~$ git clone https://github.com/SyedHamza0196/ETL.git
```
3.Create and activate a python environment called venv
   ```
   python3 -m venv venv
   venv\Scripts\activate
   ```
4. Required Python packages installed. You can install them using pip install -r requirements.txt.
5. Access token generated from HubSpot settings.
6. MYSQL server setup. You can use mysql workbench.

### Prerequisites Destination Data Base
1. This project uses MySQL Workbench as a destination source
2. Intall MySQL workbench from [here](https://dev.mysql.com/downloads/workbench/)
3. Open workbench and and create the required databas and tables
4. This project uses 1 database called `etl` and 7 tables
   -device (GA look up table)
   -event_params (GA look up table)
   -ga (Google analitcs data)
   -geo (GA look up table)
   -hubspot_contact (Hubspot contact api data)
   -hubspot_deal (Hubspot deal api data)
   -traffic_source (GA look up table)
## Configuration
Before running the ETL process, ensure that you have configured the following settings:

1. HubSpot API Credentials: Obtain an access token from HubSpot's settings and update the config.yml ile with your HubSpot API key.
2. MySQL Database: Ensure that you have a MySQL database set up and running locally. Update the config.yml file with your MySQL database credentials.
3. Running the ETL Process
Navigate to the project directory where app.py resides

Run the ETL process:
1. Open a terminal in the directory where app.py resides
2. Run the flask server
```
foo@bar:~$ ETL-main/flask --debug run
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
5. hubspot.py: Module for extracting data from HubSpot's Contacts and Deals APIs.
6. ga_4.py: Module for extracting data from Google analytics 4.
7. facebook.py: Module for extracting data from facebook.
8. requirements.txt: List of Python packages required for the project.
9. README.md: Documentation file providing an overview of the project and usage instructions.
