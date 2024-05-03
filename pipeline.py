from hubspot import HubSpotProcessor
from ga_4 import GA4Processor
from logger_config import logger
import yaml
import mysql.connector

with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

class Pipeline:
    def __init__(self):
        """
        Initializes the Pipeline object.

        Establishes a connection to the MySQL database.
        """
        self.hubspot_processor = HubSpotProcessor()
        self.ga4_processor = GA4Processor()
        
        try:
            # extablish connection
            self.etl_con = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database="etl"
            )
            # Create a cursor object
            db_cursor = self.etl_con.cursor()
            
            # set to always commit
            self.etl_con.autocommit = True
        except:
            print("DB Connection failed")
            return ("DB Connection failed") 
        
    def extract_all(self):
        """
        Extracts data from HubSpot and Google Analytics.

        Returns:
            tuple: A tuple containing extracted data from HubSpot and Google Analytics.
        """
        hubspot_extract_response = self.hubspot_processor.extract()
        if hubspot_extract_response['status'] == "fail":
            logger.info("Data extracted from Hubspot failed")
            hubspot_extract_response['data'] = None
        logger.info("Data extracted from Hubspot successfully")
            
        ga4_extract_response = self.ga4_processor.extract()
        if ga4_extract_response['status'] == "fail":
            logger.info(ga4_extract_response['message'])
            ga4_extract_response['data'] = None
        
        return hubspot_extract_response['data'], ga4_extract_response['data']ssSW

    def transform_all(self, hubspot_data, ga_data):
        """
        Transforms data from HubSpot and Google Analytics.

        Args:
            hubspot_data (dict): Data extracted from HubSpot.
            ga_data (dict): Data extracted from Google Analytics.

        Returns:
            tuple: A tuple containing transformed data from HubSpot and Google Analytics.
        """
        if hubspot_data:
            transformed_hubspot_response = self.hubspot_processor.transform(hubspot_data)
            if transformed_hubspot_response['status'] == "fail":                
                logger.error("Google Data transformation FAILED!")
                transformed_hubspot_response['data'] = None
            logger.error("Google Data transformation successful")
        
        if ga_data:
            transformed_ga_response = self.ga4_processor.transform(ga_data)
            if transformed_ga_response['status'] == "fail":                
                logger.error("Google Data transformation FAILED!")
                transformed_ga_response['data'] = None
            logger.error("Google Data transformation successful")
            
        return transformed_hubspot_response['data'], transformed_ga_response['data']
        # return None, transformed_ga_response['data']

    def load_all(self, transformed_hubspot_data, transformed_ga_data):
        """
        Loads transformed data into the MySQL database.

        Args:
            transformed_hubspot_data (list): Transformed data from HubSpot.
            transformed_ga_data (list): Transformed data from Google Analytics.

        Returns:
            dict: A dictionary containing load logs.
        """
        load_logs = {}
            
        if transformed_hubspot_data:
            hubspot_load_response = self.hubspot_processor.load(transformed_hubspot_data, self.etl_con)
            if not hubspot_load_response['status'] == "fail":
                logger.info("Hubspot loaded successfully")
                load_logs['hubspot'] = "SUCCESSFUL"
            else:
                logger.info("Hubspot loaded FAILED!")
                load_logs['hubspot'] = "FAILED!"
        
        if transformed_ga_data:
            ga_load_response = self.ga4_processor.load(transformed_ga_data, self.etl_con)
            if not ga_load_response['status'] == "fail":
                logger.info("Google analytics loading successfully")
                load_logs['ga'] = "SUCCESSFUL"
            else:
                logger.info("Google analytics loading FAILED!")
                load_logs['ga'] = "FAILED!"
        
        return load_logs
            

    def extract_transform_load_all(self):
        try:
            logger.info("Starting ETL process")
            hubspot_data, ga_data = self.extract_all()
            transformed_hubspot_data, transformed_ga_data = self.transform_all(hubspot_data, ga_data)
            self.load_all(transformed_hubspot_data, transformed_ga_data)
            logger.info("ETL process completed")
        except Exception as e:
            logger.error(f"Error during ETL process: {str(e)}")
