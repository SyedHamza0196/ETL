from hubspot import HubSpotProcessor
from ga_4 import GA4Processor
from logger_config import logger
import yaml
import mysql.connector

with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

class Pipeline:
    def __init__(self):
        self.hubspot_processor = HubSpotProcessor()
        try:
            # extablish connection
            self.etl_con = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database="etl"
            )
            # set to always commit
            self.etl_con.autocommit = True

            # Create a cursor object
        except:
            print("DB Connection failed")
            return ("DB Connection failed") 
        
    def extract_all(self):
        self.hubspot_data = self.facebook_data = None
        
        try:
            hubspot_data = self.hubspot_processor.extract()
            logger.info("Data extracted from HubSpot successfully")
        except Exception as e:
            logger.error(f"Error extracting data from HubSpot: {str(e)}")
            hubspot_data = None
        
        return hubspot_data
            
        # try:
        #     ga4_data = self.ga4_processor.extract()
        #     logger.info("Data extracted from GA4 successfully")
        # except Exception as e:
        #     print(f"Error extracting data from GA4: {str(e)}")

        # return hubspot_data

    def transform_all(self, hubspot_data):
        try:
            transformed_hubspot_data = self.hubspot_processor.transform(hubspot_data) if hubspot_data else None
            logger.info("Data transformed successfully")
        except Exception as e:
            logger.error(f"Error transforming data from HubSpot: {str(e)}")
            transformed_hubspot_data = None

        return transformed_hubspot_data

    def load_all(self, transformed_hubspot_data):
        try:
            if transformed_hubspot_data:
                self.hubspot_processor.load(transformed_hubspot_data, self.etl_con)
                logger.info("Hubspot data loaded into SQL db successfully")
            else:
                logger.warning("No HubSpot data found to load")
        except Exception as e:
            logger.error(f"Error loading hubspot data: {str(e)}")

    def extract_transform_load_all(self):
        try:
            logger.info("Starting ETL process")
            hubspot_data = self.extract_all()
            transformed_hubspot_data = self.transform_all(hubspot_data)
            self.load_all(transformed_hubspot_data)
            logger.info("ETL process completed")
        except Exception as e:
            logger.error(f"Error during ETL process: {str(e)}")
