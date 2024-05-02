import requests
import yaml
import json
import pandas as pd
# import mysql.connector
from logger_config import logger


with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

class HubSpotProcessor:
    def extract(self):
        # Extract contact data from HubSpot
        hubspot_data = {}
        hubspot_data['CONTACT'] = self.extactContacts()
        hubspot_data['DEAL'] = self.extactDeals()

        return hubspot_data
    
    def transform(self, hubspot_data):
        hubspot_data_frames = []
        objects = ['CONTACT','DEAL']
        for object in objects:
            if object == "CONTACT":
                hubspot_contact_df = pd.json_normalize(hubspot_data[object])
                # remove unnecessary columns
                # Keep only the 'createdate' column
                hubspot_contact_df = hubspot_contact_df[['properties.createdate']]
                # Rename the column to 'createdate'
                hubspot_contact_df = hubspot_contact_df.rename(columns={'properties.createdate': 'createdate'})
                hubspot_data_frames.append(hubspot_contact_df)
            
            if object == "DEAL":
                # Flatten the 'properties' dictionary
                for i, deal in enumerate(hubspot_data[object]):
                    # Remove "properties" key and merge its contents with the outer dictionary
                    hubspot_data[object][i].update(deal.pop("properties"))

                # Convert to DataFrame
                hubspot_deal_df = pd.DataFrame(hubspot_data[object])
                req_columns = ['amount', 'closedate', 'createdate', 'dealstage']
                hubspot_deal_df = hubspot_deal_df[req_columns]
                hubspot_data_frames.append(hubspot_deal_df)
        
        return hubspot_data_frames
    
    def load(self, hubspot_data_frames, etl_con):
        db_cursor = etl_con.cursor()     
        hubspot_contact_df = pd.DataFrame()
        hubspot_deal_df = pd.DataFrame()
        
        for df in hubspot_data_frames:
            if len(df.columns) == 1 and 'createdate' in df.columns:
                hubspot_contact_df = df
            else:
                hubspot_deal_df = df
        
        # Insert data into tables
        logger.info(hubspot_deal_df.columns.to_list())
        for row in hubspot_deal_df.itertuples():
            logger.info("[]\n[]\n[]\n[]\n")
            logger.info(row)
            logger.info("[]\n[]\n[]\n[]\n")
            sql = f"INSERT INTO etl.hubspot_deal(amount, closedate, createdate, dealstage) VALUES('{int(row.amount) if int(row.amount) else 0}', '{row.closedate}', '{row.createdate}', '{row.dealstage}')"
            db_cursor.execute(sql)

        # for row in hubspot_contact_df.itertuples():
        #     sql = f"INSERT INTO etl.hubspot_contact(createdate) VALUES('{row.createdate}')"
        #     db_cursor.execute(sql)
            
        # return({"status": "success", "message": "Hubspot data loaded in the DB"})
    
    def extactContacts(self):
        # WILL EXTRACT CONTACTS
        url = config['HUBSPOT']['base_url'] + config['HUBSPOT']['contact_api']
        headers = {'Authorization': 'Bearer ' + config['HUBSPOT']['access_token'], 'Content-Type': 'application/json'}

        try:
            response = requests.request("GET", url, headers=headers)
            json_response = json.loads(response.text)['results']
            print("Hubspot contact extraction is successful. Found {} contacts".format(len(json_response)))
            if (json_response and len(json_response)):
                return json_response
            else:
                return {}
        except Exception as e:
            print(f"Error during extrrracting hubspot contacts: {str(e)}")
            return {}
        
    def extactDeals(self):
        # WILL EXTRACT DEALS
        url = config['HUBSPOT']['base_url'] + config['HUBSPOT']['deal_api']
        headers = {'Authorization': 'Bearer ' + config['HUBSPOT']['access_token'], 'Content-Type': 'application/json'}

        
        try:
            response = requests.request("GET", url, headers=headers)
            json_response = json.loads(response.text)['results']
            print("Hubspot contact extraction is successful. Found {} deals".format(len(json_response)))
            if (json_response and len(json_response)):
                return json_response
            else:
                return {}
        except Exception as e:
            print(f"Error during extrrracting hubspot deals: {str(e)}")
            return {}