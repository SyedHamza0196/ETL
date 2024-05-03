import pandas as pd
import json
from logger_config import logger

class GA4Processor:
    def extract(self):
        """
        Extract data from a JSON file.

        Returns:
            dict: A dictionary containing the status and extracted data.
        """
        filename
        filename = '../../Dataset/ga_20201101.json'
        try:
            with open(filename, 'r') as file:
                ga_json_data = json.load(file)
                return {"status": "success", "data": ga_json_data}
        except:
            return {"status": "fail", "message": "Google analytics extraction failed"}
            
    
    def transform(self, ga_json_data):
        """
        Transform Google Analytics data.

        Args:
            ga_json_data (dict): JSON data extracted from Google Analytics.

        Returns:
            dict: A dictionary containing the status and transformed dataframes.
        """
        try:
            df_original = pd.DataFrame(ga_json_data)
            # copy df_original to df and describe df
            df = df_original.copy()
            
            # List columns that have just NaN keep null value appearence threshold to 75%
            nan_threshold = 0.75
            nan_columns = df.columns[df.isna().mean() > nan_threshold]
            
            # remove nan_columns columns from df
            df.drop(columns=nan_columns, inplace=True)
            
            # List colums that have empty list or emty dict
            empty_list_columns = []
            empty_dict_columns = []

            for column in df.columns:
                if df[column].apply(lambda x: isinstance(x, list) and len(x) == 0).any():
                    empty_list_columns.append(column)
                if df[column].apply(lambda x: isinstance(x, dict) and len(x) == 0).any():
                    empty_dict_columns.append(column)

            # remove the empty list and empty dict columns if any
            df.drop(columns=empty_list_columns + empty_dict_columns, inplace=True)
            
            # remove columns with redundant vaues
            df.drop(columns=['user_pseudo_id', 'user_ltv', 'stream_id', 'event_bundle_sequence_id', 'privacy_info'], inplace=True)
            
            # remove ecommerce column from df all the key value pairs in it are null
            df.drop(columns=['ecommerce'], inplace=True)
            
            # Create dataframes which will be treated as look up tables in the db. Foriegn key df will be event_data
            event_params = pd.DataFrame({
                'event_date': df['event_date'],
                'event_params': df['event_params']
            })
                
            device = pd.DataFrame({
                'event_date': df['event_date'],
                'event_params': df['device']
            })
            
            geo = pd.DataFrame({
                'event_date': df['event_date'],
                'geo': df['geo']
            })
            
            traffic_source = pd.DataFrame({
                'event_date': df['event_date'],
                'traffic_source': df['traffic_source']
            })
            
            ga_dataframes = [df, event_params, device, geo, traffic_source] 
            return {"status": "success", "data": ga_dataframes}
        except:
            return {"status": "fail", "message": "Google analytics transformation failed"} 
    
    def load(self, ga_data_frames, etl_con):
        """
        Load dataframes into a MySQL database  called etl in table ga.

        Args:
            ga_data_frames (list): List of dataframes to be loaded.
            etl_con: Database connection object.

        Returns:
            dict: A dictionary containing the status and load logs.
        """
        ga_load_logs = {}
        db_cursor = etl_con.cursor()
        
        for df in ga_data_frames:
            try:
                if "event_timestamp" in df.columns:
                    values = df[["event_date", "event_timestamp", "event_name", "user_first_touch_timestamp", "platform"]].values.tolist()
                    sql = "INSERT INTO etl.ga (event_date, event_timestamp, event_name, user_first_touch_timestamp, platform) VALUES (%s, %s, %s, %s, %s)"
                    db_cursor.executemany(sql, values)
                ga_load_logs["ga"] = "success"
            except:
                ga_load_logs["ga"] = "fail"
                
            try:
                if "event_params" in df.columns:
                    values = df[["event_date", "event_params"]].values.tolist()
                    sql = "INSERT INTO etl.ga (event_date, event_params) VALUES (%s, %s)"
                    db_cursor.executemany(sql, values)
                ga_load_logs["event_params"] = "success"
            except:
                ga_load_logs["event_params"] = "fail"
                
            try:
                if "device" in df.columns:
                    values = df[["event_date", "device"]].values.tolist()
                    sql = "INSERT INTO etl.ga (event_date, device) VALUES (%s, %s)"
                    db_cursor.executemany(sql, values)
                ga_load_logs["device"] = "success"
            except:
                ga_load_logs["device"] = "fail"
                ga_load_logs["event_params"] = "fail"
                
            try:
                if "geo" in df.columns:
                    values = df[["event_date", "geo"]].values.tolist()
                    sql = "INSERT INTO etl.ga (event_date, geo) VALUES (%s, %s)"
                    db_cursor.executemany(sql, values)
                ga_load_logs["geo"] = "success"
            except:
                ga_load_logs["geo"] = "fail"
                ga_load_logs["event_params"] = "fail"
                
            try:
                if "traffic_source" in df.columns:
                    values = df[["event_date", "traffic_source"]].values.tolist()
                    sql = "INSERT INTO etl.ga (event_date, traffic_source) VALUES (%s, %s)"
                    db_cursor.executemany(sql, values)
                ga_load_logs["traffic_source"] = "success"
            except:
                ga_load_logs["traffic_source"] = "fail"
        
        
        all_fail = all(value == 'fail' for value in ga_load_logs.values())
        if all_fail:
            return {"status": "fail", "data": ga_load_logs}    
        return {"status": "Sucessful", "data": ga_load_logs}    
