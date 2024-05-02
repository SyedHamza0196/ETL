from flask import jsonify
from pipeline import Pipeline

def run_etl():
    try:
        # ETL process
        pipeline = Pipeline()
        pipeline.extract_transform_load_all()
        return "ETL process completedd"
        # return jsonify({"status": "success", "message": "ETL process completed"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
