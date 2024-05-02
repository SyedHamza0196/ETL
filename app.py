from flask import Flask
from etl import run_etl

app = Flask(__name__)

@app.route("/")
def home_page():
    print("APP HIME PAGE")
    return "APP HOME PAGE"

# Endpoint to trigger ETL process
@app.route('/run_etl')
def trigger_etl():
    result = run_etl()
    return result

if __name__ == '__main__':
    app.run(debug=True)