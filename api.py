import flask
#from flask import request, jsonify
from pandas import DataFrame
import json
from google.cloud import bigquery
from google.oauth2 import service_account

app = flask.Flask(__name__)
app.config["DEBUG"] = True

#FILL IN YOUR PATH TO THE 'Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/Users/seangoral/bq_api_test/venv/Benioff Ocean Initiative-454f666d1896.json'

credentials = service_account.Credentials.from_service_account_file(credentials_json)

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)

# http://127.0.0.1:5000/api
@app.route('/api', methods=['GET'])
def home():
    return '''<h1>WhaleSafe API</h1>
<p>A prototype API for AIS Message json and geojson.</p>'''

# http://127.0.0.1:5000/api/v1/geo
@app.route('/api/v1/geo', methods=['GET'])
def api_geo():
    query_job = client.query("""SELECT mmsi, timestamp, implied_speed_knots,
                             ST_ASGEOJSON(linestring) AS geom
                             FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments` 
                             WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);""")  
    records = [dict(row) for row in query_job]
    json_obj = json.dumps(str(records))
    result = json_obj.replace("\\", "") 
    return (result)

@app.route('/api/v1/stats', methods=['GET'])
def api_stats():
    query = """SELECT * FROM `benioff-ocean-initiative.whalesafe_ais.mmsi_cooperation_stats`;"""
    query_job = client.query(query)
    records = [dict(row) for row in query_job]
    json_obj = json.dumps(str(records))
    result = json_obj.replace("\\", "") 

    return(result)         

# This iis a super lazy function, but it spits out a json
@app.route('/api/v1/stats/mmsi', methods=['GET'])
def api_ship_stats():
    sql = """SELECT * FROM `benioff-ocean-initiative.whalesafe_ais.mmsi_cooperation_stats`;"""
    df = client.query(sql).to_dataframe()
    return(df.to_json( orient='records', lines=True))     
         

app.run()