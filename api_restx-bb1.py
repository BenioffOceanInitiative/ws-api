from flask import Flask #, Blueprint
#from flask import request, jsonify
#from flask_restplus import Api, Resource
#from flask_restplus import Api
from flask_restx import Resource, Api
# https://flask-restx.readthedocs.io/
#werkzeug.cached_property = werkzeug.utils.cached_property
#from flask.ext.restplus import Api

import pandas as pd
import json
import geojson
from google.cloud import bigquery
from google.oauth2 import service_account
from werkzeug.contrib.fixers import ProxyFix

app = Flask(__name__)
#api = Api(app = app)
app.wsgi_app = ProxyFix(app.wsgi_app)
api = Api(
    app, version='0.1', title='WhaleSafe API', 
    description='stats and segments on for ship AIS data related to whale safety')

ns = api.namespace('whalesafe', description='whalesafe data')

todo = api.model('WhaleSafe', {
    'id': fields.Integer(readonly=True, description='The task unique identifier'),
    'task': fields.String(required=True, description='The task details')
})

app.config["DEBUG"] = True

#FILL IN YOUR PATH TO THE 'Benioff Ocean Initiative-454f666d1896.json'
#credentials_json = '/Users/seangoral/bq_api_test/venv/Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/home/admin/Benioff Ocean Initiative-454f666d1896.json'

credentials = service_account.Credentials.from_service_account_file(credentials_json)

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)

# http://127.0.0.1:5000/api

@api.route('/')
class WhaleSafeApi(Resource):

    #@app.route('/api', methods=['GET'])
    def home():
        return '''<h1>WhaleSafe API</h1>
    <p>A prototype API for AIS Message json and geojson.</p>'''
    
    # http://127.0.0.1:5000/api/v1/geo_1
    #geo = api.namespace('main', description='WhaleSafe API')
    #@app.route('/api/v1/geo', methods=['GET'])
    @api.doc('get geography')
    def api_geo():
        
        sql = ("""SELECT 
               ST_ASGEOJSON(linestring) as linestring
               FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments` 
               WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);""") 
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
        json_obj = json.dumps(str(records))
        result = json_obj.replace("\\", "") 
    
        return(result)  
        
    # http://127.0.0.1:5000/api/v1/geo_1
    #@app.route('/api/v1/geo_1', methods=['GET'])
    def api_geo_1():
        
        sql = ("""SELECT 
               CAST(mmsi AS STRING) AS mmsi, 
               CAST(timestamp AS STRING) AS timestamp, 
               CAST(lon AS STRING) AS lon,
               CAST(lat AS STRING) AS lat
               
               FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments` 
               WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);""") 
        df = client.query(sql).to_dataframe()
        
        def df_to_geojson(df, properties, lat='lat', lon='lon'):
            geojson = {'type':'FeatureCollection', 'features':[]}
            for _, row in df.iterrows():
                feature = {'type':'Feature',
                           'properties':{},
                           'geometry':{'type':'Point',
                                       'coordinates':[]}}
                feature['geometry']['coordinates'] = [row[lon],row[lat]]
                for prop in properties:
                    feature['properties'][prop] = row[prop]
                geojson['features'].append(feature)
            return geojson
        
        cols = ['mmsi', 'timestamp']
        geojson = df_to_geojson(df, cols)
        return(geojson)
    
    # http://127.0.0.1:5000/api/v1/geo_2
    #@app.route('/api/v1/geo_2', methods=['GET'])
    def api_geo_2():
        # Query BigQuery table
        sql = """SELECT 
            lon, lat, lead_lon, lead_lat, 
            CAST(mmsi AS FLOAT64) AS mmsi,
            operator AS operator,
            CAST(speed_knots AS FLOAT64) AS speed_knots, 
            CAST(implied_speed_knots AS FLOAT64) AS implied_speed_knots,
            CAST(calculated_knots AS FLOAT64) AS calculated_knots,
            FROM `benioff-ocean-initiative.scratch.gfw_ihs_segments_test` 
            WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);"""
    
        # Make into pandas dataframe
        df = client.query(sql).to_dataframe()
        
        # data to geojson function...
        def data2geojson(df):
            features = []
            insert_features = lambda X: features.append(
                    geojson.Feature(geometry=geojson.LineString(([X["lead_lon"], X["lead_lat"]],
                                                                 [X["lon"], X["lat"]])),
                                    properties=dict(mmsi=X["mmsi"],
                                                    operator=X["operator"],
                                                    speed_knots=X["speed_knots"],
                                                    implied_speed_knots=X["implied_speed_knots"],
                                                    calculated_knots=X["calculated_knots"])))
            df.apply(insert_features, axis=1)
            #with open('/Users/seangoral/bq_api_test/map1.geojson', 'w', encoding='utf8') as fp:
            geojson_obj = geojson.dumps(geojson.FeatureCollection(features, indent=2, sort_keys=True), sort_keys=True, ensure_ascii=False)
            return(geojson_obj)
        # provide columns maybe...   
        #col = ['lead_lat','lead_lon', 'lat', 'lon','mmsi','operator','speed_knots','implied_speed_knots','calculated_knots']
        # combine df and columns
        #data = pd.DataFrame(df, columns=col)
    
        results = data2geojson(df)
        return(results)
        
    #@app.route('/api/v1/stats', methods=['GET'])
    def api_stats():
        query = """SELECT * FROM `benioff-ocean-initiative.whalesafe_ais.mmsi_cooperation_stats`;"""
        query_job = client.query(query)
        records = [dict(row) for row in query_job]
        json_obj = json.dumps(str(records))
        result = json_obj.replace("\\", "") 
    
        return(result)         
    
    # This iis a super lazy function, but it spits out a json
        # http://127.0.0.1:5000/api/v1/stats/mmsi
    #@app.route('/api/v1/stats/mmsi', methods=['GET'])
    def api_ship_stats():
        sql = """SELECT * FROM `benioff-ocean-initiative.whalesafe_ais.mmsi_cooperation_stats`;"""
        df = client.query(sql).to_dataframe()
        return(df.to_json( orient='records', lines=True))     
             
    # This iis a super lazy function, but it spits out a json
        # http://127.0.0.1:5000/api/v1/stats/operator
    #@app.route('/api/v1/stats/operator', methods=['GET'])
    def api_operator_stats():
        sql = """SELECT * FROM `benioff-ocean-initiative.whalesafe_ais.operator_stats`;"""
        df = client.query(sql).to_dataframe()
        return(df.to_json( orient='records', lines=True))  

if __name__ == '__main__':
    app.run(debug=True)
