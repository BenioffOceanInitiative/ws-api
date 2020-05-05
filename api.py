from flask import Flask, Response
import pandas as pd
import json
import geojson
import geopandas as gpd
import shapely.wkt
from google.cloud import bigquery
from google.oauth2 import service_account
from flask_cors import CORS, cross_origin

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
#app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

#FILL IN YOUR PATH TO THE 'Benioff Ocean Initiative-454f666d1896.json'
#credentials_json = '/Users/seangoral/bq_api_test/venv/Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/home/admin/Benioff Ocean Initiative-454f666d1896.json'

credentials = service_account.Credentials.from_service_account_file(credentials_json)

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)

# http://127.0.0.1:5000/api
@app.route('/api', methods=['GET'])
@cross_origin()
def home():
    return '''<h1>WhaleSafe API</h1>
<p>A prototype API for AIS Message json and geojson.</p>
<a href="http://api.ships4whales.org/api/v1/stats/mmsi.json"</a>
<p>http://api.ships4whales.org/api/v1/stats/mmsi.json </p>
<a href="http://api.ships4whales.org/api/v1/stats/operator.json"</a>
<p> http://api.ships4whales.org/api/v1/stats/operator.json </p>
<a href="http://api.ships4whales.org/api/v1/geo_simp.json "</a>
<p> http://api.ships4whales.org/api/v1/geo_simp.json </p> 
'''
    
# http://127.0.0.1:5000/api/v1/geo_1
@app.route('/api/v1/geo_1', methods=['GET'])
@cross_origin()
def api_geo_1():
    
    sql = ("""SELECT 
        lon, lat, lead_lon, lead_lat, 
        CAST(CAST(timestamp AS DATE) as string) as date,
        UNIX_SECONDS(timestamp) as unix_secs,
        CAST(mmsi AS STRING) AS mmsi,
        operator AS operator,
        CAST(calculated_knots AS FLOAT64) AS calculated_knots,
        FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments`  
        WHERE DATE(timestamp) >= DATE(2020-04-23'')""") 
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
    return Response(geojson, mimetype='application/json')

# http://127.0.0.1:5000/api/v1/geo.json
@app.route('/api/v1/geo.json', methods=['GET'])
@cross_origin()
def api_geo_2():
    # Query BigQuery table
    sql = """SELECT 
        lon, lat, lead_lon, lead_lat, 
        CAST(CAST(timestamp AS DATE) as string) as date,
        UNIX_SECONDS(timestamp) as unix_secs,
        CAST(mmsi AS STRING) AS mmsi,
        operator AS operator,
        CAST(calculated_knots AS FLOAT64) AS calculated_knots,
        FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments`  
        WHERE timestamp BETWEEN '2020-04-16' AND '2020-04-17';"""

    # Make into pandas dataframe
    df = client.query(sql).to_dataframe()
    
    # data to geojson function...
    def data2geojson(df):
        features = []
        insert_features = lambda X: features.append(
                geojson.Feature(geometry=geojson.LineString(([X["lead_lon"], X["lead_lat"], X["calculated_knots"], X["unix_secs"]],
                                                             [X["lon"], X["lat"], X["calculated_knots"], X["unix_secs"]])),
                                properties=dict(date=X["date"],
                                                mmsi=X["mmsi"],
                                                operator=X["operator"]
                                                )))
        df.apply(insert_features, axis=1)
        #with open('/Users/seangoral/bq_api_test/map1.geojson', 'w', encoding='utf8') as fp:
        geojson_obj = geojson.dumps(geojson.FeatureCollection(features, indent=2, sort_keys=True), sort_keys=True, ensure_ascii=False)
        return(geojson_obj)
    # provide columns maybe...   
    #col = ['lead_lat','lead_lon', 'lat', 'lon','mmsi','operator','speed_knots','implied_speed_knots','calculated_knots']
    # combine df and columns
    #data = pd.DataFrame(df, columns=col)

    results = data2geojson(df)
    return Response(results, mimetype='application/json')
    
@app.route('/api/v1/stats', methods=['GET'])
@cross_origin()
def api_stats():
    query = """SELECT * FROM `benioff-ocean-initiative.whalesafe_ais.mmsi_cooperation_stats`;"""
    query_job = client.query(query)
    records = [dict(row) for row in query_job]
    json_obj = json.dumps(str(records))
    result = json_obj.replace("\\", "") 

    return Response(result, mimetype='application/json')         

# This iis a super lazy function, but it spits out a json
    # http://127.0.0.1:5000/api/v1/stats/mmsi.json
@app.route('/api/v1/stats/mmsi.json', methods=['GET'])
@cross_origin()
def api_ship_stats():
    sql = """SELECT * FROM `benioff-ocean-initiative.whalesafe_ais.mmsi_cooperation_stats`;"""
    df = client.query(sql).to_dataframe()
    json_obj = df.to_json( orient='records', lines=True)
    return Response(json_obj, mimetype='application/json')     
         
# This iis a super lazy function, but it spits out a json
    # http://127.0.0.1:5000/api/v1/stats/operator.json
@app.route('/api/v1/stats/operator.json', methods=['GET'])
@cross_origin()
def api_operator_stats():
    sql = """SELECT * FROM `benioff-ocean-initiative.whalesafe_ais.operator_stats`;"""
    df = client.query(sql).to_dataframe()
    json_obj = df.to_json( orient='records', lines=True)
    return Response(json_obj, mimetype='application/json')  

# http://127.0.0.1:5000/api/v1/geo_1.json
@app.route('/api/v1/geo_1.json', methods=['GET'])
@cross_origin()
def api_geojson_out():
    sql = """SELECT 
        mmsi,
        DATE(timestamp) as date,
        ( ST_UNION_AGG (linestring)) AS linestring
        FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments` 
        WHERE DATE(timestamp) > "2020-04-20" 
        group by mmsi, date
        """
    df = client.query(sql).to_dataframe()
    geometry = df['linestring'].map(shapely.wkt.loads)
    df = df.drop('linestring', axis=1)
    crs = {'init': 'epsg:4326'}
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    
    gdf['date'] = pd.to_datetime(gdf['date'], format='%Y-%m-%d')
    gdf['date'] = gdf['date'].dt.strftime('%Y-%m-%d')
        
    json_obj = json.dumps(shapely.geometry.mapping(gdf))
    
    return Response(json_obj, mimetype='application/json')

# http://127.0.0.1:5000/api/v1/geo_simplify.json
@app.route('/api/v1/geo_simplify.json', methods=['GET'])
@cross_origin()
def api_geojson_simplify():
    sql = """SELECT 
        mmsi,
        DATE(timestamp) as date,
        ( ST_UNION_AGG (linestring)) AS linestring
        FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments` 
        WHERE DATE(timestamp) > "2020-04-20" 
        group by mmsi, date
        """
    df = client.query(sql).to_dataframe()
    geometry = df['linestring'].map(shapely.wkt.loads)
    df = df.drop('linestring', axis=1)
    crs = {'init': 'epsg:4326'}
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    
    gdf_simplified = gdf
    gdf_simplified["geometry"] = gdf.geometry.simplify(tolerance=0.015, preserve_topology=True)
    
    gdf_simplified['date'] = pd.to_datetime(gdf_simplified['date'], format='%Y-%m-%d')
    gdf_simplified['date'] = gdf_simplified['date'].dt.strftime('%Y-%m-%d')
        
    json_obj = json.dumps(shapely.geometry.mapping(gdf_simplified))
    
    return Response(json_obj, mimetype='application/json')

    # http://127.0.0.1:5000/api/v1/geo_simp.json
@app.route('/api/v1/geo_simp.json', methods=['GET'])
@cross_origin()
def api_geojson_simp():
    sql = """SELECT  
           CAST(mmsi AS STRING) AS mmsi, 
           CAST(operator AS STRING) AS operator, 
           CAST(avg_speed_knots AS STRING) AS avg_speed_knots, 
           CAST(avg_implied_speed_knots AS STRING) AS avg_implied_speed_knots,
           CAST(timestamp AS STRING) AS timestamp,
           CAST(TIMESTAMP_SECONDS(timestamp) AS STRING) AS date,
           CAST(wkt AS STRING) AS wkt 
           FROM  `benioff-ocean-initiative.scratch.gfw_ihs_simple_segs`
           where TIMESTAMP_SECONDS(timestamp) >= '2020-04-01';"""
    
    df = client.query(sql).to_dataframe()
    geometry = df['wkt'].map(shapely.wkt.loads)
    df = df.drop('wkt', axis=1)
    crs = {'init': 'epsg:4326'}
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    json_obj = json.dumps(shapely.geometry.mapping(gdf), indent=2)
    
    return Response(json_obj, mimetype='application/json')

    
#app.run()
