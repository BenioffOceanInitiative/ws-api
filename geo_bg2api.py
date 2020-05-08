import pandas as pd
import json
# import geojson
import geopandas as gpd
# import shapely.wkt
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine

#FILL IN YOUR PATH TO THE 'Benioff Ocean Initiative-454f666d1896.json'
#credentials_json = '/Users/seangoral/bq_api_test/venv/Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/home/admin/Benioff Ocean Initiative-454f666d1896.json'

credentials = service_account.Credentials.from_service_account_file(credentials_json)

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)


import psycopg2
from postgis.psycopg import register

try:
  pg_conn = psycopg2.connect("dbname='gis' user='admin' port=5432 host=s4w-postgis password='S3cret!'")
  register(pg_conn)
  
except:
  print "I am unable to connect to the database"
cur=conn.cursor()


engine = create_engine('postgresql+psycopg2://admin:whalestrike@s4w-postgis:5432/gis')


sql = """SELECT 
    mmsi,
    operator,
    DATE(timestamp) as day,
    linestring AS seg_wkt
    FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments` 
    WHERE DATE(timestamp) > "2020-04-20" 
    """
lns_df = client.query(sql).to_dataframe()
lns_df.to_sql('lns', engine, if_exists='replace', index=False) #truncates the table


# sql = """
#   ALTER TABLE test_lines ADD COLUMN IF NOT EXISTS geom geometry(MultiLineString, 4326); 
#   UPDATE test_lines SET geom = ST_MULTI(ST_LINEMERGE(ST_UNION(ST_GeomFromText(segs_wkt, 4326))));
#   """
sql = """
  CREATE TABLE segs AS SELECT 
    mmsi, operator, day,
    ST_MULTI(ST_LINEMERGE(ST_UNION(ST_GeomFromText(seg_wkt, 4326)))) AS geom
  FROM lns
  GROUP BY
    mmsi, operator, day
  """
engine.execute(sql)

# sql = """
#   CREATE TABLE segs_smpl015 AS SELECT 
#     mmsi, operator, day,
#     ST_SIMPLIFY(geom, 0.015) AS geom
#   FROM segs
#   """
sql = """
  ALTER TABLE segs ADD COLUMN IF NOT EXISTS geom_smpl015 geometry(MultiLineString, 4326); 
  UPDATE segs SET
    geom_smpl015 = ST_SIMPLIFY(geom, 0.015) AS geom
  """
engine.execute(sql)

;

ST_Multi(

(SELECT ST_Intersects(ocean_shapefile.geom, whale_alert.geom) FROM ocean_shapefile);'

engine.execute(query)

engine.execute()



geometry = df['linestring'].map(shapely.wkt.loads)
df = df.drop('linestring', axis=1)
crs = {'init': 'epsg:4326'}
gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

gdf_simplified = gdf
gdf_simplified["geometry"] = gdf.geometry.simplify(tolerance=0.015, preserve_topology=True)

gdf_simplified['date'] = pd.to_datetime(gdf_simplified['date'], format='%Y-%m-%d')
gdf_simplified['date'] = gdf_simplified['date'].dt.strftime('%Y-%m-%d')
    
json_obj = json.dumps(shapely.geometry.mapping(gdf_simplified))

