import pandas as pd
import json
# import geojson
import geopandas as gpd
# import shapely.wkt
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine
import csv

#FILL IN YOUR PATH TO THE 'Benioff Ocean Initiative-454f666d1896.json'
#credentials_json = '/Users/seangoral/bq_api_test/venv/Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/home/admin/Benioff Ocean Initiative-454f666d1896.json'

credentials = service_account.Credentials.from_service_account_file(credentials_json)

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)


import psycopg2
from postgis.psycopg import register

try:
  pg_conn = psycopg2.connect("dbname='gis' user='admin' port=5432 host=s4w-postgis password='whalestrike'")
  register(pg_conn)
except:
  print("I am unable to connect to the database")
  
cur=pg_conn.cursor()


engine = create_engine('postgresql+psycopg2://admin:whalestrike@s4w-postgis:5432/gis')


sql = """SELECT 
    mmsi,
    operator,
    DATE(timestamp) as day,
    timestamp, segment_time_minutes,
    distance_km, implied_speed_knots,
    linestring
    FROM `benioff-ocean-initiative.clustered_datasets.gfw_ihs_segments` 
    WHERE DATE(timestamp) > "2020-04-20" 
    """
lns_df = client.query(sql).to_dataframe()
lns_df.to_sql('lns', engine, if_exists='replace', index=False) #truncates the table


# get 3 days of data (timestamp_max = 2020-04-04 23:59:58+00:00)
sql = """
  SELECT
    mmsi, timestamp, lon, lat, speed_knots, implied_speed_knots, source
  FROM
    `benioff-ocean-initiative.clustered_datasets.gfw_data`
  WHERE 
    DATE(timestamp) > '2020-04-01'
  """
df = client.query(sql).to_dataframe()
df.to_csv('/home/admin/plumber-api/gfw_data_2020-04-02-to-04.csv')


#mmsi	timestamp	lon	lat	speed_knots	implied_speed_knots	source	X	imo_lr_ihs_no	name_of_ship	callsign	shiptype
#gfw_ihs_data



sql = """
  SELECT 
    mmsi, operator, day, COUNT(*) AS cnt_row
  FROM lns
  GROUP BY mmsi, operator, day
  ORDER BY cnt_row DESC LIMIT 100;
 """
rows_csv = '/home/admin/plumber-api/ship-day-count_top100.csv'
df = pd.read_sql(sql, pg_conn)
df.to_csv(rows_csv)
# max(cnt_row) = 806; mmsi = 367533290 AND operator = 'Cheramie Marine LLC' AND day = '2020-04-21'


# sql = """
#   ALTER TABLE test_lines ADD COLUMN IF NOT EXISTS geom geometry(MultiLineString, 4326); 
#   UPDATE test_lines SET geom = ST_MULTI(ST_LINEMERGE(ST_UNION(ST_GeomFromText(segs_wkt, 4326))));
#   """
sql = """
  DROP TABLE IF EXISTS segs;
  CREATE TABLE segs AS SELECT 
    mmsi, operator, day,
    ST_MULTI(ST_LINEMERGE(ST_UNION(ST_GeomFromText(seg_wkt, 4326)))) AS geom
  FROM 
    ( SELECT * 
      FROM lns
      WHERE 
        mmsi = 367533290 AND operator = 'Cheramie Marine LLC' AND day = '2020-04-21') AS lns
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
    geom_smpl015 = ST_SIMPLIFY(geom, tolerance = 0.015, preserveCollapsed = True)
  """
engine.execute(sql)

#sql = "SELECT MAX(day) AS day_max FROM segs;"
res = engine.execute(sql)
res.next()

sql = "SELECT MAX(timestamp) AS timestamp_max FROM clustered_datasets.gfw_data WHERE date(timestamp) > '2020-01-01';"
client.query(sql).to_dataframe()
# 2020-04-04 23:59:58+00:00

sql = """
 SELECT JSON_BUILD_OBJECT(
  'type', 'FeatureCollection',
  'features', JSON_AGG(mmsi, operator, day, ST_AsGeoJSON(geom_smpl015))::JSON)
 )
 FROM segs WHERE day > 2020-05-01;
 """"
 
sql = """
SELECT jsonb_build_object(
    'type',     'FeatureCollection',
    'features', jsonb_agg(features.feature)
)
FROM (
  SELECT jsonb_build_object(
    'type',       'Feature',
    'id',         id,
    'geometry',   ST_AsGeoJSON(geom)::jsonb,
    'properties', to_jsonb(inputs) - 'id' - 'geom'
  ) AS feature
  FROM (
    SELECT 
      mmsi, operator, day, mmsi || '_' || operator || '_' || day AS id, geom_smpl015 AS geom 
    FROM segs
    WHERE day > '2020-04-30') inputs) features;
 """


#res = engine.execute(sql)



outfile = open(rows_csv, 'wb')
outcsv = csv.writer(outfile)
records = res.all()
[outcsv.writerow([getattr(curr, column.name) for column in MyTable.__mapper__.columns]) for curr in records]
# or maybe use outcsv.writerows(records)

outfile.close()

outfile = open(rows_csv, 'wb')
outcsv = csv.writer(outfile)

cursor = con.execute('select * from mytable')

# dump column titles (optional)
outcsv.writerow(x[0] for x in cursor.description)
# dump rows
outcsv.writerows(cursor.fetchall())

outfile.close()

row = res.next()
row = res[0]

geojson = '/home/admin/plumber-api/res.geojson'
fh = open(geojson, 'w')
row.values()[0]
fh.write([0])
#fh.write(str(res))
#fh.write("\n")
fh.close()
json.dump(row.values()[0], geojson)

with open(geojson, 'w') as f:
    json.dump(row.values(), f)

writer = csv.writer(fh, delimiter=" ", escapechar=None, quoting=csv.QUOTE_NONE)
writer.writerow(row)

with fh:
  rdr = csv.reader(fh, delimiter=' ', quotechar='|')
  for row in spamreader:
      print(', '.join(row))

for row in res:
  write

fh.write(res.next())
outcsv = csv.writer(fh)

outcsv.writerow(result.keys())
outcsv.writerows(result)

fh.close

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

