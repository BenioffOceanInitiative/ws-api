import pandas as pd
from google.cloud  import bigquery
from google.oauth2 import service_account
from sqlalchemy    import create_engine

# bigquery connection
project_id       = 'benioff-ocean-initiative'
credentials_json = '/home/admin/Benioff Ocean Initiative-454f666d1896.json'
credentials      = service_account.Credentials.from_service_account_file(credentials_json)
bq_client        = bigquery.Client(credentials=credentials, project=project_id)

# bq to df
sql = """
  SELECT * EXCEPT (geom), ST_ASTEXT(geom) AS geom_txt 
  FROM 
  `benioff-ocean-initiative.test.r_copy_cat_sample` 
  WHERE date > "2019-01-01";"""
df = bq_client.query(sql).to_dataframe()
print(df.info(memory_usage='deep'))
df
# PROBLEM: seeing mixed geometries: multipoint, point, linestring 
#           mmsi  ...                                           geom_txt
# 0    367104060  ...  MULTIPOINT(-118.270773333333 33.7350133333333,...
# 1    367104060  ...          POINT(-118.270666666667 33.7348533333333)
# 2    367104060  ...          POINT(-118.116906666667 33.5637066666667)
# 3    367104060  ...  MULTIPOINT(-118.270773333333 33.7350133333333,...
# 4    367104060  ...  MULTIPOINT(-118.270773333333 33.7350133333333,...
# ..         ...  ...                                                ...
# 481  303031000  ...  LINESTRING(-119.9471666667 33.7475833333, -119...
# 482  303031000  ...  LINESTRING(-119.958773333333 33.7483733333333,...
# 483  303031000  ...  LINESTRING(-119.973226666667 33.7493333333333,...
# 484  303031000  ...  LINESTRING(-119.9926833333 33.7507666667, -120...
# 485  303031000  ...  LINESTRING(-120.76585 33.8714, -120.7789866666...

# postgres connection
with open('/home/admin/ws_admin_pass.txt') as f:
    passwd = f.read().splitlines()[0]  
pg_engine = create_engine('postgresql+psycopg2://admin:' + passwd + '@ws-postgis:5432/gis')
pg_con    = pg_engine.connect()

# write to postgres
pg_con.execute("DROP TABLE segs_rsample")
df.to_sql('segs_rsample', con=pg_engine, schema='public', if_exists='replace', index=False)

# create geom from geom_txt
sql = """
  ALTER TABLE public.segs_rsample ADD COLUMN geom geometry(GEOMETRY,4326);
  UPDATE public.segs_rsample SET geom = ST_GeomFromText(geom_txt,4326);"""
pg_con.execute(sql)

# variety of geometries
# df_geoms = pg_con.execute("""
#   SELECT ST_GeometryType(geom) geom_type, COUNT(*) AS n 
#   FROM segs_rsample GROUP BY ST_GeometryType(geom)""")
# df_geoms.fetchall()
# [('ST_Point', 6), ('ST_MultiPoint', 7), 
#  ('ST_MultiLineString', 16), ('ST_LineString', 455), 
#  ('ST_GeometryCollection', 2)]

# simplify segments
#   EPSG:6423 https://epsg.io/6423 NAD83(2011) / California zone 5; in meters
sql = """
  ALTER TABLE public.segs_rsample 
    ADD COLUMN IF NOT EXISTS geom_s1km geometry(LINESTRING, 4326);
  UPDATE public.segs_rsample 
   SET geom_s1km = ST_Transform(ST_Simplify(ST_Transform(geom, 6423), 1000), 4326)
   WHERE ST_GeometryType(geom) = 'ST_LineString';
  """
pg_con.execute(sql)


# create function for [GeoJSON Features from PostGIS · Paul Ramsey](http://blog.cleverelephant.ca/2019/03/geojson.html)
# TODO: consider level of precision for lon/lat
#    ST_AsGeoJSON(geometry geom, integer maxdecimaldigits=9, integer options=8);
sql = """
  CREATE OR REPLACE FUNCTION rowjsonb_to_geojson(
    rowjsonb JSONB, 
    geom_column TEXT DEFAULT 'geom')
  RETURNS TEXT AS 
  $$
  DECLARE 
   json_props jsonb;
   json_geom jsonb;
   json_type jsonb;
  BEGIN
   IF NOT rowjsonb ? geom_column THEN
     RAISE EXCEPTION 'geometry column ''%'' is missing', geom_column;
   END IF;
   json_geom  := ST_AsGeoJSON((rowjsonb ->> geom_column)::geometry)::jsonb;
   json_geom  := jsonb_build_object('geometry', json_geom);
   json_props := jsonb_build_object('properties', rowjsonb - geom_column);
   json_type  := jsonb_build_object('type', 'Feature');
   return (json_type || json_geom || json_props)::text;
  END; 
  $$ 
  LANGUAGE 'plpgsql' IMMUTABLE STRICT;
"""
pg_con.execute(sql)
# ERROR: TypeError: 'dict' object does not support indexing
# So executed sql in Terminal of rstudio.whalesafe.net after logging into db like so:
#   sudo apt-get update
#   sudo apt-get install postgresql-client
#   psql -h ws-postgis -U admin -W gis
