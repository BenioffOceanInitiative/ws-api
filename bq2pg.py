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
pg_engine = create_engine('postgresql+psycopg2://admin:whalestrike@ws-postgis:5432/gis')
pg_con    = pg_engine.connect()

# write to postgres
pg_con.execute("DROP TABLE segs_rsample")
df.to_sql('segs_rsample', con=pg_engine, schema='public', if_exists='replace', index=False)

# create geom from geom_txt
sql = """
  ALTER TABLE public.segs_rsample ADD COLUMN geom geometry(GEOMETRY,4326);
  UPDATE public.segs_rsample SET geom = ST_GeomFromText(geom_txt,4326);"""
pg_con.execute(sql)


