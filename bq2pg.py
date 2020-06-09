import pandas as pd
from google.cloud  import bigquery
from google.oauth2 import service_account
from sqlalchemy    import create_engine
from datetime import date, datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta, tz
import time
import sys

def msg(txt):
  print(txt + " ~ " + str(datetime.now()))
  sys.stdout.flush()

# bigquery connection
project_id       = 'benioff-ocean-initiative'
credentials_json = '/home/admin/Benioff Ocean Initiative-454f666d1896.json'
credentials      = service_account.Credentials.from_service_account_file(credentials_json)
bq_client        = bigquery.Client(credentials=credentials, project=project_id)

# postgres connection
with open('/home/admin/ws_admin_pass.txt') as f:
    passwd = f.read().splitlines()[0]  
pg_engine = create_engine('postgresql+psycopg2://admin:' + passwd + '@ws-postgis:5432/gis')
pg_con    = pg_engine.connect()

def bq2pg(date_beg, date_end, replace_segs = False):
  # i_date   = 0
  # date_beg = '2017-01-01'
  # date_end = '2017-02-01'
  
  sql_date = f"date >= '{date_beg}' AND date < '{date_end}'"
  sql = f"""
    SELECT * EXCEPT (geom), ST_ASTEXT(geom) AS geom_txt 
    FROM 
    `benioff-ocean-initiative.whalesafe.gfw_segments_agg`
    WHERE {sql_date};
    """
  
  df = bq_client.query(sql).to_dataframe()
  # print(df.info(memory_usage='deep'))
  # df
  
  # write to postgres
  if replace_segs:
    pg_con.execute('DROP TABLE segs')
    df.to_sql('segs', con=pg_engine, schema='public', if_exists='replace', index=False)
    pg_con.execute("""
      -- DROP INDEX idx_segs_date;
      CREATE INDEX idx_segs_date ON public.segs (date DESC NULLS LAST);
      CLUSTER public.segs USING idx_segs_date;
      ANALYZE public.segs;

      ALTER TABLE public.segs ADD COLUMN geom geometry(GEOMETRY,4326);
      CREATE INDEX idx_segs_geom ON public.segs USING GIST (geom);
      """)
    # Note: using geometry(4326) vs geography() since ST_Simplify only works on geometries for now
    #   https://postgis.net/workshops/postgis-intro/geography.html
    #   [#3377 (ST_Simplify for geography) – PostGIS](https://trac.osgeo.org/postgis/ticket/3377)
  else:
    df.to_sql('segs', con=pg_engine, schema='public', if_exists='append', index=False)
    
  # TODO: log dates done in BQ

def pg_simplify():
  
  # update geometry for all
  pg_con.execute(
    """
    UPDATE public.segs 
    SET geom = ST_GeomFromText(geom_txt,4326)
    WHERE geom IS NULL;
    """)
  
  # simplify segments
  #   EPSG:6423 https://epsg.io/6423 NAD83(2011) / California zone 5; in meters
  sql = """
    ALTER TABLE public.segs
      ADD COLUMN IF NOT EXISTS geom_s1km geometry(LINESTRING, 4326);
    UPDATE public.segs
     SET geom_s1km = ST_Transform(ST_Simplify(ST_Transform(geom, 6423), 1000), 4326)
     WHERE 
        ST_GeometryType(geom) = 'ST_LineString' AND
        geom_s1km IS NULL;
    CREATE INDEX idx_segs_geom_s1km ON public.segs USING GIST (geom_s1km);
    """
  pg_con.execute(sql)

def create_pg_function_for_geojson_features():
  # run once only
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
  #pg_con.execute(sql)
  # ERROR: TypeError: 'dict' object does not support indexing
  # So executed sql in Terminal of rstudio.whalesafe.net after logging into db like so:
  #   sudo apt-get update
  #   sudo apt-get install postgresql-client
  #   psql -h ws-postgis -U admin -W gis

def months_iter(startDate, endDate):
    currentDate = startDate
    while currentDate < endDate:
        yield currentDate
        currentDate = currentDate + relativedelta(months=+1)

def load_all_by_month():
    
  # get date range
  # SELECT min(date), max(date) FROM whalesafe.gfw_segments_agg WHERE date > '1990-01-01';
  # 2017-01-01 2020-06-04
  
  months = [m for m in months_iter(date(2017, 1,1), date.today())]
  msg("START load_all_by_month() for " + str(len(months)) + " months")
  time_beg = datetime.now()
  
  for i_date, date_beg in enumerate(months_iter(date(2017, 1,1), date.today())):
    msg(str(i_date) + ": " + str(date_beg))
    
    # i_date = 10
    # date_beg = date(2017, 1,1)
    date_end = date_beg + relativedelta(months=+1)
    
    date_beg = str(date_beg)
    date_end = str(date_end)
    
    t0 = time.time()
    
    if (i_date == 0):
      bq2pg(date_beg, date_end, replace_segs = True)
    else:
      bq2pg(date_beg, date_end)
    
    secs = time.time() - t0
    t_done = t_now + (len(months) - i_date) * ((datetime.now() - time_beg) / i_date)
    t_done = t_done.replace(tzinfo=tz.gettz('UTC')).astimezone(tz.gettz('America/Los_Angeles'))
    msg('  ' + ('%0.2f' % secs) + ' seconds; expected completion: ' + str(t_done))
  msg("Finished loading months! Simplifying...")
  pg_simplify()
  msg('FINISHED!)

def load_tbl(bq_tbl, pg_tbl, fld_indexes = None):
  # bq_tbl = 'whalesafe_ais.operator_stats'; pg_tbl = 'operator_stats'; fld_indexes = ['operator','year']
  df = bq_client.query("SELECT * FROM " + bq_tbl).to_dataframe()
  pg_con.execute('DROP TABLE IF EXISTS segs')
  df.to_sql(pg_tbl, con=pg_engine, schema='public', if_exists='replace', index=False)
  for fld in fld_indexes:
    sql = "CREATE INDEX idx_{pg_tbl}_{fld} ON {pg_tbl} ({fld})".format(pg_tbl=pg_tbl, fld=fld)
    print(sql)
    pg_con.execute(sql)

def load_nonspatial():
  load_tbl('whalesafe_ais.mmsi_cooperation_stats', 'ship_stats_annual')
  load_tbl('whalesafe_ais.ship_stats'            , 'ship_stats_monthly')
  load_tbl('whalesafe_ais.operator_stats'        , 'operator_stats'    , fld_indexes = ['operator','year'])

  # operator_stats

msg("__main__")
if __name__ == "__main__":
  msg("load_all_by_month()")
  load_all_by_month() # 30 min for 2017-01-01 to 2020-06-04
  # sudo su - root
  # py=/home/admin/.local/share/r-miniconda/envs/r-reticulate/bin/python; script_py=/home/admin/github/ws-api/bq2pg.py; out_txt=/home/admin/github/ws-api/bq2pg_out.txt
  # py=/usr/bin/python3; script_py=/home/admin/github/ws-api/bq2pg.py; out_txt=/home/admin/github/ws-api/bq2pg_out.txt
  # echo py:$py $script_py:$script_py out_txt:$out_txt
  # $py $script_py >$out_txt 2>&1 &
  # cat $out_txt
  #print("test print()")
  #msg("test msg()")
  
