import pandas as pd
from google.cloud  import bigquery
from google.oauth2 import service_account
from sqlalchemy    import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import date, datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from dateutil import tz
import time
import sys

def msg(txt):
  #txt = '  158.208 seconds; expected completion: 2020-06-09 12:48:31.328176-07:00'
  print(txt + " ~ " + str(datetime.now(tz.gettz('America/Los_Angeles'))))
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

# postgres raw connection without transaction (so autocommit on) for vacuuming db
pg_raw = pg_engine.raw_connection()
pg_raw.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
pg_raw_cursor = pg_raw.cursor()

def bq2pg_segs_date(date_beg, date_end, replace_segs = False):
  # for initial data loading, one month at a time
  
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
  msg('  sql: ' + sql)
  
  df = bq_client.query(sql).to_dataframe()
  # print(df.info(memory_usage='deep'))
  # df
  
  # write to postgres
  if replace_segs:
    pg_con.execute('DROP TABLE IF EXISTS segs')
    df.to_sql('segs', con=pg_engine, schema='public', if_exists='replace', index=False)
  else:
    df.to_sql('segs', con=pg_engine, schema='public', if_exists='append', index=False)
    
  # TODO: log dates done in BQ

def load_bq2pg_latest():
  
  timestamp_last = pg_con.execute('SELECT max(timestamp_end) AS timestamp_last FROM segs').fetchall()[0].timestamp_last
  date_last      = pg_con.execute('SELECT max(date) AS date_last FROM segs').fetchall()[0].date_last
  
  date_last_m1       = date_last - relativedelta(days = 1) # backup one day
  date_last_m1_str   = date_last_m1.strftime(     '%Y-%m-%d')
  timestamp_last_str = timestamp_last.strftime('%Y-%m-%d %H:%M:%S')
  sql_timestamp = f"timestamp_end > '{timestamp_last_str}' AND date > '{date_last_m1_str}'"
  
  sql = f"""
    SELECT * EXCEPT (geom), ST_ASTEXT(geom) AS geom_txt 
    FROM 
    `benioff-ocean-initiative.whalesafe.gfw_segments_agg`
    WHERE {sql_timestamp};
    """
  msg('  sql: ' + sql)
  
  df = bq_client.query(sql).to_dataframe()
  df_rows = df.shape[0]
  
  if df_rows == 0:
    msg(f'  0 new rows since {timestamp_last_str}')
  else:
    new_timestamp_last_str = max(df.timestamp_end).strftime('%Y-%m-%d %H:%M:%S')
    msg(f'  inserting {str(df_rows)} rows: {timestamp_last_str} TO {new_timestamp_last_str}')
    # print(df.info(memory_usage='deep'))
    # df
      
    # append data to segs table in postgis
    df.to_sql('segs', con=pg_engine, schema='public', if_exists='append', index=False)
    
    # update geom to use geom_txt where null
    sql = """
      UPDATE public.segs 
        SET geom = ST_GeomFromText(geom_txt,4326)
        WHERE geom IS NULL;
      """
    msg('  sql: ' + sql)
    pg_con.execute(sql)
    
    # update simplification(s)
    sql = """
      UPDATE public.segs
       SET geom_s1km = ST_Transform(ST_Simplify(ST_Transform(geom, 6423), 1000), 4326)
       WHERE 
          ST_GeometryType(geom) = 'ST_LineString' AND
          geom_s1km IS NULL;
      """
    msg('  sql: ' + sql)
    pg_con.execute(sql)
    
    # load nonspatial
    msg("load_nonspatial()")
    load_nonspatial()
    
    # update simplification(s)
    sql = "VACUUM(ANALYZE, VERBOSE)"
    msg('  sql: ' + sql)
    pg_raw_cursor.execute(sqlmap

def pg_spatialize_segs():
  sql = """
    -- date: index and cluster
    CREATE INDEX idx_segs_date ON public.segs (date DESC NULLS LAST);
    CLUSTER public.segs USING idx_segs_date;
    
    -- geom: add, update, index
    ALTER TABLE public.segs ADD COLUMN geom geometry(GEOMETRY,4326);
    UPDATE public.segs 
      SET geom = ST_GeomFromText(geom_txt,4326)
      WHERE geom IS NULL;
    CREATE INDEX idx_segs_geom ON public.segs USING GIST (geom);
    
    -- table: update indices, clusters
    ANALYZE public.segs;
    """
  pg_con.execute(sql)

def pg_simplify_segs():
  # Note: using geometry(4326) vs geography() since ST_Simplify only works on geometries for now
  #   https://postgis.net/workshops/postgis-intro/geography.html
  #   [#3377 (ST_Simplify for geography) – PostGIS](https://trac.osgeo.org/postgis/ticket/3377)
  #   simplify segments using crs EPSG:6423 https://epsg.io/6423 NAD83(2011) / California zone 5; in meters
  
  sql = """
    UPDATE public.segs 
      SET geom = ST_GeomFromText(geom_txt,4326)
      WHERE geom IS NULL;
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
  
  tz_pt = tz.gettz('America/Los_Angeles')
  t_beg = datetime.now(tz_pt)
  
  for i_date, date_beg in enumerate(months_iter(date(2017, 1,1), date.today())):
    msg(str(i_date) + ": " + str(date_beg))
    
    # i_date = 10
    # date_beg = date(2017, 1,1)
    date_end = date_beg + relativedelta(months=+1)
    
    date_beg = str(date_beg)
    date_end = str(date_end)
    
    t0 = datetime.now()
    if (i_date == 0):
      bq2pg_segs(date_beg, date_end, replace_segs = True)
    else:
      bq2pg_segs(date_beg, date_end)
    t_secs = (datetime.now() - t0).total_seconds()
    
    t_now          = datetime.now(tz_pt)
    i_togo         = len(months) - (i_date + 1)
    t_elapsed_sec  = (t_now - t_beg).total_seconds()
    t_per_i_sec    = t_elapsed_sec / (i_date + 1)
    t_finish       = t_now + timedelta(seconds = i_togo *  t_per_i_sec)
    msg('  took ' + str(t_secs) + ' seconds; expected completion: ' + str(t_finish) + '; ' + str(t_per_i_sec) + ' seconds per i (n=' + str(len(months)) + ')')

def load_tbl(bq_tbl, pg_tbl, fld_indexes = None):
  # bq_tbl = 'whalesafe_ais.operator_stats'; pg_tbl = 'operator_stats'; fld_indexes = ['operator','year']
  df = bq_client.query("SELECT * FROM " + bq_tbl).to_dataframe()
  pg_con.execute('DROP TABLE IF EXISTS ' + pg_tbl)
  df.to_sql(pg_tbl, con=pg_engine, schema='public', if_exists='replace', index=False)
  for fld in fld_indexes:
    sql = "CREATE INDEX idx_{pg_tbl}_{fld} ON {pg_tbl} ({fld})".format(pg_tbl=pg_tbl, fld=fld)
    print(sql)
    pg_con.execute(sql)

def vacuum_db():
  pg_con.execute("VACUUM(FULL, ANALYZE, VERBOSE)")
  # InternalError: (psycopg2.errors.ActiveSqlTransaction) VACUUM cannot run inside a transaction block
  # So ran from rstudio.whalesafe.net Terminal:
  #   admin:~$ psql -h ws-postgis -U admin -W gis
  #   gis=# VACUUM(FULL, ANALYZE, VERBOSE);


def load_nonspatial():
  load_tbl('stats.ship_stats_annual'     , 'ship_stats_annual'     , fld_indexes = ['mmsi','operator','year'])
  load_tbl('stats.ship_stats_monthly'    , 'ship_stats_monthly'    , fld_indexes = ['mmsi','operator','year','month'])
  load_tbl('stats.operator_stats_annual' , 'operator_stats_annual' , fld_indexes = ['operator','year'])
  load_tbl('stats.operator_stats_monthly', 'operator_stats_monthly', fld_indexes = ['operator','year'])

  # operator_stats

msg("__main__")
if __name__ == "__main__":
  
  # initial load
  # msg("load_all_by_month()")
  # load_all_by_month() # 30 min for 2017-01-01 to 2020-06-04
  # msg("pg_spatialize_segs()")
  # pg_spatialize_segs()
  # msg("pg_simplify_segs()")
  # pg_simplify_segs()
  # msg("vacuum_db()")
  # vacuum_db()
  # msg("FINISHED!")

  msg("load_bq2pg_latest()")
  load_bq2pg_latest()

  # sudo su - root
  # py=/home/admin/.local/share/r-miniconda/envs/r-reticulate/bin/python; script_py=/home/admin/github/ws-api/bq2pg.py; out_txt=/home/admin/github/ws-api/bq2pg_out.txt
  # py=/usr/bin/python3; script_py=/home/admin/github/ws-api/bq2pg.py; out_txt=/home/admin/github/ws-api/bq2pg_out.txt
  # echo py:$py $script_py:$script_py out_txt:$out_txt
  # $py $script_py >$out_txt 2>&1 &
  # cat $out_txt
  #print("test print()")
  #msg("test msg()")
  
  # set up cron job in Debian 10 (per `lsb_release -a`)
  # sudo su - root
  # apt-get update; apt-get install cron; crontab -l
  # crontab -e
  # 0 18 * * * /usr/bin/python3 /home/admin/github/ws-api/bq2pg.py > /home/admin/github/ws-api/bq2pg_out.txt 2>&1
  # apt install mailutils
  
  # TODO: add mail to cron by setting up whalesafe gmail account 
  #       in 'less secure mode' and initialize in /etc/ssmtp/ssmtp.conf per: 
  #         https://linuxhint.com/bash_script_send_email/
  # mail -s 'whalesafe: cron bq2pg.py finished' ben@ecoquants.com <<< `cat /home/admin/github/ws-api/bq2pg_out.txt`
  
  
