library(here)
library(glue)
library(DBI)
library(RPostgres)
library(dplyr)
library(readr)
library(lubridate)
library(stringr)

passwd_txt <- "/home/admin/ws_admin_pass.txt"
dir_cache  <- "/home/admin/api_cache"

passwd <- readLines(passwd_txt)

# TODO: consider gzip ----

# https://github.com/rstudio/plumber/issues/103
# #return compressed bin
# x %>%  
#   serialize(NULL)%>%  
#   memCompress(type = 'gzip')  
# #undo compress and serialize 
# x <- GET(url) %>%    
#   .$content %>%    
#   memDecompress(type = 'gzip') %>%  
#   unserialize()

# https://community.rstudio.com/t/plumber-apis-timing-big-discrepancies-between-r-studio-connect-and-local-runs/29483/11
# @post /predict
# @serializer contentType list(type="gzip")
# function(req, res) {
#   
#   # Function logic here
#   
#   json_file <- tempfile()
#   gz_file <- paste0(json_file, ".gz")
#   jsonlite::write_json(df_final, json_file)
#   R.utils::gzip(json_file, gz_file)
#   readBin(gz_file, "raw", n = file.info(gz_file)$size)
# }

# db connect ----
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname   = "gis",
  host     = "ws-postgis",
  port     = 5432,
  user     = "admin",
  password = passwd)
# dbListTables(con)  

# helper functions ----

args2where <- function(..., args_numeric = NULL, where_pfx = T){
  args <- list(...)

  w <- c()
  for (var in names(args)){
    val <- args[[var]]
    if (!is.null(val)){
      if (var %in% args_numeric){
        var_w <- glue("{var} = {val}")
      } else {
        var_w <- glue("{var} = '{val}'")
      }
      w <- c(w, var_w)
    }
  }
  if (length(w) > 0)
    w <- paste(w, collapse = ' AND ')
  
  if (where_pfx & length(w) > 0)
    w <- paste("WHERE", w)
  
  if (length(w) == 0)
    w <- ""
  
  w
}

sql2db <- function(sql, do_msg = T){
  
  t0 <- now()
  res <- dbGetQuery(con, sql)
  td <- (now() - t0) %>% as.numeric() %>% round(digits=2)
  
  if(do_msg){
    t_now <- now(tzone = "America/Los_Angeles") # %>% format("")
    fxn   <- deparse(sys.calls()[[sys.nframe()-1]])
    fxn   <- paste(fxn, collapse = "") %>% str_replace("^(.*)\\((.*)$", "\\1()")
    msg   <- glue("{fxn} dbGetQuery(con, sql) took {td} secs ~ {t_now} PDT\n  sql: {sql}", .trim = F)
    message(msg)
  }
  
  res
}

#* return table of operators as CSV
#* @serializer contentType list(type="text/csv")
#* @get /operators_csv
function(){
  
  csv_file <- tempfile(fileext = ".csv")
  
  on.exit(unlink(csv_file), add = TRUE)

  operators <- tbl(con, "operator_stats")
  
  d <- operators %>% 
    arrange(operator) %>% 
    collect()

  write_csv(d, csv_file)
  
  readBin(csv_file, "raw", n=file.info(csv_file)$size)
}

get_operators <- function(...){

  sql_where <- args2where(
    ..., 
    args_numeric = c("year"))
  
  sql <- glue("SELECT * FROM operator_stats {sql_where} ORDER BY operator, year")
  
  sql2db(sql)
}
#* return list of operators as JSON
#* @param operator eg "Foss Maritime Co"
#* @param year eg "2019"
#* @get /operators
function(operator = NULL, year = NULL){
  get_operators(
    operator = operator, 
    year     = year)
}

get_ship_stats_monthly <- function(...){

  sql_where <- args2where(
    ...,
    args_numeric = c("mmsi","operator_code","year","month"))

  sql <- glue("SELECT * FROM ship_stats_monthly {sql_where} ORDER BY mmsi, year, month")
  
  sql2db(sql)
}
#* return monthly ship stats as JSON
#* @param operator
#* @param operator_code
#* @param mmsi
#* @param name_of_ship
#* @param shiptype
#* @param ship_category
#* @param year
#* @param month
#* @param month_grade
#* @get /ship_stats_monthly
function(
  operator=NULL, operator_code=NULL, 
  mmsi=NULL, name_of_ship=NULL, 
  shiptype=NULL, ship_category=NULL, 
  year=NULL, month=NULL,
  month_grade=NULL){
  
  get_ship_stats_monthly(
    operator      = operator,
    operator_code = operator_code,
    mmsi          = mmsi,
    name_of_ship  = name_of_ship,
    shiptype      = shiptype,
    ship_category = ship_category,
    year          = year,
    month         = month,
    month_grade   = month_grade)

}

get_ship_stats_annual <- function(...){
  
  sql_where <- args2where(
    ..., 
    args_numeric = c("mmsi","operator_code","year"))
  
  sql <- glue("SELECT * FROM ship_stats_annual {sql_where} ORDER BY mmsi, year")
  
  sql2db(sql)
}
#* return annual ship stats as JSON
#* @param operator
#* @param operator_code
#* @param mmsi
#* @param name_of_ship
#* @param shiptype
#* @param ship_category
#* @param year
#* @param cooperation_score
#* @get /ship_stats_annual
function(
  operator=NULL, operator_code=NULL, 
  mmsi=NULL, name_of_ship=NULL, 
  shiptype=NULL, ship_category=NULL, 
  year=NULL, 
  cooperation_score=NULL){
  
  get_ship_stats_annual(
    operator      = operator, 
    operator_code = operator_code, 
    mmsi          = mmsi, 
    name_of_ship  = name_of_ship, 
    shiptype      = shiptype, 
    ship_category = ship_category, 
    year          = year, 
    cooperation_score = cooperation_score)
}

#* return table of ships (ship_stats_annual) as CSV
#* @serializer contentType list(type="text/csv")
#* @get /ships_csv
function(){
  csv_file <- tempfile(fileext = ".csv")

  on.exit(unlink(csv_file), add = TRUE)

  d <- tbl(con, "ship_stats_annual") %>% 
    collect()

  message(glue(
    "
    /ships_csv
      nrow(d):{nrow(d)}
      csv_file:{csv_file}
    "))

  write_csv(d, csv_file)

  readBin(csv_file, "raw", n=file.info(csv_file)$size)
}

get_ship_segments <- function(date_beg = NULL, date_end = NULL, bbox = NULL, mmsi = NULL, simplify = NULL){
  # sql fields ----
  if (!is.null(simplify)){
    stopifnot(simplify %in% c("1km"))
    fld_geom = glue("geom_s{simplify}")
  } else{
    fld_geom = "geom"
  }
  flds <- glue("
    mmsi, date, seg_id, 
    speed_bin_num, avg_speed_knots_final, 
    total_distance_nm, 
    timestamp_beg, timestamp_end, 
    npts, {fld_geom} AS geom")
  # TODO?: beg_lon, beg_lat, end_lon, end_lat?
  
  # sql where ----
  where <- c()
  
  if (!is.null(mmsi))
    where <- c(where, glue("mmsi = {mmsi}"))
  
  if (!is.null(date_beg))
    where <- c(where, glue("date >= '{date_beg}'"))
  
  if (!is.null(date_end))
    where <- c(where, glue("date <= '{date_end}'"))
  
  # sql where bbox ----
  # bounding box in decimal degrees: lon_min,lat_min,lon_max,lat_max
  if (!is.null(bbox)){
    
    b <- strsplit(bbox, ",")[[1]] %>% as.numeric()
    x1 <- b[1]; y1 <- b[2]; x2 <- b[3]; y2 <- b[4]
    sql_bbox <- glue("ST_Intersects({fld_geom}, 'SRID=4326;POLYGON(({x2} {y1}, {x2} {y2}, {x1} {y2}, {x1} {y1}, {x2} {y1}))')")
    
    where <- c(where, sql_bbox)
  }
  
  if (length(where) > 0){
    sql_where <- paste(" AND ", paste(where, collapse = "AND \n"))
  } else {
    sql_where <- ""
  }
  
  #[GeoJSON Features from PostGIS · Paul Ramsey](http://blog.cleverelephant.ca/2019/03/geojson.html)
  sql <- glue(
    "SELECT rowjsonb_to_geojson(to_jsonb(tbl.*)) AS txt
    FROM (
      SELECT {flds} 
      FROM public.segs 
      WHERE 
        ST_GeometryType(geom) = 'ST_LineString' {sql_where}) tbl;")
  
  message(glue(
    "{Sys.time()}: dbGetQuery() begin
          sql:{sql}
    "))
  res <- dbGetQuery(con, sql)
  
  paste(
    '{
    "type": "FeatureCollection","name": "WhaleSafe_API_segs","crs": 
    { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
    "features": [',
    paste(res$txt, collapse = ",\n"),
    "]}") 
  
}
#* return GeoJSON of ship segments
#* @param date_beg begin date, in format YYYY-mm-dd, eg 2019-10-01
#* @param date_end end date, in format YYYY-mm-dd, eg 2019-10-07
#* @param bbox bounding box in decimal degrees: lon_min,lat_min,lon_max,lat_max
#* @param mmsi AIS ship ID, eg 248896000
#* @param simplify simplification specifying tolerance (pre-rendered): 1km
#* @serializer contentType list(type="application/json")
#* @get /ship_segments
function(date_beg = NULL, date_end = NULL, bbox = NULL, mmsi = NULL, simplify = NULL){
  get_ship_segments(date_beg, date_end, bbox, mmsi, simplify)
}

#* redirect to the swagger interface 
#* @get /
#* @html
function(req, res) {
  res$status <- 303 # redirect
  res$setHeader("Location", "./__swagger__/")
  "<html>
  <head>
    <meta http-equiv=\"Refresh\" content=\"0; url=./__swagger__/\" />
  </head>
  <body>
    <p>For documentation on this API, please visit <a href=\"http://api.ships4whales.org/__swagger__/\">http://api.ships4whales.org/__swagger__/</a>.</p>
  </body>
</html>"
}
