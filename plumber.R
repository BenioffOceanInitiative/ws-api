library(here)
library(glue)
library(DBI)
library(RPostgres)
#library(dplyr)
#library(readr)
#library(sf)
#library(geojsonsf)
#library(jsonlite)
# library(leaflet)
# library(mapview)

passwd <- readLines("/home/admin/ws_admin_pass.txt")

# connection to database
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname   = "gis",
  host     = "ws-postgis",
  port     = 5432,
  user     = "admin",
  password = passwd)

# dbListTables(con)  

#* page through operators as JSON
#* @param sort_by field to sort by; default = operator
#* @param n_perpage number of rows per page of results; default = 20
#* @param page page of results; default = 1
#* @get /operators_page
function(sort_by="operator", n_perpage = 20, page = 1){
  # sort_by = "operator"; n_perpage = "20"; page = "1"
  
  n_perpage <- as.integer(n_perpage)
  page      <- as.integer(page)
  
  # eg page 2: 21 to 30
  n_beg = (page - 1) * n_perpage + 1
  n_end = n_beg + n_perpage - 1
  
  operators <- tbl(con, "operator_stats")
  
  d <- operators %>% 
    #arrange(!!sort_by) %>% 
    mutate(
      row_n = row_number()) %>% 
    filter(
      row_n >= !!n_beg,
      row_n <= !!n_end) %>% 
    select(operator, 
           grade, 
           `compliance score (reported speed)`, 
           `total distance (km)`, 
           `total distance (nautcal miles)`, 
           `distance (nautcal miles) under 10 knots`, 
           `distance (nautcal miles) over 10 knots`) %>% 
    collect()
  
  n_records <- operators %>% 
    summarize(n = n()) %>% 
    collect() %>% 
    pull(n)
  n_pages <- n_records %/% n_perpage + 
    ifelse(n_records %% n_perpage > 0, 1, 0)
  attr(d, "n_records") <- n_records
  attr(d, "n_pages")   <- n_pages
  
  d
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

#* return list of operators as JSON
#* @get /operators
function(){
  
  sql <- glue(
  "SELECT 
operator,
year,
year_coop_score,
year_ship_count,
CASE 
  WHEN ((total_distance_km_under_10/total_distance_km) * 100) >= 99
  THEN 'A+'
  WHEN ((total_distance_km_under_10/total_distance_km) * 100) < 99 
  AND ((total_distance_km_under_10/total_distance_km) * 100) >= 90
  THEN 'A'
  WHEN ((total_distance_km_under_10/total_distance_km) * 100) < 90 
  AND ((total_distance_km_under_10/total_distance_km) * 100) >= 80
  THEN 'B'
  WHEN ((total_distance_km_under_10/total_distance_km) * 100) < 80 
  AND ((total_distance_km_under_10/total_distance_km) * 100) >= 70
  THEN 'C'
  WHEN ((total_distance_km_under_10/total_distance_km) * 100) < 70 
  AND ((total_distance_km_under_10/total_distance_km) * 100) >= 60
  THEN 'D'
  ELSE 'F'
  END AS grade,
  total_distance_km,
total_distance_km_under_10,
total_distance_km_btwn_10_12,
total_distance_km_btwn_12_15,
total_distance_km_over_15,
  avg_speed_knots,
  mmsi_list
FROM(
SELECT
    operator, 
    year, 
    ROUND(AVG(coop_score),2) AS year_coop_score, 
    COUNT(distinct(mmsi)) AS year_ship_count,
    SUM( total_distance_km ) AS total_distance_km,
    SUM( total_distance_km_under_10 ) AS total_distance_km_under_10 ,
    SUM( total_distance_km_btwn_10_12 ) AS total_distance_km_btwn_10_12 ,
    SUM( total_distance_km_btwn_12_15 ) AS total_distance_km_btwn_12_15 ,
    SUM( total_distance_km_over_15 ) AS total_distance_km_over_15 ,
    SUM( avg_speed_knots ) AS avg_speed_knots,
    STRING_AGG(CAST(mmsi AS STRING), ', ') AS mmsi_list,
    #STRING_AGG(DISTINCT(shiptype), ', ') AS ship_types
  FROM 
    `benioff-ocean-initiative.whalesafe_ais.mmsi_cooperation_stats`
  GROUP BY operator, year
  ORDER BY year_coop_score DESC);")
  
  message(glue(
    "{Sys.time()}: dbGetQuery() begin
          sql:{sql}
    
    "))
  
  operator_stats <- dbGetQuery(con, sql)
}

#* return mmsi stats table as JSON
#* @get /mmsi_cooperation_stats
function(){
  
  sql <- glue(
    "SELECT * 
    FROM 
    `benioff-ocean-initiative.whalesafe_ais.mmsi_cooperation_stats`;")
  
  message(glue(
    "{Sys.time()}: dbGetQuery() begin
          sql:{sql}
    
    "))
  
  stats <- dbGetQuery(con, sql)
}

# TODO: @get /ships_by_operator

#* return table of ships as CSV
#* @serializer contentType list(type="text/csv")
#* @get /ships_csv
function(){
  csv_file <- tempfile(fileext = ".csv")

  on.exit(unlink(csv_file), add = TRUE)

  d <- tbl(con, "ship_stats_2019") %>% 
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

#* return GeoJSON of ship segments
#* @param mmsi AIS ship ID, eg 248896000
#* @param date_beg begin date, in format YYYY-mm-dd, eg 2019-10-01
#* @param date_end end date, in format YYYY-mm-dd, eg 2019-10-07
#* @param bbox bounding box in decimal degrees: lon_min,lat_min,lon_max,lat_max
#* @param simplify simplification specifying tolerance (pre-rendered): 1km
#* @serializer contentType list(type="application/json")
#* @get /ship_segments
function(bbox = NULL, date_end = NULL, date_beg = NULL, mmsi = NULL, simplify = NULL){
  # mmsi = 477136800
  
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
