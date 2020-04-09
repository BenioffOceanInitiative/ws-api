library(here) # TODO: depend on here in whalesafe4r
library(glue)
library(DBI)
library(dplyr)
library(readr)
library(sf)
library(geojsonsf)
library(jsonlite)

library(bigrquery)


service_account_json <- "/home/admin/Benioff Ocean Initiative-454f666d1896.json"

bq_auth(path = service_account_json)

con <- dbConnect(
  bigquery(),
  project = "benioff-ocean-initiative",
  dataset = "whalesafe_ais",
  billing = "benioff-ocean-initiative")

con 

dbListTables(con)


dir_api <- "/srv/ws-api"
db_yml  <- file.path(dir_api, ".amazon_rds.yml")

# library(s4wr)                        # normal:    load installed library
devtools::load_all("/srv/whalesafe4r") # developer: load source library

# connect to database
con    <- db_connect(db_yml)

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
#* @param date_beg begin date, preferably in format YYYY-mm-dd, eg 2019-10-01
#* @param date_end end date, preferably in format YYYY-mm-dd, eg 2019-10-07
#* @param bbox bounding box in decimal degrees: lon_min,lat_min,lon_max,lat_max
#* @serializer contentType list(type="application/json")
#* @get /ship_segments
function(mmsi = NULL, date_beg = NULL, date_end = NULL, bbox = NULL){
  # mmsi = 477136800
  
  # cols <- "
  #   name, mmsi, 
  #   speed, seg_mins, seg_km, seg_kmhr, seg_knots, speed_diff, 
  #   beg_dt, end_dt, 
  #   beg_lon, beg_lat, end_lon, end_lat, 
  #   geometry" # year, gid, geometry, date
  cols <- "
    name_of_ship, mmsi, callsign, shiptype, imo_lr_ihs_no, length,
    operator, operator_code
    timestamp, lon, lat,
    distance, date_diff_minutes, speed_knots, implied_speed_knots, 
    source,
    linestring" # length_km? beg_dt, end_dt, beg_lon, beg_lat, end_lon, end_lat?
  
  sql <- glue("SELECT {cols} FROM gfw_ihs_segments LIMIT 10")
  
  segs <- st_read(dsn = con, query = sql)
  
  
  
  
  
  # geojson example
  nc = st_read(system.file("shape/nc.shp", package="sf"))
  write_sf(nc, "nc.geojson")
  
  summary(nc) # note that AREA was computed using Euclidian area on lon/lat degrees
  
  segs <- dbGetQuery(
    con, 
    "SELECT mmsi, timestamp, lon, lat, ST_AsGeoJSON(linestring) as ln_geom FROM gfw_ihs_segments LIMIT 3")
  
  # convert dataframe with a GeoJSON column to R sf object to then output all as GeoJSON
  # https://gis.stackexchange.com/questions/324863/manipulating-polygon-data-imported-from-google-bigquery-gis-within-r
  segs$geom <- do.call(rbind, lapply(segs$ln_geom, read_sf))$geometry
  st_geometry(segs) <- segs$geom
  segs <- segs %>% select(-ln_geom)
  
  #show 
  segs
  # Simple feature collection with 3 features and 4 fields
  # geometry type:  LINESTRING
  # dimension:      XY
  # bbox:           xmin: -119.8206 ymin: 33.7407 xmax: -118.1996 ymax: 34.16744
  # epsg (SRID):    4326
  # proj4string:    +proj=longlat +datum=WGS84 +no_defs
  # # A tibble: 3 x 5
  # mmsi timestamp             lon   lat                                     geom
  # <int> <dttm>              <dbl> <dbl>                         <LINESTRING [°]>
  #   1 477136800 2018-05-18 22:36:39 -118.  33.7   (-118.1996 33.7407, -118.1996 33.7408)
  # 2 636016519 2018-07-31 22:19:28 -118.  33.8 (-118.2181 33.75612, -118.2181 33.75612)
  # 3 218092000 2019-07-19 16:32:02 -120.  34.2 (-119.8206 34.16744, -119.8166 34.16654)
  writeLines(sf_geojson(segs), "segs5.geojson")
  # open geojson file and paste contents into http://geojson.io
  
  
  segs_sf
  
  jsonlite::toJSON(segs)
  
  cat(toJSON(segs, auto_unbox = TRUE))
  writeLines(toJSON(segs, auto_unbox = TRUE), "segs.geojson")
  
  library(purrr)
  library(tidyr)
  
  segs_x <- segs_df %>% 
    nest(properties = -contains("ln_geojson")) %>% 
    mutate(
     geometry = map(ln_geojson, fromJSON)) %>% 
    select(-ln_geojson)
  segs_x %>% select(-geometry) %>% toJSON()
  
  writeLines(toJSON(segs_x, auto_unbox = TRUE, pretty = T), "segs4.geojson")
  jsonlite::toJSON(segs_x)
  
  geojsonio::as.json(segs_x)
  segs_x <- segs_df$ln_geojson
  
  segs_ls <- list(
    "type" = "FeatureCollection",
    "crs"  = list(
      "type" = "name", 
      "properties" = list(
        "name"= "urn:ogc:def:crs:EPSG::4326" )),
    "features"=c(
      list(
        "type" = "Feature",
        "properties" = segs_df %>% slice(1) %>% select(-ln_geojson) %>% as.list(),
        "geometry"   = segs_df %>% slice(1) %>% pull(ln_geojson) %>% fromJSON()),
      list(
        "type" = "Feature",
        "properties" = segs_df %>% slice(1) %>% select(-ln_geojson) %>% as.list(),
        "geometry"   = segs_df %>% slice(1) %>% pull(ln_geojson) %>% fromJSON())))
    
  writeLines(toJSON(segs_ls, auto_unbox = T, pretty = T), "segs2.json")
  
    [
      { "type": "Feature", 
        "properties": {"mmsi":351819000,"timestamp":"2019-12-18 23:28:05","lon":-118.2383,"lat":33.7517},
        "geometry": 
    
    )
  
  toJSON(segs_ls, auto_unbox = T, pretty = T)
  jsonify::as.json(segs_ls)
  
  segs <- as.list(segs)
  segs$geometry = unbox(segs$ln_geojson)
  segs %>% 
    select(-ln_geojson) %>% 
    toJSON() %>% 
    writeLines("segs.geojson")
  
  library(jsonlite)
  s <- '{"type":"Topology","objects":{"map": "0"}}'
  j <- fromJSON(s)
  j$type <- unbox(j$type)
  j$objects$map <- unbox(j$objects$map)
  toJSON(j)
  
  read_sf("segs.geojson") %>% plot()
  
  library(geojsonR)
  
  
  # TODO: capture "Billed: 2.52 GB"
  
  where <- c()
  
  if (!is.null(mmsi))
    where <- c(where, glue("mmsi = {mmsi}"))
  
  if (!is.null(date_beg))
    where <- c(where, glue("beg_dt >= to_date('{date_beg}','YYYY-MM-DD')")) # TODO?: date_trunc('day', beg_dt)
  
  if (!is.null(date_end))
    where <- c(where, glue("end_dt <= to_date('{date_end}','YYYY-MM-DD')"))

  # bbox bounding box in decimal degrees: lon_min,lat_min,lon_max,lat_max
  if (!is.null(bbox)){

    b <- strsplit(bbox, ",")[[1]] %>% as.numeric()
    x1 <- b[1]; y1 <- b[2]; x2 <- b[3]; y2 <- b[4]
    sql_bbox <- glue("ST_Intersects(geometry, 'SRID=4326;POLYGON(({x2} {y1}, {x2} {y2}, {x1} {y2}, {x1} {y1}, {x2} {y1}))')")
  
    where <- c(where, sql_bbox)
  }
  
  if (length(where) > 0){
    sql_where <- paste(where, collapse = " AND \n")
    sql <- glue("{sql} WHERE {sql_where}")
  }
  
  message(glue(
    "/ship_segments
       sql:{sql}"))
  
  segs <- st_read(dsn = con, query = sql)
  
  sf_geojson(segs)
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
