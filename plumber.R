library(here) # TODO: depend on here in whalesafe4r
library(glue)
library(DBI)
library(dplyr)
library(readr)
library(sf)
library(geojsonsf)
library(jsonlite)

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
#* @param date_end end date, preferably in format YYYY-mm-dd, eg 2019-11-01
#* @param bbox bounding box in decimal degrees: lon_min,lat_min,lon_max,lat_max
#* @serializer contentType list(type="application/json")
#* @get /ship_segments
function(mmsi = NULL, date_beg = NULL, date_end = NULL, bbox = NULL){
  
  # mmsi = "248896000"; date_beg=NULL; date_end=NULL; bbox = NULL
  sql <- "SELECT * FROM vsr_segments"
  #segs <- tbl(con, "vsr_segments")
  
  # segs %>% 
  #   summarize(
  #     min_dt = min(beg_dt),
  #     max_dt = max(end_dt)) %>% 
  #   collect()
  # min_dt: 2018-01-01
  # max_dt: 2019-11-09

  # vsrs <- st_read(dsn = con, layer = "vsr_zones")
  # st_bbox(vsrs)
  #       xmin       ymin       xmax       ymax 
  # -121.02994   33.29988 -117.48026   34.57384
  
  where <- c()
  
  if (!is.null(mmsi))
    where <- c(where, glue("mmsi = {mmsi}"))
  
  if (!is.null(date_beg))
    where <- c(where, glue("beg_dt >= {date_beg}")) # TODO?: date_trunc('day', beg_dt)
  
  if (!is.null(date_end))
    where <- c(where, glue("end_dt <= {date_end}"))

  # c bounding box in decimal degrees: lon_min,lat_min,lon_max,lat_max
  if (!is.null(bbox)){

    #bbox <- "-120,33,-119,34"
    b <- strsplit(bbox, ",")[[1]] %>% as.numeric()
    x1 <- b[1]; y1 <- b[2]; x2 <- b[3]; y2 <- b[4]; 
    sql_bbox <- glue("ST_Intersects(geometry, 'SRID=4326;POLYGON(({x2} {y1}, {x2} {y2}, {x1} {y2}, {x1} {y1}, {x2} {y1}))')")
  
    where <- c(where, sql_bbox)
  }
  
  if (length(where) > 0){
    sql_where <- paste(where, collapse = " AND ")
    sql <- glue("{sql} WHERE {sql_where}")
  }
  
  message(glue("sql:{sql}"))
  
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
