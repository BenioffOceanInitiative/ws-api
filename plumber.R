library(here) # TODO: depend on here in whalesafe4r
library(glue)
library(DBI)
library(dplyr)
library(sf)
library(geojsonsf)
library(jsonlite)

dir_api <- "/srv/ws-api"
db_yml  <- file.path(dir_api, ".amazon_rds.yml")

# library(s4wr)                        # normal:    load installed library
devtools::load_all("/srv/whalesafe4r") # developer: load source library

# connect to database
con    <- db_connect(db_yml)

#* List ship operators
#* @param sort_by field to sort by; default = operator
#* @param n_perpage number of rows per page of results; default = 20
#* @param page page of results; default = 1
#* @get /operators
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

# TODO: @get /ships_by_operator

#* Return GeoJSON by ship
#* @param mmsi AIS ship ID
#* @param date_beg begin date
#* @param date_end end date
#* @param bbox bounding box in decimal degrees: lon_min,lat_min,lon_max,lat_max
#* @serializer contentType list(type="application/json")
#* @get /ship_segments
function(mmsi){
  
  # mmsi = "248896000"; date_beg=NULL; date_end=NULL; bbox = NULL

  segs <- st_read(dsn = con, query = glue("SELECT * FROM vsr_segments WHERE mmsi = {mmsi};"))
  
  #if (!is.null(date_beg))
  # TODO: convert date_beg to date, and check that it's a date
  #segs <- filter(segs, date(beg_dt) >= date_beg)
  
  sf_geojson(segs) 
}



#* Echo back the input
#* @param msg The message to echo
#* @get /echo
function(msg=""){
  list(msg = paste0("The message is: '", msg, "'"))
}

#* Plot a histogram
#* @param n number of observations to feed into rnorm()
#* @png
#* @get /plot
function(n=100){
  rand <- rnorm(n)
  hist(rand)
}

#* Return the sum of two numbers
#* @param a The first number to add
#* @param b The second number to add
#* @post /sum
function(a, b){
  as.numeric(a) + as.numeric(b)
}

#* Redirect to the swagger interface 
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
