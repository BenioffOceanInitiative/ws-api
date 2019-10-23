# run API by sourcing this R script in RStudio
library(plumber)
r <- plumb("plumber.R")
r$run(port=8000)
# open in web browser: http://localhost:8000/__swagger__/
# for more, see https://www.rplumber.io/docs