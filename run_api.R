# run API by sourcing this R script in RStudio
library(plumber)
r <- plumb("plumber.R")
#r$run(port=8888, host="0.0.0.0")
r$run(port=8888)
# open in web browser: http://localhost:8888/__swagger__/
# for more, see https://www.rplumber.io/docs