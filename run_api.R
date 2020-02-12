# run API by sourcing this R script in RStudio
library(plumber)
r <- plumb("/srv/ws-api/plumber.R")
#r$run(port=8888)
r$run(port=8888, host="0.0.0.0", swagger = T)
# open in web browser: http://localhost:8888/__swagger__/
# for more, see https://www.rplumber.io/docs

# To start on rstudio.ships4whales.org:
#   Rscript /srv/ws-api/run_api.R &
# To restart the process:
#   ps -eaf | grep ws-api
#   # admin      494   442  0 Feb07 pts/0    00:00:08 /usr/local/lib/R/bin/exec/R --no-save --no-restore --slave --no-restore --file=/srv/ws-api/run_api.R
#   kill -9 494