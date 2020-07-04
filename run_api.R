# run API by sourcing this R script in RStudio
library(plumber)

plumber_r <- "/share/github/ws-api/plumber.R"

plumber$new(plumber_r)$run(port=8888, host="0.0.0.0", swagger = T)

## Reference
#  - custom serializer: https://github.com/rstudio/plumber/issues/344#issuecomment-439492586

#r <- plumb("/srv/ws-api/plumber.R")
#r$run(port=8888)
#r$run(port=8888, host="0.0.0.0", swagger = T)

# open in web browser: http://localhost:8888/__swagger__/
# open in web browser: http://api.whalesafe.com
# for more, see https://www.rplumber.io/docs

# To stop on rstudio.whalesafe.net in Terminal:
#   ps -eaf | grep ws-api
#   # admin      494   442  0 Feb07 pts/0    00:00:08 /usr/local/lib/R/bin/exec/R --no-save --no-restore --slave --no-restore --file=/srv/ws-api/run_api.R
#   sudo kill -9 494
# To start on rstudio.whalesafe.net in Terminal:
#   sudo su root
#   Rscript /share/github/ws-api/run_api.R &
#   exit
