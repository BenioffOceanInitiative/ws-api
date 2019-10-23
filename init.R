library(tidyverse)
library(here)

ais_url <- "https://raw.githubusercontent.com/mvisalli/shipr/master/data-raw/sbais.csv"
ais_csv <- here("data/sbais.csv")

if (!file.exists(ais_csv)){
  read_csv(ais_url) %>% 
    write_csv(ais_csv)
}

ais <- read_csv(ais_csv)
ais
View(ais)