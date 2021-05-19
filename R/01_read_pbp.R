
library(tidyverse)
library(dplyr)
library(stringr)
library(arrow)


years_vec <- 2002:2020

# --- read from play_by_play_{year}.parquet ---------
# future::plan("multisession")
progressr::with_progress({
  p <- progressr::progressor(along = years_vec)
  pbp_games <- purrr::map_dfr(years_vec, function(y){
    pbp_g <- arrow::read_parquet(glue::glue("wnba/pbp/parquet/play_by_play_{y}.parquet"))
    p(sprintf("y=%s", as.integer(y)))
    return(pbp_g)
  })
})
