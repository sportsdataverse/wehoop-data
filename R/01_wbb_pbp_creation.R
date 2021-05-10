
library(tidyverse)
library(dplyr)
library(stringr)
library(arrow)

years_vec <- 2002:2021
# --- compile into play_by_play_{year}.parquet ---------
future::plan("multisession")
progressr::with_progress({
  p <- progressr::progressor(along = years_vec)
  pbp_games <- purrr::map_dfr(years_vec, function(y){
    
    pbp_g <- data.frame()
    pbp_list <- list.files(path = glue::glue('wbb/{y}/'))
    print(glue::glue('wbb/{y}/'))
    pbp_g <- furrr::future_map_dfr(pbp_list, function(x){
      pbp <- jsonlite::fromJSON(glue::glue('wbb/{y}/{x}'))$plays
      pbp$game_id <- gsub(".json","", x)
      return(pbp)
    })
    pbp_g <- pbp_g %>% janitor::clean_names()
    ifelse(!dir.exists(file.path("wbb/pbp")), dir.create(file.path("wbb/pbp")), FALSE)
    ifelse(!dir.exists(file.path("wbb/pbp/csv")), dir.create(file.path("wbb/pbp/csv")), FALSE)
    write.csv(pbp_g, file=gzfile(glue::glue("wbb/pbp/csv/play_by_play_{y}.csv.gz")), row.names = FALSE)
    ifelse(!dir.exists(file.path("wbb/pbp/rds")), dir.create(file.path("wbb/pbp/rds")), FALSE)
    saveRDS(pbp_g,glue::glue("wbb/pbp/rds/play_by_play_{y}.rds"))
    ifelse(!dir.exists(file.path("wbb/pbp/parquet")), dir.create(file.path("wbb/pbp/parquet")), FALSE)
    
    arrow::write_parquet(pbp_g, glue::glue("wbb/pbp/parquet/play_by_play_{y}.parquet"))
    p(sprintf("y=%s", as.integer(y)))
    return(pbp_g)
  })
})

df_game_ids <- as.data.frame(
  dplyr::distinct(pbp_games %>% 
                    dplyr::select(game_id, season, season_type, home_team_name, away_team_name))) %>% 
  dplyr::arrange(-season)

write.csv(df_game_ids, 'wbb/wbb_games_in_data_repo.csv',row.names=FALSE)
