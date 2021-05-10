
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

df_game_ids <- dplyr::bind_rows(
  as.data.frame(dplyr::distinct(pbp_games %>% 
                                  dplyr::select(game_id, season, season_type, home_team_name, away_team_name))), game_ids) %>% 
  dplyr::distinct(game_id, season, season_type, home_team_name, away_team_name) %>% 
  as.data.frame() %>% 
  dplyr::arrange(-season,-game_id)

write.csv(df_game_ids, 'wnba/wnba_games_in_data_repo.csv')
