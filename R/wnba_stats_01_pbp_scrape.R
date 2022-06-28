rm(list = ls())
gc()
.libPaths("C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0")
Sys.setenv(R_LIBS="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman',lib=Sys.getenv("R_LIBS"), repos='http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(tidyr, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(janitor, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(wehoop, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(future, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(furrr, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(stringr, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(tibble, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))
suppressPackageStartupMessages(suppressMessages(library(zoo, lib.loc="C:\\Users\\sgilani\\AppData\\Local\\Programs\\R\\R-4.2.0\\library")))

options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- 2017:wehoop::most_recent_wnba_season()

# --- wnba_stats_pbp_with_lineups ---------
wnba_stats_pbp_with_lineups <- function(y){
  
  cli::cli_process_start("Starting WNBA Stats (Data API) play-by-play parse for {y}!")
  player_logs <- wehoop::wnba_leaguegamelog(season = y, player_or_team = "P") %>%  # change season here
    purrr::pluck("LeagueGameLog") %>%
    janitor::clean_names() %>%
    dplyr::mutate(team_location = ifelse(stringr::str_detect(matchup, "\\@"), "away", "home"),
                  dplyr::across(c("player_id", "team_id", "game_id"), as.integer))
  
  function_pbp <- function(x){
    wehoop::wnba_data_pbp(x) %>%
      dplyr::mutate(game_id = x)
  }
  
  games <- player_logs %>%
    dplyr::distinct(game_id) %>%
    dplyr::pull(game_id)
  
  player_games <- player_logs %>%
    dplyr::distinct(season_id, game_id, team_id, team_abbreviation, player_id, player_name) %>% 
    dplyr::mutate(game_id = as.integer(game_id)) 
  
  future::plan(future::multicore)
  wnba_pbp_df_raw <- purrr::map_df(games, function_pbp)
  
  suppressWarnings(
    wnba_pbp_df_raw <- wnba_pbp_df_raw %>%
      dplyr::mutate(dplyr::across(c("epid", "opid", "game_id"), as.integer)) 
  )
  wnba_pbp_df <- wnba_pbp_df_raw %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(pid = player_id, player1 = player_name),
                     by = "pid") %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(epid = player_id, player2 = player_name),
                     by = "epid") %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(opid = player_id, player3 = player_name),
                     by = "opid") %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(tid = team_id, slug_team = team_abbreviation),
                     by = "tid") %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(oftid = team_id, off_slug_team = team_abbreviation),
                     by = "oftid") %>%
    dplyr::select(dplyr::any_of(
      c("game_id", "period", "clock" = "cl", "number_event" = "evt", 
        "msg_type" = "etype", "act_type" = "mtype", 
        "slug_team", "off_slug_team", 
        "player1", "player2", "player3", 
        "description" = "de", "desc_value" = "opt1", "opt2", 
        "hs", "vs", "ord", 
        "locX", "locY"))) %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(game_id, slug_team = team_abbreviation, team_location) %>%
                       tidyr::pivot_wider(names_from = team_location,
                                          values_from = slug_team,
                                          names_prefix = "team_"),
                     by = "game_id") %>%
    dplyr::as_tibble()
  
  wnba_pbp_df <- wnba_pbp_df %>%
    dplyr::mutate(number_original = number_event) %>%
    tidyr::separate(clock, into = c("min_remain", "sec_remain"), sep = ":", remove = FALSE, convert = TRUE) %>%
    dplyr::mutate(secs_left_qtr = (min_remain * 60) + sec_remain) %>%                       
    dplyr::mutate(secs_start_qtr = dplyr::case_when(                                                                        
      period %in% c(1:5) ~ (period - 1) * 600,
      TRUE ~ 2400 + (period - 5) * 300
    )) %>%
    dplyr::mutate(secs_passed_qtr = ifelse(period %in% c(1:4), 600 - secs_left_qtr, 300 - secs_left_qtr),  
                  secs_passed_game = secs_passed_qtr + secs_start_qtr) %>%
    dplyr::arrange(game_id, secs_passed_game) %>%
    dplyr::filter(msg_type != 18) %>%     # instant replay
    dplyr::group_by(game_id) %>%
    dplyr::mutate(number_event = dplyr::row_number()) %>%  # new numberEvent column with events in the right order
    dplyr::ungroup() %>%
    dplyr::select(-c(dplyr::contains("remain"), secs_left_qtr, secs_start_qtr, secs_passed_qtr)) %>%
    dplyr::arrange(game_id, number_event) %>%
    dplyr::mutate(shot_pts = desc_value * ifelse(msg_type %in% c(1:3) & !stringr::str_detect(description, "Missed"), 1, 0)) %>%
    dplyr::group_by(game_id) %>%
    dplyr::mutate(hs = cumsum(dplyr::coalesce(dplyr::if_else(slug_team == team_home, shot_pts, 0), 0)),
                  vs = cumsum(dplyr::coalesce(dplyr::if_else(slug_team == team_away, shot_pts, 0), 0))) %>%
    dplyr::ungroup()
  
  # Lineups -----------------------------------------------------------------
  
  # players that were subbed in or out in quarter
  players_subbed <- wnba_pbp_df %>%
    dplyr::filter(msg_type == 8) %>%
    dplyr::select(game_id, period, number_event, 
                  player_in = player2,  
                  player_out = player1,
                  description) %>%
    tidyr::pivot_longer(cols = dplyr::starts_with("player"),
                        names_to = "in_out",
                        values_to = c("player_name"),
                        names_prefix = "player_") %>%
    dplyr::arrange(game_id, period, number_event) %>%
    dplyr::distinct(game_id, period, player_name, .keep_all = TRUE) %>%
    dplyr::distinct(game_id, period, player_name, in_out) %>%
    dplyr::mutate(starter = ifelse(in_out == "out", 1, 0)) %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(game_id=as.integer(game_id), player_id, player_name),
                     by=c("game_id","player_name"))
  
  # find every player that contributed to pbp in quarter and remove those that were subbed. The ones remaining played the entire quarter. Then add the players that were subbed and started (first sub was out)
  starters_quarters <- wnba_pbp_df %>%
    dplyr::filter(!(msg_type == 6 & act_type %in% c(11, 12, 16, 18, 30))) %>%
    dplyr::filter(!msg_type %in% c(9, 11)) %>% # timeout and ejection
    dplyr::select(game_id, period, dplyr::starts_with("player")) %>%
    tidyr::pivot_longer(cols = dplyr::starts_with("player")) %>%
    dplyr::filter(!is.na(value),
                  value != 0) %>%
    dplyr::distinct(game_id, period, player_name = value) %>%
    dplyr::anti_join(players_subbed, by = c("game_id", "period", "player_name")) %>%
    dplyr::bind_rows(players_subbed %>%
                       dplyr::filter(starter == 1)) %>%
    dplyr::transmute(game_id, period, player_name) %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(game_id = as.integer(game_id), player_id = as.integer(player_id), player_name, slug_team = team_abbreviation),
                     by = c("game_id", "player_name"))
  
  # see if there are quarters when couldn't find 5 starters for team (player played entire quarter and didn't contribute to pbp)
  qtrs_missing <- starters_quarters %>%
    dplyr::count(game_id, period, slug_team) %>%
    dplyr::filter(n != 5)
  
  # Find missing starter from period
  missing_st_qtr <- function(game_id_miss, period_miss){
    wehoop::wnba_boxscoretraditionalv2(game_id = game_id_miss, start_period = period_miss, end_period = period_miss, range_type = 1) %>%
      purrr::pluck("PlayerStats") %>%
      dplyr::mutate(period = period_miss) %>%
      dplyr::filter((period_miss > 4 & MIN == "5:00") | (period_miss <= 4 & MIN == "10:00")) %>%
      dplyr::mutate(dplyr::across(c(FGM:PLUS_MINUS), as.integer)) %>%
      dplyr::filter(FGA + FTA + REB + AST + STL + BLK + TO + PF == 0)
  }
  
  missing_starters <- purrr::map2_df(qtrs_missing$game_id, qtrs_missing$period, missing_st_qtr)
  
  # put together table with starter for every team in every period, adding missing starters
  if(nrow(missing_starters)>0){
    starters_quarters <- starters_quarters %>%
      dplyr::bind_rows(missing_starters %>%
                         janitor::clean_names() %>%
                         dplyr::transmute(game_id = as.integer(game_id), period, player_id = as.integer(player_id), player_name, slug_team = team_abbreviation)) 
  }
  starters_quarters <- starters_quarters %>%
    dplyr::arrange(game_id, period, slug_team) %>%
    dplyr::group_by(game_id, period, slug_team) %>%
    dplyr::mutate(lineup_start = paste(sort(unique(player_name)), collapse = ", ")) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(game_id = as.integer(game_id), slug_team = team_abbreviation, team_location),
                     by = c("game_id","slug_team"))
  
  # starters_quarters %>% count(str_count(lineup_start, ","))
  
  # find lineup before and after every sub
  lineup_subs <- wnba_pbp_df %>%
    dplyr::filter(msg_type == 8) %>%
    dplyr::left_join(starters_quarters, by = c("game_id", "period", "slug_team")) %>%
    dplyr::select(game_id, number_event, period, clock, slug_team, player_out = player1, player_in = player2, 
                  team_location, lineup_before = lineup_start) %>%
    dplyr::group_by(game_id, period, slug_team) %>%
    dplyr::mutate(lineup_before = ifelse(dplyr::row_number() == 1, lineup_before, NA_character_)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(lineup_before = stringr::str_split(lineup_before, ", ")) %>% 
    dplyr::arrange(game_id, number_event) %>%
    dplyr::group_by(game_id, period, slug_team) %>%
    dplyr::mutate(lineup_after = purrr::accumulate2(player_in, player_out, ~dplyr::setdiff(c(..1, ..2), ..3), .init = lineup_before[[1]])[-1],
                  lineup_before = dplyr::coalesce(lineup_before, dplyr::lag(lineup_after))) %>%
    dplyr::ungroup() %>% 
    dplyr::mutate(dplyr::across(dplyr::starts_with("lineup"), ~ purrr::map_chr(., ~ paste(.x, collapse = ", "))))
  
  # add lineup to every event of pbp
  lineup_game <- wnba_pbp_df %>%
    dplyr::left_join(starters_quarters %>%
                       dplyr::select(-slug_team) %>%
                       tidyr::pivot_wider(names_from = team_location,
                                          values_from = lineup_start,
                                          values_fn={dplyr::first},
                                          names_prefix = "lineup_start_") %>%
                       dplyr::mutate(msg_type = 12),
                     by = c("game_id", "period", "msg_type")) %>%
    dplyr::left_join(lineup_subs %>%
                       dplyr::select(-c(clock, dplyr::starts_with("player"))) %>%
                       dplyr::distinct(game_id, number_event, period, slug_team, team_location, lineup_before, lineup_after) %>% 
                       tidyr::pivot_wider(names_from = team_location,
                                          values_from = dplyr::starts_with("lineup"),
                                          values_fn = {dplyr::first}),
                     by = c("game_id", "period", "number_event", "slug_team")) %>%
    dplyr::mutate(dplyr::across(c(lineup_before_home, lineup_after_home), ~ ifelse(!is.na(lineup_start_home), lineup_start_home, .)),
                  dplyr::across(c(lineup_before_away, lineup_after_away), ~ ifelse(!is.na(lineup_start_away), lineup_start_away, .))) %>%
    dplyr::group_by(game_id, period) %>%
    dplyr::mutate(lineup_home = zoo::na.locf(lineup_after_home, na.rm = FALSE),
                  lineup_away = zoo::na.locf(lineup_after_away, na.rm = FALSE),
                  lineup_home = dplyr::coalesce(lineup_home, zoo::na.locf(lineup_before_home, fromLast = TRUE, na.rm = FALSE)),
                  lineup_away = dplyr::coalesce(lineup_away, zoo::na.locf(lineup_before_away, fromLast = TRUE, na.rm = FALSE))) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(lineup_home = purrr::map_chr(stringr::str_split(lineup_home, ", "), ~ paste(sort(.), collapse = ", ")),
                  lineup_away = purrr::map_chr(stringr::str_split(lineup_away, ", "), ~ paste(sort(.), collapse = ", "))) %>%
    dplyr::select(-c(dplyr::starts_with("lineup_start"), dplyr::starts_with("lineup_before"), dplyr::starts_with("lineup_after")))
  
  
  # Possessions -------------------------------------------------------------
  
  poss_initial <- lineup_game %>%
    dplyr::mutate(possession = dplyr::case_when(msg_type %in% c(1, 2, 5) ~ 1,
                                                msg_type == 3 & act_type %in% c(12, 15) ~ 1,
                                                TRUE ~ 0))
  
  # finding lane violations that are not specified
  lane_description_missing <- poss_initial %>%
    dplyr::group_by(game_id, secs_passed_game) %>%
    dplyr::filter(sum(msg_type == 3 & act_type == 10) > 0,
                  sum(msg_type == 6 & act_type == 2) > 0,
                  sum(msg_type == 7 & act_type == 3) > 0,
                  sum(msg_type == 1) == 0) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(possession = ifelse(msg_type == 3 & act_type == 10, 1, possession)) %>%
    dplyr::select(game_id, number_event, off_slug_team, possession)
  
  # identify turnovers from successful challenge + jump ball that are not specified
  jumpball_turnovers <- poss_initial %>%
    dplyr::group_by(game_id, period) %>%
    dplyr::mutate(prev_poss = zoo::na.locf0(ifelse(possession == 1, off_slug_team, NA_character_)),
                  next_poss = zoo::na.locf0(ifelse(possession == 1, off_slug_team, NA_character_), fromLast = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(game_id, secs_passed_game) %>%
    dplyr::mutate(team_reb_chall = sum(msg_type == 9) > 0 & sum(msg_type == 4 & is.na(player1)) > 0) %>% 
    dplyr::ungroup() %>%
    dplyr::filter(msg_type == 10 & act_type == 1 & 
                    dplyr::lag(msg_type) == 9 &
                    slug_team == dplyr::lag(slug_team) &
                    prev_poss == next_poss &
                    dplyr::lag(team_reb_chall) == FALSE) %>%
    dplyr::mutate(possession = 1) %>%
    dplyr::transmute(game_id, number_event, off_slug_team = ifelse(slug_team == team_home, team_away, team_home), possession) %>%
    dplyr::mutate(
      off_slug_team = as.character(off_slug_team),
      slug_team = as.character(off_slug_team))
  
  # identify and change consecutive possessions
  suppressWarnings(
    change_consec <- poss_initial %>%
      dplyr::rows_update(lane_description_missing, by = c("game_id", "number_event")) %>%
      dplyr::rows_update(jumpball_turnovers, by = c("game_id", "number_event")) %>%
      dplyr::filter(possession == 1 | (msg_type == 6 & act_type == 30)) %>%
      dplyr::group_by(game_id, period) %>%
      dplyr::filter(possession == dplyr::lead(possession) && off_slug_team == dplyr::lead(off_slug_team)) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(possession = 0) %>%
      dplyr::select(game_id, number_event, possession)
  )
  # replace in data
  poss_non_consec <- poss_initial %>%
    dplyr::rows_update(lane_description_missing, by = c("game_id", "number_event")) %>%
    dplyr::rows_update(jumpball_turnovers, by = c("game_id", "number_event")) %>%
    dplyr::rows_update(change_consec, by = c("game_id","number_event"))
  
  
  # find start of possessions
  start_possessions <- poss_non_consec %>%
    dplyr::filter((possession == 1 & (msg_type %in% c(1, 5, 10) | (msg_type == 3 & shot_pts > 0))) | (msg_type == 4 & act_type == 0 & desc_value == 0) | (msg_type == 3 & act_type == 10) | (msg_type == 6 & act_type == 6)) %>%
    dplyr::filter(!(msg_type == 3 & act_type == 10 & dplyr::lag(msg_type) == 6 & dplyr::lag(act_type) == 6)) %>%
    dplyr::filter(!(msg_type == 6 & act_type == 6)) %>%
    dplyr::group_by(game_id, secs_passed_game, slug_team) %>%
    dplyr::mutate(and1 = sum(msg_type == 1) > 0 & sum(msg_type == 3) > 0) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(start_poss = ifelse(and1 & msg_type %in% c(1, 3), NA_character_, clock),
                  number_event = ifelse(msg_type == 4, number_event, number_event + 1)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(!is.na(start_poss))
  
  # add start of possession column to table
  poss_non_consec <- poss_non_consec %>%
    dplyr::left_join(start_possessions %>%
                       dplyr::select(game_id, number_event, start_poss),
                     by = c("game_id", "number_event")) %>%
    dplyr::group_by(game_id, period) %>%
    dplyr::mutate(start_poss = ifelse(dplyr::row_number() == 1, clock, start_poss),
                  start_poss = zoo::na.locf(start_poss)) %>%
    dplyr::ungroup()
  
  
  ##### Adding extra possessions
  
  addit_poss <- poss_non_consec %>%
    dplyr::filter(msg_type %in% c(1:5) & !(msg_type == 3 & act_type %in% c(16, 18:19, 20, 27:29, 25:26)) & !(msg_type == 4 & act_type == 1)) %>%
    dplyr::group_by(game_id, period) %>%
    dplyr::filter(dplyr::row_number() == max(dplyr::row_number())) %>%
    dplyr::ungroup() %>%
    dplyr::filter(clock != "00:00.0" & !(msg_type == 4 & desc_value == 1)) %>%
    dplyr::transmute(game_id, period, start_poss = clock, possession = 1,
                     off_slug_team = ifelse(msg_type == 4 | msg_type == 3 & act_type %in% c(19, 20, 29, 26), 
                                            slug_team, 
                                            ifelse(slug_team == team_home, team_away, team_home)),
                     msg_type = 99, act_type = 0, number_original = 0, description = "Last possession of quarter") %>%
    dplyr::left_join(poss_non_consec %>%
                       dplyr::filter(msg_type == 13) %>%
                       dplyr::select(-c(number_original, msg_type, act_type, start_poss,
                                        description, possession, off_slug_team)),
                     by = c("game_id", "period")) %>%
    dplyr::mutate(number_event = number_event - 0.5,
                  slug_team = off_slug_team)
  
  
  # Adding extra possessions
  pbp_poss <- poss_non_consec %>%
    dplyr::bind_rows(addit_poss) %>%
    dplyr::arrange(game_id, number_event)
  
  # Editing free throws pts and poss position ------------------------------------------------------------
  
  # connecting free throws to fouls
  
  ## TECHNICALS
  ### find unidentified double technicals (instead of description showing double technical, there's one event for each but no FTs)
  unident_double_techs <- lineup_game %>%
    dplyr::filter(!msg_type %in% c(9, 11)) %>%   # ejection or timeout
    dplyr::filter((game_id == dplyr::lead(game_id) & secs_passed_game == dplyr::lead(secs_passed_game) & msg_type == 6 & act_type == 11 & msg_type == dplyr::lead(msg_type) & act_type == dplyr::lead(act_type) & slug_team != dplyr::lead(slug_team)) | (game_id == dplyr::lag(game_id) & secs_passed_game == dplyr::lag(secs_passed_game) & msg_type == 6 & act_type == 11 & msg_type == dplyr::lag(msg_type) & act_type == dplyr::lag(act_type) & slug_team != dplyr::lag(slug_team))) %>%
    dplyr::transmute(game_id, secs_passed_game, slug_team, number_event, description = stringr::str_replace(description, "Technical", "Double Technical"))
  
  techs <- lineup_game %>%
    dplyr::rows_update(unident_double_techs, by = c("game_id", "secs_passed_game", "slug_team", "number_event")) %>%
    dplyr::filter(stringr::str_detect(description, "Technical|Defense 3 Second") & !stringr::str_detect(description, "Double Technical")) %>%
    dplyr::group_by(game_id, secs_passed_game, msg_type) %>%
    dplyr::mutate(sequence_num = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(game_id, secs_passed_game, number_event, msg_type = ifelse(msg_type == 3, "ft", "foul"), sequence_num) 
  techs_dups <- techs %>%
    dplyr::group_by(game_id, secs_passed_game, sequence_num, msg_type) %>%
    dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
    dplyr::filter(n > 1L) 
  techs <- techs %>% 
    tidyr::pivot_wider(names_from = msg_type,
                       values_from = number_event,
                       values_fn = {dplyr::first},
                       names_prefix = "number_event_")
  
  ## FLAGRANT - CLEAR PATH
  flagrant_clear <- lineup_game %>%
    dplyr::filter(msg_type == 3 & act_type %in% c(18:20, 25:26, 27:29)) %>%
    dplyr::select(game_id, secs_passed_game, number_event_ft = number_event, slug_team) %>%
    dplyr::left_join(lineup_game %>%
                       dplyr::filter(msg_type == 6 & act_type %in% c(9, 14, 15)) %>%
                       dplyr::transmute(game_id, secs_passed_game, number_event_foul = number_event, 
                                        slug_team = ifelse(slug_team == team_home, team_away, team_home)),
                     by = c("game_id", "secs_passed_game", "slug_team"))
  
  ## REGULAR FOULS
  other_fouls <- lineup_game %>%
    dplyr::filter(msg_type %in% c(3, 6)) %>%
    dplyr::filter(!stringr::str_detect(description, "Technical|Defense 3 Second"),
                  !(msg_type == 3 & act_type %in% c(18:20, 25:26, 27:29)),
                  !(msg_type == 6 & act_type %in% c(9, 14, 15)))
  
  regular_fouls <- other_fouls %>%
    dplyr::filter(msg_type == 3) %>%
    dplyr::select(game_id, secs_passed_game, number_event_ft = number_event, slug_team, player_fouled = player1) %>%
    dplyr::left_join(other_fouls %>%
                       dplyr::filter(msg_type == 6 & stringr::str_detect(description, "FT")) %>%
                       dplyr::transmute(game_id, secs_passed_game, number_event_foul = number_event, player_fouled = player3,
                                        slug_team = ifelse(slug_team == team_home, team_away, team_home)),
                     by = c("game_id", "secs_passed_game", "slug_team", "player_fouled")) %>%
    dplyr::left_join(other_fouls %>%
                       dplyr::filter(msg_type == 6 & stringr::str_detect(description, "FT")) %>%
                       dplyr::transmute(game_id, secs_passed_game, number_event_foul_y = number_event, 
                                        number_event_foul = NA_integer_,
                                        slug_team = ifelse(slug_team == team_home, team_away, team_home)),
                     by = c("game_id", "secs_passed_game", "slug_team", "number_event_foul")) %>%
    dplyr::mutate(number_event_foul = dplyr::coalesce(number_event_foul, number_event_foul_y)) %>%
    dplyr::select(-number_event_foul_y)
  
  # putting everything together
  fouls_stats <- dplyr::bind_rows(regular_fouls, flagrant_clear, techs) %>%
    dplyr::select(game_id, secs_passed_game, number_event_ft, number_event_foul) %>%
    dplyr::left_join(pbp_poss %>%
                       dplyr::select(game_id, number_event_ft = number_event, slug_team, shot_pts, team_home, team_away, possession),
                     by = c("game_id", "number_event_ft")) %>%
    # dplyr::filter(is.na(shot_pts))  # test to see if there's nothing missing
    dplyr::group_by(game_id, slug_team, number_event = number_event_foul, team_home, team_away) %>%
    dplyr::summarise(total_fta = dplyr::n(),
                     total_pts = sum(shot_pts),
                     total_poss = sum(possession),
                     .groups = "keep") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(shot_pts_home = ifelse(slug_team == team_home, total_pts, 0),
                  shot_pts_away = ifelse(slug_team == team_away, total_pts, 0),
                  poss_home = ifelse(slug_team == team_home, total_poss, 0),
                  poss_away = ifelse(slug_team == team_away, total_poss, 0)) %>%
    dplyr::select(game_id, number_event, total_fta, shot_pts_home:poss_away)
  
  pbp_poss_final <- pbp_poss %>%
    # mutate(possession = ifelse(start_poss == "00:00.0", 0, possession)) %>%   # considering nba.com bug when play has wrong clock at 00:00.0 (correct would be to not have this line)
    dplyr::left_join(fouls_stats,
                     by = c("game_id", "number_event")) %>%
    dplyr::mutate(shot_pts_home = dplyr::coalesce(shot_pts_home, ifelse(msg_type == 1 & slug_team == team_home, shot_pts, 0)),
                  shot_pts_away = dplyr::coalesce(shot_pts_away, ifelse(msg_type == 1 & slug_team == team_away, shot_pts, 0)),
                  poss_home = dplyr::coalesce(poss_home, ifelse(msg_type != 3 & possession == 1 & slug_team == team_home, possession, 0)),
                  poss_away = dplyr::coalesce(poss_away, ifelse(msg_type != 3 & possession == 1 & slug_team == team_away, possession, 0))) %>%
    dplyr::group_by(game_id, period) %>%
    dplyr::mutate(secs_played = dplyr::lead(secs_passed_game) - secs_passed_game,
                  secs_played = dplyr::coalesce(secs_played, 0)) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(player_logs %>%
                       dplyr::distinct(game_id = as.integer(game_id), game_date = as.Date(game_date)),
                     by = "game_id")
  
  
  # Add garbage time --------------------------------------------------------
  
  pbp_final_gt <- pbp_poss_final %>%
    dplyr::left_join(starters_quarters %>%
                       dplyr::filter(period == 1) %>%
                       dplyr::select(-c(period, slug_team)) %>%
                       tidyr::pivot_wider(names_from = team_location,
                                          values_from = lineup_start,
                                          values_fn = {dplyr::first},
                                          names_prefix = "lineup_start_"),
                     by = c("game_id", "player_name", "player_id")) %>%
    dplyr::mutate(dplyr::across(c(dplyr::contains("lineup")), ~ stringr::str_split(., ", "), .names = "{.col}_list")) %>%
    dplyr::mutate(total_starters_home = purrr::map_int(purrr::map2(lineup_home_list, lineup_start_home_list, dplyr::intersect), length),
                  total_starters_away = purrr::map_int(purrr::map2(lineup_away_list, lineup_start_away_list, dplyr::intersect), length)) %>%
    dplyr::select(-dplyr::contains("list")) %>%
    dplyr::mutate(margin_before = dplyr::case_when(shot_pts > 0 & slug_team == team_home ~ abs(hs - shot_pts - vs),
                                                   shot_pts > 0 & slug_team == team_away ~ abs(vs - shot_pts - hs),
                                                   TRUE ~ abs(hs - vs))) %>%
    dplyr::mutate(garbage_time = dplyr::case_when(
      # score differential >= 25 for minutes 10-7.5:
      secs_passed_game >= 1800 & secs_passed_game < 1950 & margin_before >= 25 & total_starters_home + total_starters_away <= 2 & period == 4 ~ 1,
      # score differential >= 20 for minutes 7.5-5:
      secs_passed_game >= 1950 & secs_passed_game < 2100 & margin_before >= 20 & total_starters_home + total_starters_away <= 2 & period == 4 ~ 1,
      # score differential >= 10 for minutes 5 and under:
      secs_passed_game >= 2100 & margin_before >= 10 & total_starters_home + total_starters_away <= 2 & period == 4 ~ 1,
      TRUE ~ 0)) %>%
    dplyr::group_by(game_id) %>%
    dplyr::mutate(max_nongarbage = max(number_event[which(garbage_time == 0)])) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      garbage_time = ifelse(garbage_time == 1 & number_event < max_nongarbage, 0, garbage_time),
      def_slug_team = dplyr::case_when(
        off_slug_team == team_away ~ team_home,
        off_slug_team == team_home ~ team_away,
        TRUE ~ NA_character_)) %>%
    dplyr::select(-dplyr::any_of(c(dplyr::starts_with("lineup_start_"), "max_nongarbage", "opt2", "ord", "locX", "locY")))
  
  #--- Add player IDs to lineup columns -----
  suppressWarnings(
    pbp_final_gt <- pbp_final_gt %>% 
      dplyr::mutate(
        off_lineup = dplyr::case_when(
          off_slug_team == team_home ~ lineup_home,
          off_slug_team == team_away ~ lineup_away,
          TRUE ~ NA_character_),
        def_lineup = dplyr::case_when(
          off_slug_team == team_away ~ lineup_home,
          off_slug_team == team_home ~ lineup_away,
          TRUE ~ NA_character_)) %>% 
      tidyr::separate(off_lineup, into = c("off_player1", "off_player2", "off_player3", "off_player4", "off_player5"), sep = ", ", remove = FALSE, convert = TRUE) %>%
      tidyr::separate(def_lineup, into = c("def_player1", "def_player2", "def_player3", "def_player4", "def_player5"), sep = ", ", remove = FALSE, convert = TRUE)
  )
  pbp_final_gt_with_ids <- pbp_final_gt %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(off_player1_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","off_slug_team"="team_abbreviation","off_player1"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(off_player2_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","off_slug_team"="team_abbreviation","off_player2"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(off_player3_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","off_slug_team"="team_abbreviation","off_player3"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(off_player4_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","off_slug_team"="team_abbreviation","off_player4"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(off_player5_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","off_slug_team"="team_abbreviation","off_player5"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(def_player1_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","def_slug_team"="team_abbreviation","def_player1"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(def_player2_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","def_slug_team"="team_abbreviation","def_player2"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(def_player3_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","def_slug_team"="team_abbreviation","def_player3"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(def_player4_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","def_slug_team"="team_abbreviation","def_player4"="player_name")) %>% 
    dplyr::left_join(player_games %>% 
                       dplyr::rename(def_player5_id = player_id) %>% 
                       dplyr::select(-team_id,-season_id), 
                     by = c("game_id","def_slug_team"="team_abbreviation","def_player5"="player_name"))
  
  
  
  # Stats -----------------------------------------------------------
  
  lineup_stats_existing <- pbp_final_gt_with_ids %>%
    dplyr::group_by(game_id, slug_team) %>%
    dplyr::mutate(stint_home = ifelse(slug_team == team_home, cumsum(msg_type == 8) + 1, NA_integer_),
                  stint_away = ifelse(slug_team == team_away, cumsum(msg_type == 8) + 1, NA_integer_)) %>%
    dplyr::group_by(game_id) %>%
    dplyr::mutate(dplyr::across(dplyr::starts_with("stint"), ~ zoo::na.locf0(., fromLast = TRUE)),
                  dplyr::across(dplyr::starts_with("stint"), ~ zoo::na.locf(.))) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_longer(cols = dplyr::starts_with("lineup"),
                        names_to = "lineup_location",
                        values_to = "lineup",
                        names_prefix = "lineup_") %>%
    dplyr::filter(!is.na(lineup)) %>% 
    dplyr::mutate(pts_team = ifelse(lineup_location == "home", shot_pts_home, shot_pts_away),
                  pts_opp = ifelse(lineup_location == "away", shot_pts_home, shot_pts_away),
                  poss_team = ifelse(lineup_location == "home", poss_home, poss_away),
                  poss_opp = ifelse(lineup_location == "away", poss_home, poss_away),
                  slug_team = ifelse(lineup_location == "home", team_home, team_away),
                  slug_opp = ifelse(lineup_location == "away", team_home, team_away),
                  stint = ifelse(lineup_location == "home", stint_home, stint_away)) %>%
    dplyr::select(game_id, game_date, period, stint, number_event, msg_type, description, lineup, pts_team, pts_opp,
                  poss_team, poss_opp, secs_played, slug_team, slug_opp, garbage_time) %>%
    dplyr::group_by(game_id, game_date, period, stint, slug_team, slug_opp, lineup, garbage_time) %>%
    dplyr::summarise(dplyr::across(c(pts_team, pts_opp, poss_team, poss_opp, secs_played), sum), 
                     .groups = "keep") %>%
    dplyr::ungroup() 
  
  lineup_stats <- lineup_stats_existing %>%
    dplyr::filter(secs_played + poss_opp + poss_team + pts_opp + pts_team > 0) %>%
    dplyr::group_by(game_id, slug_team) %>%
    dplyr::mutate(stint = dplyr::row_number()) %>%
    dplyr::ungroup()
  
  all_name_lineups <- lineup_stats_existing %>% 
    dplyr::distinct(lineup) %>% 
    dplyr::mutate(lineup_id = dplyr::row_number())
  
  lineup_stats <- lineup_stats %>% 
    dplyr::left_join(all_name_lineups, by = "lineup")
  
  pbp_final_gt_with_ids <- pbp_final_gt_with_ids %>% 
    dplyr::left_join(all_name_lineups %>% 
                       dplyr::rename(off_lineup_id = lineup_id), 
                     by = c("off_lineup" = "lineup")) %>% 
    dplyr::left_join(all_name_lineups %>% 
                       dplyr::rename(def_lineup_id = lineup_id), 
                     by = c("def_lineup" = "lineup")) 
  all_name_lineups$season <- y
  lineup_stats$season <- y 
  lineup_stats_existing$season <- y 
  pbp_final_gt_with_ids$season <- y 
  
  pbp_final_gt_with_ids <- pbp_final_gt_with_ids  %>%
    wehoop:::make_wehoop_data("WNBA Stats Play-by-Play Information from wehoop data repository",Sys.time()) 
  
  
  #--- Write to disk -----
  ## all lineups
  all_name_lineups <- all_name_lineups %>%
    wehoop:::make_wehoop_data("WNBA Stats Lineups Index Information from wehoop data repository",Sys.time()) 
  
  ifelse(!dir.exists(file.path("wnba_stats/lineup_index")), dir.create(file.path("wnba_stats/lineup_index")), FALSE)
  ifelse(!dir.exists(file.path("wnba_stats/lineup_index/csv")), dir.create(file.path("wnba_stats/lineup_index/csv")), FALSE)
  data.table::fwrite(all_name_lineups, file=paste0("wnba_stats/lineup_index/csv/lineup_index_",y,".csv"))
  
  ifelse(!dir.exists(file.path("wnba_stats/lineup_index/qs")), dir.create(file.path("wnba_stats/lineup_index/qs")), FALSE)
  qs::qsave(all_name_lineups,glue::glue("wnba_stats/lineup_index/qs/lineup_index_{y}.qs"))
  
  ifelse(!dir.exists(file.path("wnba_stats/lineup_index/rds")), dir.create(file.path("wnba_stats/lineup_index/rds")), FALSE)
  saveRDS(all_name_lineups,glue::glue("wnba_stats/lineup_index/rds/lineup_index_{y}.rds"))
  
  ifelse(!dir.exists(file.path("wnba_stats/lineup_index/parquet")), dir.create(file.path("wnba_stats/lineup_index/parquet")), FALSE)
  arrow::write_parquet(all_name_lineups, glue::glue("wnba_stats/lineup_index/parquet/lineup_index_{y}.parquet"))
  
  lineup_stats <- lineup_stats %>% 
    wehoop:::make_wehoop_data("WNBA Stats Lineups Stats Information from wehoop data repository",Sys.time()) 
  
  ifelse(!dir.exists(file.path("wnba_stats/lineup_stats")), dir.create(file.path("wnba_stats/lineup_stats")), FALSE)
  ifelse(!dir.exists(file.path("wnba_stats/lineup_stats/csv")), dir.create(file.path("wnba_stats/lineup_stats/csv")), FALSE)
  data.table::fwrite(lineup_stats, file=paste0("wnba_stats/lineup_stats/csv/lineup_stats_",y,".csv"))
  
  ifelse(!dir.exists(file.path("wnba_stats/lineup_stats/qs")), dir.create(file.path("wnba_stats/lineup_stats/qs")), FALSE)
  qs::qsave(lineup_stats,glue::glue("wnba_stats/lineup_stats/qs/lineup_stats_{y}.qs"))
  
  ifelse(!dir.exists(file.path("wnba_stats/lineup_stats/rds")), dir.create(file.path("wnba_stats/lineup_stats/rds")), FALSE)
  saveRDS(lineup_stats,glue::glue("wnba_stats/lineup_stats/rds/lineup_stats_{y}.rds"))
  
  ifelse(!dir.exists(file.path("wnba_stats/lineup_stats/parquet")), dir.create(file.path("wnba_stats/lineup_stats/parquet")), FALSE)
  arrow::write_parquet(lineup_stats, glue::glue("wnba_stats/lineup_stats/parquet/lineup_stats_{y}.parquet"))
  
  ## pbp_final_gt
  ifelse(!dir.exists(file.path("wnba_stats/pbp")), dir.create(file.path("wnba_stats/pbp")), FALSE)
  ifelse(!dir.exists(file.path("wnba_stats/pbp/csv")), dir.create(file.path("wnba_stats/pbp/csv")), FALSE)
  data.table::fwrite(pbp_final_gt_with_ids, file=paste0("wnba_stats/pbp/csv/play_by_play_",y,".csv"))
  
  ifelse(!dir.exists(file.path("wnba_stats/pbp/qs")), dir.create(file.path("wnba_stats/pbp/qs")), FALSE)
  qs::qsave(pbp_final_gt_with_ids,glue::glue("wnba_stats/pbp/qs/play_by_play_{y}.qs"))
  
  ifelse(!dir.exists(file.path("wnba_stats/pbp/rds")), dir.create(file.path("wnba_stats/pbp/rds")), FALSE)
  saveRDS(pbp_final_gt_with_ids,glue::glue("wnba_stats/pbp/rds/play_by_play_{y}.rds"))
  
  ifelse(!dir.exists(file.path("wnba_stats/pbp/parquet")), dir.create(file.path("wnba_stats/pbp/parquet")), FALSE)
  arrow::write_parquet(pbp_final_gt_with_ids, glue::glue("wnba_stats/pbp/parquet/play_by_play_{y}.parquet"))
  
  cli::cli_process_done(msg_done = "Finished WNBA Stats (Data API) play-by-play parse for {y}!")
  
}

# ---- Year-level WNBA Play-by-Play with lineups---------

ls <- purrr::map(years_vec, function(.y){
  tictoc::tic()
  wnba_stats_pbp_with_lineups(y=.y)
  tictoc::toc()
})
