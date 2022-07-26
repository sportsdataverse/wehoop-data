rm(list = ls())
gc()
.libPaths("C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")
Sys.setenv(R_LIBS="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman',lib=Sys.getenv("R_LIBS"), repos='http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))
suppressPackageStartupMessages(suppressMessages(library(qs, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc="C:\\Users\\saiem\\AppData\\Local\\R\\win-library\\4.2")))

options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- wehoop:::most_recent_wbb_season()
# --- compile into play_by_play_{year}.parquet ---------
wbb_pbp_games <- function(y){
  cli::cli_process_start("Starting wbb play_by_play parse for {y}!")
  pbp_g <- data.frame()
  pbp_list <- list.files(path = glue::glue('wbb/{y}/'))
  pbp_g <- purrr::map_dfr(pbp_list, function(x){
    pbp <- jsonlite::fromJSON(glue::glue('wbb/{y}/{x}'))$plays
    if(length(pbp)>1){
      pbp$game_id <- gsub(".json","", x)
    }
    return(pbp)
  })
  if(nrow(pbp_g)>0 && length(pbp_g)>1){
    pbp_g <- pbp_g %>% janitor::clean_names()
    pbp_g <- pbp_g %>% 
      dplyr::mutate(
        game_id = as.integer(.data$game_id)
      )
  }
  if(!('coordinate_x' %in% colnames(pbp_g)) && length(pbp_g)>1){
    pbp_g <- pbp_g %>% 
      dplyr::mutate(
        coordinate_x = NA_real_,
        coordinate_y = NA_real_
      )
  }
  ifelse(!dir.exists(file.path("wbb/pbp")), dir.create(file.path("wbb/pbp")), FALSE)
  ifelse(!dir.exists(file.path("wbb/pbp/csv")), dir.create(file.path("wbb/pbp/csv")), FALSE)
  if(nrow(pbp_g)>1){
    pbp_g <- pbp_g %>%
      wehoop:::make_wehoop_data("ESPN WBB Play-by-Play Information from wehoop data repository",Sys.time())
    data.table::fwrite(pbp_g, file=paste0("wbb/pbp/csv/play_by_play_",y,".csv.gz"))
    
    ifelse(!dir.exists(file.path("wbb/pbp/qs")), dir.create(file.path("wbb/pbp/qs")), FALSE)
    qs::qsave(pbp_g,glue::glue("wbb/pbp/qs/play_by_play_{y}.qs"))
    
    ifelse(!dir.exists(file.path("wbb/pbp/rds")), dir.create(file.path("wbb/pbp/rds")), FALSE)
    saveRDS(pbp_g,glue::glue("wbb/pbp/rds/play_by_play_{y}.rds"))
    
    ifelse(!dir.exists(file.path("wbb/pbp/parquet")), dir.create(file.path("wbb/pbp/parquet")), FALSE)
    arrow::write_parquet(pbp_g, glue::glue("wbb/pbp/parquet/play_by_play_{y}.parquet"))
  }
  sched <- data.table::fread(paste0('wbb/schedules/csv/wbb_schedule_',y,'.csv'))
  sched <- sched %>%
    dplyr::mutate(
      game_id = as.integer(.data$id),
      status_display_clock = as.character(.data$status_display_clock))
  if(nrow(pbp_g)>0){
    sched <- sched %>%
      dplyr::mutate(
        PBP = ifelse(.data$game_id %in% unique(pbp_g$game_id), TRUE,FALSE))
  } else {
    sched$PBP <- FALSE
  }
  
  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date))
  final_sched <- final_sched %>%
    wehoop:::make_wehoop_data("ESPN WBB Schedule Information from wehoop data repository",Sys.time())
  data.table::fwrite(final_sched,paste0("wbb/schedules/csv/wbb_schedule_",y,".csv"))
  qs::qsave(final_sched,glue::glue('wbb/schedules/qs/wbb_schedule_{y}.qs'))
  saveRDS(final_sched, glue::glue('wbb/schedules/rds/wbb_schedule_{y}.rds'))
  arrow::write_parquet(final_sched, glue::glue('wbb/schedules/parquet/wbb_schedule_{y}.parquet'))
  rm(sched)
  rm(final_sched)
  rm(pbp_g)
  gc()
  cli::cli_process_done(msg_done = "Finished wbb play_by_play parse for {y}!")
  return(NULL)
}

all_games <- purrr::map(years_vec, function(y){
  wbb_pbp_games(y)
})

sched_list <- list.files(path = glue::glue('wbb/schedules/csv/'))
sched_g <-  purrr::map_dfr(sched_list, function(x){
  sched <- data.table::fread(paste0('wbb/schedules/csv/',x)) %>%
    dplyr::mutate(
      status = as.character(.data$status)
    )
  return(sched)
})
sched_g <- sched_g %>%
  wehoop:::make_wehoop_data("ESPN WBB Schedule Information from wehoop data repository",Sys.time())
data.table::fwrite(sched_g %>% dplyr::arrange(desc(.data$date)), 'wbb_schedule_master.csv')
data.table::fwrite(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'wbb/wbb_games_in_data_repo.csv')
qs::qsave(sched_g %>% dplyr::arrange(desc(.data$date)), 'wbb_schedule_master.qs')
qs::qsave(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'wbb/wbb_games_in_data_repo.qs')
arrow::write_parquet(sched_g %>% dplyr::arrange(desc(.data$date)),glue::glue('wbb_schedule_master.parquet'))
arrow::write_parquet(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'wbb/wbb_games_in_data_repo.parquet')


rm(sched_g)
rm(sched_list)
rm(years_vec)
gc()