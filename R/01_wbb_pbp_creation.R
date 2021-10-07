.libPaths("C:/Users/saiem/Documents/R/win-library/4.0")
Sys.setenv(R_LIBS="C:/Users/saiem/Documents/R/win-library/4.0")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman',lib=Sys.getenv("R_LIBS"), repos='http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(furrr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(future, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))

options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- 2021:wehoop:::most_recent_wbb_season()
# --- compile into play_by_play_{year}.parquet ---------
future::plan("multisession")
progressr::with_progress({
  p <- progressr::progressor(along = sort(years_vec, decreasing = TRUE))
  pbp_games <- purrr::map_dfr(sort(years_vec, decreasing = TRUE), function(y){
    
    pbp_g <- data.frame()
    pbp_list <- list.files(path = glue::glue('wbb/{y}/'))
    print(glue::glue('wbb/{y}/'))
    pbp_g <- furrr::future_map_dfr(pbp_list, function(x){
      pbp <- jsonlite::fromJSON(glue::glue('wbb/{y}/{x}'))$plays
      pbp$game_id <- gsub(".json","", x)
      return(pbp)
    })
    if(nrow(pbp_g)>0){
      pbp_g <- pbp_g %>% janitor::clean_names()
      pbp_g <- pbp_g %>% 
        dplyr::mutate(
          game_id = as.integer(.data$game_id)
        )
    }
    
    ifelse(!dir.exists(file.path("wbb/pbp")), dir.create(file.path("wbb/pbp")), FALSE)
    ifelse(!dir.exists(file.path("wbb/pbp/csv")), dir.create(file.path("wbb/pbp/csv")), FALSE)
    write.csv(pbp_g, file=gzfile(glue::glue("wbb/pbp/csv/play_by_play_{y}.csv.gz")), row.names = FALSE)
    ifelse(!dir.exists(file.path("wbb/pbp/rds")), dir.create(file.path("wbb/pbp/rds")), FALSE)
    saveRDS(pbp_g,glue::glue("wbb/pbp/rds/play_by_play_{y}.rds"))
    ifelse(!dir.exists(file.path("wbb/pbp/parquet")), dir.create(file.path("wbb/pbp/parquet")), FALSE)
    
    arrow::write_parquet(pbp_g, glue::glue("wbb/pbp/parquet/play_by_play_{y}.parquet"))
    sched <- read.csv(glue::glue('wbb/schedules/csv/wbb_schedule_{y}.csv'))
    sched <- sched %>%
      dplyr::mutate(
        status.displayClock = as.character(.data$status.displayClock),
        PBP = ifelse(.data$game_id %in% unique(pbp_g$game_id), TRUE,FALSE)
      )
    write.csv(dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date)),glue::glue('wbb/schedules/csv/wbb_schedule_{y}.csv'), row.names=FALSE)
    p(sprintf("y=%s", as.integer(y)))
    return(pbp_g)
  })
})
future::plan("multisession")
all_games <- purrr::map(years_vec, function(y){
  pbp_g <- pbp_games %>% 
    dplyr::filter(.data$season == y)
  
  ifelse(!dir.exists(file.path("wbb/pbp")), dir.create(file.path("wbb/pbp")), FALSE)
  ifelse(!dir.exists(file.path("wbb/pbp/csv")), dir.create(file.path("wbb/pbp/csv")), FALSE)
  write.csv(pbp_g, file=gzfile(glue::glue("wbb/pbp/csv/play_by_play_{y}.csv.gz")), row.names = FALSE)
  ifelse(!dir.exists(file.path("wbb/pbp/rds")), dir.create(file.path("wbb/pbp/rds")), FALSE)
  saveRDS(pbp_g,glue::glue("wbb/pbp/rds/play_by_play_{y}.rds"))
  ifelse(!dir.exists(file.path("wbb/pbp/parquet")), dir.create(file.path("wbb/pbp/parquet")), FALSE)
  arrow::write_parquet(pbp_g, glue::glue("wbb/pbp/parquet/play_by_play_{y}.parquet"))
  
})
sched_list <- list.files(path = glue::glue('wbb/schedules/csv/'))
sched_g <-  purrr::map_dfr(sched_list, function(x){
  sched <- read.csv(glue::glue('wbb/schedules/csv/{x}')) %>%
    dplyr::mutate(
      status.displayClock = as.character(.data$status.displayClock)
    )
  return(sched)
})


write.csv(sched_g %>% dplyr::arrange(desc(.data$date)), 'wbb_schedule_master.csv', row.names = FALSE)
write.csv(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'wbb/wbb_games_in_data_repo.csv', row.names = FALSE)

