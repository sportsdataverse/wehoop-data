#!/bin/bash
git pull
python scrape_wnba_schedules.py
python scrape_wnba_json.py
git pull
git add .
"C:\Program Files\R\R-4.2.0\bin\Rscript.exe" R/espn_wnba_01_pbp_creation.R
"C:\Program Files\R\R-4.2.0\bin\Rscript.exe" R/espn_wnba_02_team_box_creation.R
"C:\Program Files\R\R-4.2.0\bin\Rscript.exe" R/espn_wnba_03_player_box_creation.R
git pull
git add wnba/* wnba_schedule_master.parquet