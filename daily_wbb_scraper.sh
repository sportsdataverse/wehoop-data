#!/bin/bash
git pull
python scrape_wbb_schedules.py
python scrape_wbb_json.py
git pull
git add .
"C:\Program Files\R\R-4.2.0\bin\Rscript.exe" R/espn_wbb_01_pbp_creation.R
"C:\Program Files\R\R-4.2.0\bin\Rscript.exe" R/espn_wbb_02_team_box_creation.R
"C:\Program Files\R\R-4.2.0\bin\Rscript.exe" R/espn_wbb_03_player_box_creation.R
git pull
git add wbb/* wbb_schedule_master.parquet