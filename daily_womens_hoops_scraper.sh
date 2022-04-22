#!/bin/bash
bash daily_wnba_scraper.sh
bash daily_wbb_scraper.sh
git pull
git commit -m "WNBA and WBB Play-by-Play and Schedules update" || echo "No changes to commit"
git pull
git push