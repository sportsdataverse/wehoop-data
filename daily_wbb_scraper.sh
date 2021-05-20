python scrape_wbb_schedules.py
python scrape_wbb_json.py
git add .
git commit -m "daily update" || echo "No changes to commit"
"C:\Program Files\R\R-4.0.5\bin\Rscript.exe" R/01_wbb_pbp_creation.R
git add .
git commit -m "WBB Play-by-play and Schedules update" || echo "No changes to commit"
git push