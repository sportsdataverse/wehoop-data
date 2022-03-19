import os, json
import re
import http
import time
import urllib.request
import pyreadr
import pyarrow as pa
import pandas as pd
import sportsdataverse as sdv
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path
path_to_schedules = "wbb/schedules"
final_file_name = "wbb_schedule_master.csv"

def download_schedule(season, path_to_schedules=None):
    df = sdv.wbb.espn_wbb_calendar(season)
    calendar = df['dateURL'].tolist()
    ev = pd.DataFrame()
    for d in calendar:
        date_schedule = sdv.wbb.espn_wbb_schedule(dates=d)
        ev = pd.concat([ev,date_schedule],axis=0, ignore_index=True)
    ev = ev[ev['season_type'].isin([2,3])]
    ev = ev.drop('competitors', axis=1)
    ev = ev.drop_duplicates(subset=['game_id'], ignore_index=True)
    if path_to_schedules is not None:
        ev.to_csv(f"{path_to_schedules}/csv/wbb_schedule_{season}.csv", index = False)
        ev.to_parquet(f"{path_to_schedules}/parquet/wbb_schedule_{season}.parquet", index = False)
        pyreadr.write_rds(f"{path_to_schedules}/rds/wbb_schedule_{season}.rds", ev)
    return ev
def main():

    years_arr = range(2022,2023)
    schedule_table = pd.DataFrame()
    for year in years_arr:
        year_schedule = download_schedule(year, path_to_schedules)
        schedule_table = pd.concat([schedule_table, year_schedule], axis=0)
    csv_files = [pos_csv.replace('.csv', '') for pos_csv in os.listdir(path_to_schedules) if pos_csv.endswith('.csv')]
    glued_data = pd.DataFrame()
    for index, js in enumerate(csv_files):
        x = pd.read_csv(f"{path_to_schedules}/{js}.csv", low_memory=False)
        glued_data = pd.concat([glued_data,x],axis=0)
    glued_data.to_csv(final_file_name, index=False)

if __name__ == "__main__":
    main()
