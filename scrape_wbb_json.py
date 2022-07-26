import os, json
import re
import http
import pyreadr
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import sportsdataverse as sdv
import xgboost as xgb
import multiprocessing
import time
import urllib.request
from tqdm import tqdm
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path
path_to_raw = "wbb/json/raw"
path_to_final = "wbb/json/final"
path_to_errors = "wbb/errors"
run_processing = True
rescrape_all = True
def main():

    years_arr = range(2007,2023)
    schedule = pd.read_parquet('wbb_schedule_master.parquet', engine='auto', columns=None)
    schedule = schedule.sort_values(by=['season','season_type'], ascending = True)
    schedule["game_id"] = schedule["game_id"].astype(int)
    schedule = schedule[schedule['status_type_completed']==True]
    if rescrape_all == False:
        schedule_in_repo = pd.read_parquet('wbb/wbb_games_in_data_repo.parquet', engine='auto', columns=None)
        schedule_in_repo["game_id"] = schedule_in_repo["game_id"].astype(int)
        done_already = schedule_in_repo['game_id']
        schedule = schedule[~schedule['game_id'].isin(done_already)]
    schedule_with_pbp = schedule[schedule['season']>=2002]

    for year in years_arr:
        print("Scraping year {}...".format(year))
        games = schedule[(schedule['season']==year)].reset_index()['game_id'].tolist()
        print(f"Number of Games: {len(games)}")
        bad_schedule_keys = pd.DataFrame()
        # this finds our json files
        path_to_raw_json = "{}/".format(path_to_raw)
        path_to_final_json = "{}/".format(path_to_final)
        Path(path_to_raw_json).mkdir(parents=True, exist_ok=True)
        Path(path_to_final_json).mkdir(parents=True, exist_ok=True)
        # json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]

        for game in tqdm(games):
            try:
                g = sdv.wbb.espn_wbb_pbp(game_id = game, raw=True)
            except (TypeError) as e:
                print("TypeError: game_id = {}\n {}".format(game, e))
                continue
            except (IndexError) as e:
                print("IndexError: game_id = {}\n {}".format(game, e))
                continue
            except (KeyError) as e:
                print("KeyError: game_id = {}\n {}".format(game, e))
                continue
            except (ValueError) as e:
                print("DecodeError: game_id = {}\n {}".format(game, e))
                continue
            except (AttributeError) as e:
                print("AttributeError: game_id = {}\n {}".format(game, e))
                continue
            with open("{}{}.json".format(path_to_raw_json, game),'w') as f:
                json.dump(g, f, indent=2, sort_keys=False)
            if run_processing == True:
                try:
                    processed_data = sdv.wbb.wbb_pbp_disk(
                        game_id = game,
                        path_to_json = path_to_raw
                    )

                    result = sdv.wbb.helper_wbb_pbp(
                        game_id = game,
                        pbp_txt = processed_data
                    )
                    fp = "{}{}.json".format(path_to_final_json, game)
                    with open(fp,'w') as f:
                        json.dump(result, f, indent=2, sort_keys=False)
                except (IndexError) as e:
                    print("IndexError: game_id = {}\n {}".format(game, e))
                except (KeyError) as e:
                    print("KeyError: game_id = {}\n {}".format(game, e))
                    continue
                except (ValueError) as e:
                    print("DecodeError: game_id = {}\n {}".format(game, e))
                    continue
                except (AttributeError) as e:
                    print("AttributeError: game_id = {}\n {}".format(game, e))
                    continue

        print("Finished Scraping year {}...".format(year))
if __name__ == "__main__":
    main()
