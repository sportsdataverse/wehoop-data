# CLAUDE.md — wehoop-data Development Guide

**ARCHIVE — do not add new scrapers, builders, or datasets here.** New work
goes to the successor repos: wehoop-wbb-data, wehoop-wnba-data, and
wehoop-wnba-stats-data. This repo exists for history and for any
still-referenced release tags.

## Repo Overview

`wehoop-data` is a **local-development orchestrator** that bundles
together a copy of the women's-hoops scrape + parse pipelines (ESPN WBB,
ESPN WNBA, WNBA Stats) so a single contributor can refresh local data on
their machine without standing up multiple checkouts. It does NOT power
production data flow — outputs are committed back to this repo for
local-dev convenience, but the canonical data path runs through the
dedicated `*-raw` / `*-data` repos and lands in `sportsdataverse-data`
releases.

There is no production CI here; the daily refresh of the public data
artifacts is owned by the per-source repos.

## Pipeline Position

```
                       PRODUCTION (canonical)                                LOCAL DEV (this repo)
                       =====================                                 =====================

ESPN APIs ──[python]──▶ wehoop-{wnba,wbb}-raw ──▶ wehoop-{wnba,wbb}-data ──▶ sportsdataverse-data
                                                                                  │
                                                                                  ▼
                                                                              wehoop pkg

WNBA Stats API ─────────────────────────────────▶ wehoop-wnba-stats-data ────▶ sportsdataverse-data
                                                                                  │
                                                                                  ▼
                                                                              wehoop pkg
                                                                                                       wehoop-data [HERE]
                                                                                                       Local copy of all
                                                                                                       three pipelines for
                                                                                                       single-machine use.
                                                                                                       NOT the production
                                                                                                       data path.
```

## Build & Development Commands

The root-level shell scripts orchestrate per-sport scrape + parse passes.
Outputs are written to `wbb/`, `wnba/`, `wnba_stats/` and committed back
to this repo for local convenience.

```sh
# Full women's-hoops refresh (WNBA + WBB scrape + parse)
bash daily_womens_hoops_scraper.sh -s 2025 -e 2025 -r false

# Per-sport: scrape (Python) then parse (R)
bash daily_wbb_scraper.sh        -s 2025 -e 2025 -r false
bash daily_wbb_R_processor.sh    -s 2025 -e 2025 -r false

bash daily_wnba_scraper.sh       -s 2025 -e 2025 -r false
bash daily_wnba_R_processor.sh   -s 2025 -e 2025 -r false

# Direct calls
python3 scrape_wbb_schedules.py  -s 2025 -e 2025 -r false
python3 scrape_wbb_json.py       -s 2025 -e 2025 -r false
python3 scrape_wnba_schedules.py -s 2025 -e 2025 -r false
python3 scrape_wnba_json.py      -s 2025 -e 2025 -r false

Rscript R/espn_wbb_01_pbp_creation.R         -s 2025 -e 2025
Rscript R/espn_wbb_02_team_box_creation.R    -s 2025 -e 2025
Rscript R/espn_wbb_03_player_box_creation.R  -s 2025 -e 2025
Rscript R/espn_wnba_01_pbp_creation.R        -s 2025 -e 2025
Rscript R/espn_wnba_02_team_box_creation.R   -s 2025 -e 2025
Rscript R/espn_wnba_03_player_box_creation.R -s 2025 -e 2025
Rscript R/wnba_stats_01_pbp.R 2025 2025      # positional args
```

`scrape_*_threaded.py` variants are higher-throughput drop-in replacements
for the single-threaded scrapers.

## Project Structure

```
R/
  espn_wbb_0[1-3]_*.R               # Mirror of wehoop-wbb-data/R/
  espn_wnba_0[1-3]_*.R              # Mirror of wehoop-wnba-data/R/
  wnba_stats_00_proxy.R             # Proxy helper for WNBA Stats calls
  wnba_stats_01_pbp.R               # Mirror of wehoop-wnba-stats-data/R/
  utils.R                           # Shared helpers
scrape_wbb_*.py                     # Mirror of wehoop-wbb-raw/python/
scrape_wnba_*.py                    # Mirror of wehoop-wnba-raw/python/
wnba_pbp_creation.py                # Mirror of wehoop-wnba-raw/python/
daily_womens_hoops_scraper.sh       # Top-level fan-out: WNBA + WBB
daily_wbb_scraper.sh                # WBB scrape (Python)
daily_wbb_R_processor.sh            # WBB parse (R)
daily_wnba_scraper.sh               # WNBA scrape (Python)
daily_wnba_R_processor.sh           # WNBA parse (R)
themes/                             # ggplot themes for ad-hoc plotting
wbb/, wnba/, wnba_stats/            # Output staging for local artifacts
*_schedule_master.{csv,parquet,qs,rds}  # Local schedule master tables
```

The R scripts and Python scripts here are **copies** of the production
scripts in `wehoop-{wbb,wnba}-{raw,data}` and `wehoop-wnba-stats-data`.
When the upstream production scripts change, this repo can drift.

## Cross-Repo References

- Shared coding conventions, R style, ESPN wrapper pattern, WNBA Stats notes: <https://github.com/sportsdataverse/wehoop/blob/main/CLAUDE.md>
- Production WBB scrape: <https://github.com/sportsdataverse/wehoop-wbb-raw>
- Production WBB parse + upload: <https://github.com/sportsdataverse/wehoop-wbb-data>
- Production WNBA scrape: <https://github.com/sportsdataverse/wehoop-wnba-raw>
- Production WNBA parse + upload: <https://github.com/sportsdataverse/wehoop-wnba-data>
- Production WNBA Stats parse + upload: <https://github.com/sportsdataverse/wehoop-wnba-stats-data>

## Project-Specific Gotchas

- **This is a LOCAL DEV orchestrator, NOT production CI.** Outputs committed here are for development convenience; the public data flow goes through the dedicated `-raw → -data → sportsdataverse-data` pipeline. Do not rely on this repo's outputs as a source of truth.
- The R and Python scripts here are mirrors of production scripts in the per-source repos. Keep them in sync with the upstream production scripts (this repo is the integration view, not the authoritative copy).
- The WNBA Stats path needs `proxylist.csv` two levels up from this repo (`../../proxylist.csv`) to avoid throttling on `stats.wnba.com`.
- There is no `.github/workflows/` directory in this repo by design; production CI lives in the per-source repos.
- `daily_womens_hoops_scraper.sh` only fans out the scrape stage (WNBA + WBB scrapers); the parse stage must be invoked separately via the `*_R_processor.sh` scripts.

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
chore: refresh local mirror of espn_wnba_01_pbp_creation.R
fix(local): align proxylist.csv path resolution with production scripts
docs: clarify local-dev-only status in README
```

Prefer scoped subjects. Use `type!:` or a `BREAKING CHANGE:` footer for
breaking changes. Split unrelated work into separate commits.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
