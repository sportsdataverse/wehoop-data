# wehoop-data Copilot Instructions

**ARCHIVE — do not add new scrapers, builders, or datasets here.** New work
goes to the successor repos: wehoop-wbb-data, wehoop-wnba-data, and
wehoop-wnba-stats-data. This repo exists for history and for any
still-referenced release tags.

## Project Context

This repo is a **local-development orchestrator** that bundles together
copies of the WBB, WNBA, and WNBA Stats scrape + parse pipelines for
single-machine use. It is **not** part of the production data flow — the
public data path runs through `wehoop-{wbb,wnba}-raw`,
`wehoop-{wbb,wnba}-data`, and `wehoop-wnba-stats-data`, landing in
`sportsdataverse-data` releases that the `wehoop` R package consumes.
Outputs in `wbb/`, `wnba/`, `wnba_stats/` are committed here for
contributor convenience only.

There is no production CI here.

## Repository Workflow

- Branch from `main`; `main` is the default branch.
- Treat the R and Python scripts as mirrors of the production scripts in the per-source repos. Sync with upstream when those change.
- Do not point any external pipeline (CI, Discord bot, downstream consumer) at this repo's `main` as a data source — use `sportsdataverse-data` releases.

## Build & Development Commands

```sh
# Top-level fan-out (scrape stage only; parse stage is separate)
bash daily_womens_hoops_scraper.sh -s 2025 -e 2025 -r false

# Per-sport scrape + parse
bash daily_wbb_scraper.sh        -s 2025 -e 2025 -r false
bash daily_wbb_R_processor.sh    -s 2025 -e 2025 -r false
bash daily_wnba_scraper.sh       -s 2025 -e 2025 -r false
bash daily_wnba_R_processor.sh   -s 2025 -e 2025 -r false

# WNBA Stats (positional CLI args)
Rscript R/wnba_stats_01_pbp.R 2025 2025
```

`scrape_*_threaded.py` variants are higher-throughput drop-ins.

## Code Style

- Follow the parent package's R and Python style guides — see `wehoop/CLAUDE.md` and `sportsdataverse-py/CLAUDE.md`.
- Don't add bespoke parsing here — fix bugs in the upstream production scripts and re-mirror.
- The WNBA Stats path expects `proxylist.csv` at `../../proxylist.csv` (relative to script CWD).

## Cross-Repo References

- Conventions: <https://github.com/sportsdataverse/wehoop/blob/main/CLAUDE.md>
- Python SDK: <https://github.com/sportsdataverse/sportsdataverse-py/blob/main/CLAUDE.md>
- Production WBB:        <https://github.com/sportsdataverse/wehoop-wbb-raw>, <https://github.com/sportsdataverse/wehoop-wbb-data>
- Production WNBA:       <https://github.com/sportsdataverse/wehoop-wnba-raw>, <https://github.com/sportsdataverse/wehoop-wnba-data>
- Production WNBA Stats: <https://github.com/sportsdataverse/wehoop-wnba-stats-data>

## Conventional Commits

Use: `type(scope): description`. Common types: `chore`, `fix`, `docs`, `refactor`. Use `type!:` or a `BREAKING CHANGE:` footer for breaking changes.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
