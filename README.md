# Movie Library: CLI Tools

## Overview
- Use Jellyfin as the source of truth to list low‑resolution movies with strong critic scores.
- Enrich those titles via YTS to find higher‑quality releases and magnet links (no downloading).

## Quick Start
- Setup (script): `source scripts/setup.sh`  (keeps venv active)
- Setup (manual): `python3 -m venv .venv && source .venv/bin/activate && pip install -e .`
- Jellyfin: `python -m cli jellyfin --min-rt 6` (reads env `JELLYFIN_API_KEY`/`JELLYFIN_BASE_URL` from direnv `.envrc`)
- YTS enrich: `python -m cli yts-jf --verbose`

## CLI
- Jellyfin: `python -m cli jellyfin --min-rt 6 --verbose` (writes `data/jf_lowres_rt.csv`)
- YTS enrich (from Jellyfin CSV): `python -m cli yts-jf --verbose` (writes `data/yts_lowq.csv`)
- YTS search: `python -m cli yts-search --key "matrix"` (table of matches or details by `--id`)
- YTS TUI browser: `python -m cli yts-ui --key "matrix"` (navigate matches, Enter for details)
- Add magnets to Transmission GUI from CSV: `python -m cli add data/yts_lowq.csv` (expects `magnet` column)
- YTS mirrors: set `YTS_API_BASE` to a working mirror (comma-separated). Defaults include `https://www.yts-official.to/api/v2` first.
- Entry point: `movie-library-cli` provides the same commands. Short alias: `ml` works the same.

### Jellyfin (low‑res + high RT)
- Find movies below 720p with strong Rotten Tomatoes critic score.
- Example: `python -m cli jellyfin --min-rt 6` (writes `data/jf_lowres_rt.csv`).
- Options:
  - Configure base URL via env `JELLYFIN_BASE_URL` (default `http://localhost:8096`). Use direnv `.envrc` or export vars.
  - `--max-height` (default `719` for “< 720p”)
  - `--min-rt` (values ≤10 treated as 10‑point scale → percent)
  - `--out-csv` (defaults to `data/jf_lowres_rt.csv`)
  - `--verbose` to log requests

## Artifacts
- `data/jf_lowres_rt.csv` — Jellyfin output including `name,year,critic_rating,max_height,jellyfin_id,imdb_id,tmdb_id`.
- `data/yts_lowq.csv` — YTS enrichment output with available qualities and a suggested magnet.

## Matching Strategy
- Identification: uses Jellyfin’s metadata (including IMDb/TMDb IDs) instead of filename heuristics.
- YTS matching: prefers IMDb ID when present; otherwise falls back to title+year with fuzzy matching.

## Configuration
- Project-level config lives in direnv `.envrc`. To avoid repeated `direnv allow` prompts, keep `.envrc` stable and put personal values in `.envrc.local`.
  - `JELLYFIN_BASE_URL`: default `http://localhost:8096`
  - `JELLYFIN_API_KEY`: required API token
  - `YTS_API_BASE`: required YTS domain (site root or API path), e.g., `https://www.yts-official.to`
  - After edits: `direnv allow` (first time) then only `direnv reload` when changing `.envrc.local`.

## CSV Schemas
- `jf_lowres_rt.csv`:
  - `name, year, critic_rating, critic_summary, max_height, jellyfin_id, imdb_id, tmdb_id`
- `yts_lowq.csv`:
  - Copies Jellyfin columns and adds `yts_title, yts_year, yts_url, yts_quality_available, yts_next_quality, magnet`

## Troubleshooting
- YTS DNS/blocks: set `YTS_API_BASE` to a working mirror and rerun.
- Jellyfin 400 on `/Users/Me`: ensure `JELLYFIN_API_KEY` is valid; try `curl -H "X-Emby-Token: $JELLYFIN_API_KEY" "$JELLYFIN_BASE_URL/System/Info/Public"`.
- SSL warning on macOS (LibreSSL): harmless over HTTP; ignored for local Jellyfin.

## Agents Guide
- Contracts, defaults, and full heuristics live in `AGENTS.md`.
