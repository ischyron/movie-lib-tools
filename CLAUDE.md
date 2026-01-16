# Claude AI Agent Guide

## Repository Overview
This is a movie library manager that:
- Queries Jellyfin for low-resolution movies with strong critic ratings
- Enriches movie data by searching YTS for higher-quality releases
- Generates magnet links for potential upgrades (no downloading)

## Key Files and Structure
```
.
├── CLAUDE.md          # Contracts, defaults, and heuristics (authoritative)
├── README.md          # High-level usage guide
├── cli.py             # Main CLI entry point
├── jellyfin.py        # Jellyfin API integration
├── yts.py             # YTS search and enrichment logic
├── data/
│   ├── jf_lowres_rt.csv    # Jellyfin query results
│   └── yts_lowq.csv        # YTS-enriched results with magnet links
├── scripts/           # One-off utility scripts
└── .envrc             # Environment configuration (direnv)
```

## Workflow

### 1. Jellyfin Query Phase
Query Jellyfin for low-resolution (<720p) movies with strong RT scores:
```bash
python -m cli jellyfin --min-rt 6 --verbose
```
- Outputs: `data/jf_lowres_rt.csv`
- Schema: `name,year,critic_rating,critic_summary,max_height,jellyfin_id,imdb_id,tmdb_id`
- Config: Uses `JELLYFIN_BASE_URL` and `JELLYFIN_API_KEY` from `.envrc`

### 2. YTS Enrichment Phase
Enrich Jellyfin results with YTS data:
```bash
python -m cli yts-jf --verbose
```
- Inputs: `data/jf_lowres_rt.csv`
- Outputs: `data/yts_lowq.csv`
- Adds columns: `yts_title,yts_year,yts_url,yts_quality_available,yts_next_quality,magnet`
- Config: Uses `YTS_API_BASE` from `.envrc` (supports comma-separated mirrors)

## Critical Design Principles

### 1. Jellyfin as Source of Truth
- NEVER use filename-based heuristics
- Trust Jellyfin's metadata for title, year, and provider IDs
- Prefer IMDb ID matching when available

### 2. Configuration Philosophy
- Project setup belongs in `.envrc` (not CLI flags)
- Required env vars:
  - `JELLYFIN_BASE_URL` (default: `http://localhost:8096`)
  - `JELLYFIN_API_KEY` (required)
  - `YTS_API_BASE` (required, supports comma-separated mirrors)
- Personal overrides go in `.envrc.local`
- Verbosity can be controlled via `MLM_VERBOSE` env var

### 3. Quality Threshold
- Low quality = strictly less than 720p (`max_height <= 719`)
- YTS enrichment skips items already at 720p or higher

### 4. YTS Resilience
- Mirror rotation on DNS/connection errors
- Exponential backoff with configurable retries
- Default timeout: 12s, slow threshold: 9s
- Handles non-JSON responses gracefully

## Common Patterns

### When Adding Features
1. Read the relevant CSV schemas in AGENTS.md
2. Check if env vars are needed (add to `.envrc.example` if so)
3. Avoid introducing CLI flags for project setup
4. Maintain backward compatibility with CSV schemas

### When Fixing Bugs
1. Check if the issue affects matching logic (jellyfin.py or yts.py)
2. Verify that Jellyfin metadata is being trusted over filenames
3. Test with verbose mode to see request/response details
4. Consider mirror rotation logic for YTS failures

### When Refactoring
1. Keep CSV schemas unchanged unless absolutely necessary
2. Preserve the separation between scan (Jellyfin) and enrich (YTS) phases
3. Don't add unnecessary abstractions for one-time operations
4. Update AGENTS.md if defaults or contracts change

## Testing Workflow

### Manual Testing
```bash
# 1. Setup environment
source .venv/bin/activate
direnv allow  # first time only

# 2. Query Jellyfin
python -m cli jellyfin --min-rt 6 --verbose

# 3. Enrich with YTS
python -m cli yts-jf --verbose

# 4. Verify outputs
head -5 data/jf_lowres_rt.csv
head -5 data/yts_lowq.csv
```

### Debugging YTS Issues
```bash
# Test with specific mirror
YTS_API_BASE=https://yts.mx python -m cli yts-jf --verbose

# Use multiple mirrors (comma-separated)
YTS_API_BASE=https://yts.rs,https://yts.mx python -m cli yts-jf --verbose
```

## Common Errors and Solutions

### Error: "set JELLYFIN_API_KEY"
**Cause:** Missing or unset API key
**Fix:** Add to `.envrc` and run `direnv allow`

### Error: YTS timeout/DNS failures
**Cause:** YTS mirror is blocked or slow
**Fix:** Set `YTS_API_BASE` to a working mirror or use comma-separated list

### Error: "line contains NUL"
**Cause:** Corrupted CSV file
**Fix:** Code already handles this defensively in `_iter_csv_rows()`

### Error: Jellyfin 400 on `/Users/...`
**Cause:** Invalid API key or permissions issue
**Fix:** Test with `curl -H "X-Emby-Token: $JELLYFIN_API_KEY" "$JELLYFIN_BASE_URL/System/Info/Public"`

## Code Style and Conventions

### General
- Use type hints where appropriate (already done in yts.py)
- Verbose logging should use ANSI colors (RED, YELLOW, GREEN)
- Always flush CSV writes for progress visibility
- Handle keyboard interrupts gracefully

### CSV Operations
- Use `csv.DictReader` and `csv.DictWriter` for all CSV operations
- Preserve header order when adding columns
- Flush after each row write for long-running operations
- Strip NUL bytes and handle encoding errors defensively

### Network Requests
- Use requests.Session with persistent headers
- Implement exponential backoff for retries
- Check Content-Type before parsing JSON
- Switch mirrors on DNS errors immediately

### Error Handling
- Log errors with context (title, year, etc.) when verbose=True
- Continue processing other items on single-item failures
- Preserve partial results (don't lose work on errors)

## Git Workflow

### Artifacts
- All CSVs in `data/` are git-tracked
- Commit CSVs after successful runs (they're source data)
- Use meaningful commit messages (see git log for style)

### Excluded Files
- `.venv/` (virtual environment)
- `__pycache__/` (Python bytecode)
- `.envrc.local` (personal overrides)

## Advanced: Extending Functionality

### Adding New Commands
1. Add parser in `build_parser()` in cli.py
2. Add command handler in `main()` function
3. Import necessary functions from jellyfin.py or yts.py
4. Update README.md with usage examples
5. Update AGENTS.md if new contracts/defaults are introduced

### Adding New Enrichment Sources
1. Create new module (e.g., `tmdb_enrich.py`)
2. Follow yts.py patterns: CSV in/out, verbose logging, error handling
3. Add command to cli.py
4. Document CSV schema changes in AGENTS.md

## Quick Reference

### Entry Points
- Module: `python -m cli <command>`
- Installed: `movie-library-cli <command>` or `ml <command>`

### Commands
- `jellyfin`: Query Jellyfin for low-res, high-RT movies
- `yts-jf`: Enrich Jellyfin CSV with YTS data (prefers IMDb IDs)
- `yts`: Legacy enrichment for custom CSVs

### Key Functions
- `jellyfin.list_lowres_highrt()`: Query Jellyfin and write CSV
- `yts.yts_lookup_from_jf_csv()`: Enrich from Jellyfin CSV
- `yts.yts_search()`: Search YTS by title+year
- `yts.yts_search_by_imdb()`: Search YTS by IMDb ID

## Performance Tips
- YTS phase is sequential by default (one request at a time)
- Use mirrors close to your location for lower latency
- Enable verbose mode only when debugging (adds overhead)
- Consider timeout adjustments for slow networks

## Security Notes
- API keys are read from environment (never hardcoded)
- Magnet links are generated but not automatically downloaded
- YTS queries use public API (no authentication)
- Jellyfin API key should be kept private (excluded via .gitignore)
