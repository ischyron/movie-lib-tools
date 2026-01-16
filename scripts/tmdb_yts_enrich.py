#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from yts import yts_lookup_from_csv


TMDB_BASE = "https://api.themoviedb.org/3"


def extract_year_from_title(title: str) -> Tuple[str, Optional[int]]:
    s = title.strip()
    m = re.search(r"\b(19|20)\d{2}\b", s)
    year = int(m.group(0)) if m else None
    cut_at = m.end() if m else None
    if cut_at is not None:
        s = s[:cut_at].strip()
    # normalize separators and remove common qualifiers
    s = re.sub(r"\s+-\s+", " ", s)
    s = s.replace(".", " ")
    s = re.sub(r"\b(REMASTERED|UNRATED|DIRECTOR'S CUT|EXTENDED|LIMITED)\b", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s, year


def tmdb_search_movie(api_key: str, title: str, year: Optional[int], timeout: float = 10.0) -> Tuple[str, Optional[int]]:
    params: Dict[str, str] = {
        "api_key": api_key,
        "query": title,
        "include_adult": "false",
        "language": "en-US",
        "page": "1",
    }
    if year is not None:
        params["year"] = str(year)
    r = requests.get(f"{TMDB_BASE}/search/movie", params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", []) or []
    if not results:
        return title, year
    # Prefer exact year match, then highest popularity
    def year_of(res) -> Optional[int]:
        rd = res.get("release_date") or ""
        return int(rd[:4]) if len(rd) >= 4 and rd[:4].isdigit() else None

    same_year = []
    if year is not None:
        for res in results:
            y = year_of(res)
            if y == year:
                same_year.append(res)
    pool = same_year if same_year else results
    best = max(pool, key=lambda res: float(res.get("popularity") or 0.0))
    t = (best.get("title") or best.get("original_title") or title).strip()
    y = year_of(best)
    return t, y if y is not None else year


def normalize_csv(in_path: Path, out_path: Path, tmdb_key: Optional[str]) -> None:
    # read titles (expects at least a 'title' column, else use first column)
    with in_path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fields = r.fieldnames or []
    title_col = "title" if "title" in fields else (fields[0] if fields else None)
    if not title_col:
        raise RuntimeError("No title column in input CSV")

    norm_rows: List[Dict[str, str]] = []
    for row in rows:
        raw_title = (row.get(title_col) or "").strip()
        if not raw_title:
            continue
        t, y = extract_year_from_title(raw_title)
        if tmdb_key:
            try:
                t, y = tmdb_search_movie(tmdb_key, t, y)
            except Exception:
                pass
        norm_rows.append({"title": t, "year": str(y or "")})

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["title", "year"])
        w.writeheader()
        for r in norm_rows:
            w.writerow(r)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Normalize titles via TMDb then YTS-enrich in-place.")
    ap.add_argument("--in", dest="in_csv", default="data/manual_recovery.csv", type=Path)
    ap.add_argument("--out", dest="out_csv", default="data/manual_recovery.csv", type=Path)
    ap.add_argument("--tmdb-key", default=os.getenv("TMDB_API_KEY", ""))
    ap.add_argument("--sequential", action="store_true", help="Run YTS sequentially")
    args = ap.parse_args(argv)

    tmdb_key = args.tmdb_key or ""
    if not tmdb_key:
        print("WARNING: TMDB_API_KEY not set; normalizing without TMDb lookups")
    normalize_csv(args.in_csv, args.out_csv, tmdb_key if tmdb_key else None)

    # Call YTS enrichment in-place
    yts_lookup_from_csv(
        input_csv=args.out_csv,
        output_csv=None,
        is_lost=False,
        in_place=True,
        refresh=True,
        concurrency=(1 if args.sequential else 4),
        timeout=12.0,
        retries=2,
        slow_after=9.0,
        verbose=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

