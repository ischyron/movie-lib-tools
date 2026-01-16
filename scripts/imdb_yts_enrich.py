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


def extract_year_from_title(title: str) -> Tuple[str, Optional[int]]:
    s = title.strip()
    m = re.search(r"\b(19|20)\d{2}\b", s)
    year = int(m.group(0)) if m else None
    if m:
        # cut off anything after the year
        s = s[:m.end()].strip()
    # strip common noise
    s = re.sub(r"\s+-\s+", " ", s)
    s = s.replace(".", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # remove qualifiers
    s = re.sub(r"\b(REMASTERED|UNRATED|DIRECTOR'S CUT|EXTENDED|LIMITED)\b", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s, year


def omdb_lookup(title: str, year: Optional[int], apikey: str, timeout: float = 10.0) -> Tuple[str, Optional[int]]:
    params = {"apikey": apikey, "type": "movie"}
    # exact title lookup with optional year
    params["t"] = title
    if year:
        params["y"] = str(year)
    r = requests.get("https://www.omdbapi.com/", params=params, timeout=timeout)
    data = r.json()
    if data.get("Response") == "True":
        t = data.get("Title") or title
        y = data.get("Year")
        yv = int(y[:4]) if y and y[:4].isdigit() else year
        return t, yv
    # fallback: search
    params.pop("t", None)
    params.pop("y", None)
    params["s"] = title
    r = requests.get("https://www.omdbapi.com/", params=params, timeout=timeout)
    data = r.json()
    if data.get("Response") == "True":
        candidates = data.get("Search", [])
        # prefer same year, else first
        if year:
            for c in candidates:
                y = c.get("Year")
                if y and y[:4].isdigit() and int(y[:4]) == year:
                    return c.get("Title") or title, int(y[:4])
        c0 = candidates[0]
        y = c0.get("Year")
        yv = int(y[:4]) if y and y[:4].isdigit() else year
        return c0.get("Title") or title, yv
    return title, year


def normalize_csv(in_path: Path, out_path: Path, omdb_key: Optional[str]) -> None:
    # read titles (expects at least a 'title' column, else use first column)
    with in_path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fields = r.fieldnames or []
    title_col = "title" if "title" in fields else fields[0] if fields else None
    if not title_col:
        raise RuntimeError("No title column in input CSV")

    norm_rows: List[Dict[str, str]] = []
    for row in rows:
        raw_title = (row.get(title_col) or "").strip()
        if not raw_title:
            continue
        t, y = extract_year_from_title(raw_title)
        if omdb_key:
            try:
                t, y = omdb_lookup(t, y, omdb_key)
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
    ap = argparse.ArgumentParser(description="Normalize titles via IMDb (OMDb) then YTS-enrich in-place.")
    ap.add_argument("--in", dest="in_csv", default="data/manual_recovery.csv", type=Path)
    ap.add_argument("--out", dest="out_csv", default="data/manual_recovery.csv", type=Path)
    ap.add_argument("--omdb-key", default=os.getenv("OMDB_API_KEY", ""))
    ap.add_argument("--sequential", action="store_true", help="Run YTS sequentially")
    args = ap.parse_args(argv)

    omdb_key = args.omdb_key or ""
    normalize_csv(args.in_csv, args.out_csv, omdb_key if omdb_key else None)

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

