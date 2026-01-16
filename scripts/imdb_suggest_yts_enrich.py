#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from yts import yts_lookup_from_csv


SUGGEST_BASE = "https://v2.sg.media-imdb.com/suggestion"


def extract_year_from_title(title: str) -> Tuple[str, Optional[int]]:
    s = title.strip()
    m = re.search(r"\b(19|20)\d{2}\b", s)
    year = int(m.group(0)) if m else None
    if m:
        s = s[:m.end()].strip()
    # normalize
    s = re.sub(r"\s+-\s+", " ", s)
    s = s.replace(".", " ")
    s = re.sub(r"\b(REMASTERED|UNRATED|DIRECTOR'S CUT|EXTENDED|LIMITED)\b", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s, year


def imdb_suggest(title: str, timeout: float = 8.0) -> List[Dict]:
    if not title:
        return []
    t = title.strip()
    first = t[0].lower()
    if not ("a" <= first <= "z"):
        first = "_"
    import urllib.parse as up
    url = f"{SUGGEST_BASE}/{first}/{up.quote(t)}.json"
    r = requests.get(url, timeout=timeout)
    if r.status_code != 200:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    return (data.get("d") or []) if isinstance(data, dict) else []


def pick_best_imdb(cands: List[Dict], want_title: str, want_year: Optional[int]) -> Tuple[str, Optional[int]]:
    # Filter to movies (feature films) when possible
    feats = [c for c in cands if (c.get("q") or "").lower() in ("feature", "movie")]
    pool = feats if feats else cands

    def year_of(c: Dict) -> Optional[int]:
        y = c.get("y")
        try:
            return int(y)
        except Exception:
            return None

    # Prefer same year
    if want_year is not None:
        same = [c for c in pool if year_of(c) == want_year]
        if same:
            pool = same

    # Score: prefer with rank (higher is better), then title similarity (rough), then year closeness
    def norm(s: str) -> str:
        s = s.lower()
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    want_norm = norm(want_title)

    def score(c: Dict) -> Tuple[float, float, float]:
        rank = float(c.get("rank") or 0.0)
        cand_title = c.get("l") or ""
        cand_norm = norm(cand_title)
        # simple token overlap
        wt = set(want_norm.split())
        ct = set(cand_norm.split())
        overlap = len(wt & ct) / max(1.0, len(wt))
        y = year_of(c)
        year_bonus = 0.0
        if want_year is not None and y is not None:
            year_bonus = -abs(want_year - y)
        return (rank, overlap, year_bonus)

    if not pool:
        return want_title, want_year
    best = max(pool, key=score)
    best_title = best.get("l") or want_title
    best_year = year_of(best) if year_of(best) is not None else want_year
    return best_title, best_year


def normalize_csv(in_path: Path, out_path: Path) -> None:
    with in_path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fields = r.fieldnames or []
    title_col = "title" if "title" in fields else (fields[0] if fields else None)
    if not title_col:
        raise RuntimeError("No title column in input CSV")

    norm_rows: List[Dict[str, str]] = []
    for row in rows:
        raw = (row.get(title_col) or "").strip()
        if not raw:
            continue
        t, y = extract_year_from_title(raw)
        try:
            cands = imdb_suggest(t)
            t2, y2 = pick_best_imdb(cands, t, y)
            t, y = t2, y2
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
    ap = argparse.ArgumentParser(description="Normalize titles via IMDb suggest, then YTS-enrich in-place.")
    ap.add_argument("--in", dest="in_csv", default="data/manual_recovery.csv", type=Path)
    ap.add_argument("--out", dest="out_csv", default="data/manual_recovery.csv", type=Path)
    ap.add_argument("--sequential", action="store_true")
    args = ap.parse_args(argv)

    normalize_csv(args.in_csv, args.out_csv)
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

