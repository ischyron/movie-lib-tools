from __future__ import annotations

import csv
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Set
import os
import difflib
import csv as _csv
from tempfile import NamedTemporaryFile

import requests
import os as _os
from urllib.parse import urljoin as _urljoin
try:
    import urllib3.exceptions as _u3e
except Exception:  # pragma: no cover
    class _u3e:  # type: ignore
        NameResolutionError = Exception


def _is_dns_error(exc: Exception) -> bool:
    cur: Optional[BaseException] = exc  # type: ignore
    visited = set()
    while cur and id(cur) not in visited:
        visited.add(id(cur))
        if isinstance(cur, _u3e.NameResolutionError):
            return True
        msg = str(cur)
        if "nodename nor servname provided" in msg or "Name or service not known" in msg:
            return True
        cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
    return False
TMDB_KEY_DEFAULT = os.getenv("TMDB_API_KEY", "")
OMDB_KEY_DEFAULT = os.getenv("OMDB_API_KEY", "")
import urllib.parse as _up

# Known mirrors; env YTS_API_BASE can override/augment (comma-separated)
_YTS_DEFAULT_BASES = [
    "https://www.yts-official.to/api/v2",
    "https://yts.rs/api/v2",
    "https://yts.lt/api/v2",
    "https://yts.mx/api/v2",
    "https://yts.pm/api/v2",
    "https://yts.ag/api/v2",
    "https://yts.am/api/v2",
]


def _yts_bases() -> List[str]:
    # Allow override via env; supports comma-separated list of sites or API roots
    env = (_os.getenv("YTS_API_BASE") or "").strip()
    bases: List[str] = []
    if env:
        for raw in env.split(','):
            b = raw.strip()
            if not b:
                continue
            # Accept site root or explicit /api/v2
            if b.endswith('/api/v2'):
                nb = b.rstrip('/')
            else:
                nb = b.rstrip('/') + '/api/v2'
            bases.append(nb)
    # Default mirrors (order chosen for reliability); includes latest known official
    bases.extend(_YTS_DEFAULT_BASES)
    # de-dup while preserving order
    seen = set()
    uniq = []
    for b in bases:
        if b not in seen:
            uniq.append(b)
            seen.add(b)
    return uniq


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json, */*;q=0.1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36 movie-library-cli/0.1",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s


def _json_from_response(r: requests.Response, verbose: bool) -> Optional[dict]:
    ctype = (r.headers.get("Content-Type") or "").lower()
    text_snippet = None
    try:
        if "application/json" in ctype or r.text.strip().startswith("{"):
            return r.json()
        else:
            text_snippet = r.text[:160].replace("\n", " ").strip()
            return None
    except Exception as e:
        # Not JSON or parse failed
        try:
            text_snippet = r.text[:160].replace("\n", " ").strip()
        except Exception:
            text_snippet = None
        if verbose:
            print(f"{YELLOW}[yts] non-JSON response; snippet='{text_snippet or ''}'{RESET}")
        return None
IMDB_SUGGEST_BASE = "https://v2.sg.media-imdb.com/suggestion"
TMDB_BASE = "https://api.themoviedb.org/3"

# Console colors
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"

# Preference config (can be tuned centrally)
RATING_UHD_THRESHOLD = 7.0
PREF_QUALITIES_HIGH = ["2160p", "1080p", "720p"]
PREF_QUALITIES_DEFAULT = ["1080p", "720p"]


@dataclass
class YTSMovie:
    id: int
    title: str
    year: int
    url: str
    torrents: List[Dict]
    rating: float
    imdb_code: str


def _render_table(rows: List[Dict[str, str]], columns: List[Tuple[str, str]]) -> str:
    if not rows:
        return ""
    widths: List[int] = []
    for header, key in columns:
        max_len = len(header)
        for row in rows:
            val = row.get(key, "")
            max_len = max(max_len, len(str(val)))
        widths.append(max_len)

    def fmt_row(row: Dict[str, str]) -> str:
        parts = []
        for (header, key), width in zip(columns, widths):
            parts.append(str(row.get(key, "")).ljust(width))
        return " | ".join(parts)

    header_line = " | ".join(h.ljust(w) for (h, _), w in zip(columns, widths))
    sep_line = "-+-".join("-" * w for w in widths)
    body = [fmt_row(r) for r in rows]
    return "\n".join([header_line, sep_line, *body])


def _sanitize_title(s: str) -> str:
    # Normalize separators, drop year in parentheses, strip punctuation, lower
    s = re.sub(r"[._]+", " ", s)
    s = re.sub(r"\((\d{4})\)", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _build_query(title: str, year: Optional[int]) -> str:
    q = title
    if year:
        q = f"{title} {year}"
    return q


def yts_search(title: str, year: Optional[int], timeout: float, retries: int, slow_after: float, verbose: bool) -> List[YTSMovie]:
    q = _build_query(_sanitize_title(title), year)
    params = {
        "query_term": q,
        "limit": 10,
        "sort_by": "year",
        "order_by": "desc",
    }
    movies: List[YTSMovie] = []
    bases = _yts_bases()
    sess = _build_session()
    for base in bases:
        url = f"{base}/list_movies.json"
        attempt = 0
        backoff = 0.75
        while True:
            attempt += 1
            t0 = time.monotonic()
            try:
                if verbose:
                    print(f"[yts] GET {url} q='{q}' attempt={attempt}")
                r = sess.get(url, params=params, timeout=timeout, allow_redirects=True)
                elapsed = time.monotonic() - t0
                if verbose:
                    print(f"[yts] status={r.status_code} elapsed={elapsed:.2f}s")
                r.raise_for_status()
                data = _json_from_response(r, verbose=verbose)
                if data is None:
                    # This base likely doesn't serve the API JSON (HTML/redirect or blocked). Switch mirror.
                    if verbose:
                        print(f"{RED}[yts] {base} did not return JSON; switching mirror{RESET}")
                    break
                if verbose:
                    movies_dbg = []
                    try:
                        for m in (data.get("data", {}) or {}).get("movies", []) or []:
                            movies_dbg.append({
                                "title": m.get("title"),
                                "year": m.get("year"),
                                "rating": m.get("rating"),
                                "torrents": [
                                    {"quality": t.get("quality"), "type": t.get("type"), "size": t.get("size")}
                                    for t in (m.get("torrents") or [])
                                ],
                            })
                    except Exception:
                        movies_dbg = ["<parse error>"]
                    print(f"{GREEN}[yts] response movies: {movies_dbg}{RESET}")
                movies = []
                for m in (data.get("data", {}) or {}).get("movies", []) or []:
                    movies.append(
                        YTSMovie(
                            id=m["id"],
                            title=m.get("title") or "",
                            year=m.get("year") or 0,
                            url=m.get("url") or "",
                            torrents=m.get("torrents") or [],
                            rating=float(m.get("rating") or 0.0),
                            imdb_code=(m.get("imdb_code") or "").strip(),
                        )
                    )
                if elapsed >= slow_after and attempt <= retries:
                    if verbose:
                        print(f"{YELLOW}[yts] slow ({elapsed:.2f}s >= {slow_after}s); retrying after {backoff:.2f}s{RESET}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return movies
            except Exception as e:
                elapsed = time.monotonic() - t0
                # On DNS failures, switch mirror immediately
                if _is_dns_error(e):
                    if verbose:
                        print(f"{RED}[yts] DNS error on {base}: {e}; switching mirror{RESET}")
                    break
                if attempt <= retries:
                    wait = backoff
                    if verbose:
                        print(f"{RED}[yts] error on {base}: {e} (elapsed {elapsed:.2f}s); retry {attempt}/{retries} after {wait:.2f}s{RESET}")
                    time.sleep(wait)
                    backoff *= 2
                    continue
                if verbose:
                    print(f"{RED}[yts] failed on {base} after {attempt-1} retries: {e}{RESET}")
                break
    if verbose:
        print(f"{RED}[yts] all mirrors failed{RESET}")
    return []

def yts_search_by_imdb(imdb_id: str, timeout: float, retries: int, slow_after: float, verbose: bool) -> List[YTSMovie]:
    if not imdb_id:
        return []
    params = {
        "query_term": imdb_id,
        "limit": 10,
        "sort_by": "year",
        "order_by": "desc",
    }
    movies: List[YTSMovie] = []
    bases = _yts_bases()
    sess = _build_session()
    for base in bases:
        url = f"{base}/list_movies.json"
        attempt = 0
        backoff = 0.75
        while True:
            attempt += 1
            t0 = time.monotonic()
            try:
                if verbose:
                    print(f"[yts] GET {url} imdb='{imdb_id}' attempt={attempt}")
                r = sess.get(url, params=params, timeout=timeout, allow_redirects=True)
                elapsed = time.monotonic() - t0
                if verbose:
                    print(f"[yts] status={r.status_code} elapsed={elapsed:.2f}s")
                r.raise_for_status()
                data = _json_from_response(r, verbose=verbose)
                if data is None:
                    if verbose:
                        print(f"{RED}[yts] {base} did not return JSON; switching mirror{RESET}")
                    break
                movies = []
                for m in (data.get("data", {}) or {}).get("movies", []) or []:
                    movies.append(
                        YTSMovie(
                            id=m["id"],
                            title=m.get("title") or "",
                            year=m.get("year") or 0,
                            url=m.get("url") or "",
                            torrents=m.get("torrents") or [],
                            rating=float(m.get("rating") or 0.0),
                            imdb_code=(m.get("imdb_code") or "").strip(),
                        )
                    )
                if elapsed >= slow_after and attempt <= retries:
                    if verbose:
                        print(f"{YELLOW}[yts] slow ({elapsed:.2f}s >= {slow_after}s); retrying after {backoff:.2f}s{RESET}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return movies
            except Exception as e:
                elapsed = time.monotonic() - t0
                if _is_dns_error(e):
                    if verbose:
                        print(f"{RED}[yts] DNS error on {base}: {e}; switching mirror{RESET}")
                    break
                if attempt <= retries:
                    wait = backoff
                    if verbose:
                        print(f"{RED}[yts] error on {base}: {e} (elapsed {elapsed:.2f}s); retry {attempt}/{retries} after {wait:.2f}s{RESET}")
                    time.sleep(wait)
                    backoff *= 2
                    continue
                if verbose:
                    print(f"{RED}[yts] failed on {base} after {attempt-1} retries: {e}{RESET}")
                break
    if verbose:
        print(f"{RED}[yts] all mirrors failed{RESET}")
    return []


def yts_movie_details(identifier: str, timeout: float, retries: int, slow_after: float, verbose: bool) -> Optional[Dict]:
    if not identifier:
        return None
    key = "imdb_id" if identifier.lower().startswith("tt") else "movie_id"
    params = {key: identifier}
    bases = _yts_bases()
    sess = _build_session()
    for base in bases:
        url = f"{base}/movie_details.json"
        attempt = 0
        backoff = 0.75
        while True:
            attempt += 1
            t0 = time.monotonic()
            try:
                if verbose:
                    print(f"[yts] GET {url} {key}='{identifier}' attempt={attempt}")
                r = sess.get(url, params=params, timeout=timeout, allow_redirects=True)
                elapsed = time.monotonic() - t0
                if verbose:
                    print(f"[yts] status={r.status_code} elapsed={elapsed:.2f}s")
                r.raise_for_status()
                data = _json_from_response(r, verbose=verbose)
                if data is None:
                    if verbose:
                        print(f"{RED}[yts] {base} did not return JSON; switching mirror{RESET}")
                    break
                movie = (data.get("data", {}) or {}).get("movie") or None
                if elapsed >= slow_after and attempt <= retries:
                    if verbose:
                        print(f"{YELLOW}[yts] slow ({elapsed:.2f}s >= {slow_after}s); retrying after {backoff:.2f}s{RESET}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return movie
            except Exception as e:
                elapsed = time.monotonic() - t0
                if _is_dns_error(e):
                    if verbose:
                        print(f"{RED}[yts] DNS error on {base}: {e}; switching mirror{RESET}")
                    break
                if attempt <= retries:
                    wait = backoff
                    if verbose:
                        print(f"{RED}[yts] error on {base}: {e} (elapsed {elapsed:.2f}s); retry {attempt}/{retries} after {wait:.2f}s{RESET}")
                    time.sleep(wait)
                    backoff *= 2
                    continue
                if verbose:
                    print(f"{RED}[yts] failed on {base} after {attempt-1} retries: {e}{RESET}")
                break
    if verbose:
        print(f"{RED}[yts] all mirrors failed{RESET}")
    return None


def _render_movies_table(movies: List[YTSMovie]) -> str:
    rows: List[Dict[str, str]] = []
    for m in movies:
        rows.append({
            "title": m.title,
            "year": str(m.year or ""),
            "rating": f"{m.rating:.1f}" if m.rating else "",
            "yts_id": str(m.id),
            "imdb": m.imdb_code,
            "url": m.url,
        })
    columns = [
        ("Title", "title"),
        ("Year", "year"),
        ("Rating", "rating"),
        ("YTS ID", "yts_id"),
        ("IMDb", "imdb"),
        ("URL", "url"),
    ]
    return _render_table(rows, columns)


def _render_movie_detail(movie: Dict) -> str:
    summary_rows = [{
        "title": movie.get("title", ""),
        "year": str(movie.get("year") or ""),
        "rating": f"{float(movie.get('rating') or 0):.1f}" if movie.get("rating") else "",
        "yts_id": str(movie.get("id") or ""),
        "imdb": movie.get("imdb_code") or "",
        "runtime": f"{movie.get('runtime')} min" if movie.get("runtime") else "",
        "url": movie.get("url") or "",
    }]
    summary_cols = [
        ("Title", "title"),
        ("Year", "year"),
        ("Rating", "rating"),
        ("Runtime", "runtime"),
        ("YTS ID", "yts_id"),
        ("IMDb", "imdb"),
        ("URL", "url"),
    ]
    parts = [_render_table(summary_rows, summary_cols)]
    torrents = movie.get("torrents") or []
    if torrents:
        torrent_rows: List[Dict[str, str]] = []
        for t in torrents:
            mag = ""
            try:
                mag = magnet_from_torrent(movie.get("title", ""), t)
            except Exception:
                mag = ""
            torrent_rows.append({
                "quality": t.get("quality") or "",
                "type": t.get("type") or "",
                "size": t.get("size") or "",
                "seeds": str(t.get("seeds") or ""),
                "peers": str(t.get("peers") or ""),
                "magnet": mag,
            })
        torrent_cols = [
            ("Quality", "quality"),
            ("Type", "type"),
            ("Size", "size"),
            ("Seeds", "seeds"),
            ("Peers", "peers"),
            ("Magnet", "magnet"),
        ]
        parts.append(_render_table(torrent_rows, torrent_cols))
    return "\n\n".join([p for p in parts if p])


def yts_cli_search(key: Optional[str], identifier: Optional[str], timeout: float, retries: int, slow_after: float, verbose: bool) -> None:
    if identifier:
        movie = yts_movie_details(identifier, timeout=timeout, retries=retries, slow_after=slow_after, verbose=verbose)
        if not movie:
            print(f"No result for id '{identifier}'")
            return
        print(_render_movie_detail(movie))
        return

    if not key:
        print("Provide either --key or --id")
        return
    movies = yts_search(key, None, timeout=timeout, retries=retries, slow_after=slow_after, verbose=verbose)
    if not movies:
        print(f"No matches for '{key}'")
        return
    print(_render_movies_table(movies))

def _rank_from_height(height: Optional[int]) -> float:
    if not height:
        return 0.0
    if height >= 2160:
        return 3.0
    if height >= 1440:
        return 2.5
    if height >= 1080:
        return 2.0
    if height >= 720:
        return 1.0
    return 0.0

def yts_lookup_from_jf_csv(
    input_csv: Path,
    output_csv: Optional[Path],
    in_place: bool,
    timeout: float,
    retries: int,
    slow_after: float,
    verbose: bool,
) -> None:
    rows = list(_iter_csv_rows(input_csv))
    header = list(rows[0].keys()) if rows else []
    add_cols = ["yts_title", "yts_year", "yts_url", "yts_quality_available", "yts_next_quality", "magnet"]
    for c in add_cols:
        if c not in header:
            header.append(c)

    # Choose output path
    if in_place:
        out_path = input_csv
    else:
        out_path = output_csv or input_csv.parent / "yts_lowq.csv"

    with open(out_path, "w", newline="") as f_out:
        w = csv.DictWriter(f_out, fieldnames=header)
        w.writeheader()
        for row in rows:
            title = row.get("name") or row.get("title") or ""
            year = row.get("year")
            imdb_id = (row.get("imdb_id") or "").strip()
            height = None
            try:
                h = row.get("max_height")
                if isinstance(h, str) and h.isdigit():
                    height = int(h)
                elif isinstance(h, int):
                    height = h
            except Exception:
                height = None
            cur_rank = _rank_from_height(height)
            if verbose:
                print(f"[yts] jf item: title='{title}' year='{year or ''}' imdb='{imdb_id}' cur_rank={cur_rank}")

            # Enrich only titles that are strictly below 720p
            if height is not None and height >= 720:
                out_row = {k: row.get(k, "") for k in header}
                for c in add_cols:
                    if c not in out_row:
                        out_row[c] = ""
                w.writerow(out_row)
                f_out.flush()
                try:
                    os.fsync(f_out.fileno())
                except Exception:
                    pass
                continue

            movies = yts_search_by_imdb(imdb_id, timeout=timeout, retries=retries, slow_after=slow_after, verbose=verbose) if imdb_id else yts_search(title, int(year) if year else None, timeout=timeout, retries=retries, slow_after=slow_after, verbose=verbose)
            match = None
            if imdb_id:
                for m in movies:
                    if (m.imdb_code or "").lower() == imdb_id.lower():
                        match = m
                        break
            if match is None:
                match = _best_match(movies, title, int(year) if year else None)

            enriched = {c: "" for c in add_cols}
            if match:
                all_q = []
                for t in match.torrents:
                    q = t.get("quality") or ""
                    typ = t.get("type") or ""
                    all_q.append(f"{q}.{typ}")
                want_q, next_tor = _choose_next_quality(match, cur_rank)
                mag = magnet_from_torrent(match.title, next_tor) if next_tor else ""
                enriched = {
                    "yts_title": match.title,
                    "yts_year": match.year or "",
                    "yts_url": match.url,
                    "yts_quality_available": ",".join(all_q),
                    "yts_next_quality": want_q,
                    "magnet": mag,
                }

            out_row = {k: row.get(k, "") for k in header}
            out_row.update(enriched)
            w.writerow(out_row)
            f_out.flush()
            try:
                os.fsync(f_out.fileno())
            except Exception:
                pass
    print(f"Wrote {out_path}")


def _title_similarity(a: str, b: str) -> float:
    a_n = _sanitize_title(a)
    b_n = _sanitize_title(b)
    return difflib.SequenceMatcher(None, a_n, b_n).ratio()


def _best_match(movies: List[YTSMovie], title: str, year: Optional[int]) -> Optional[YTSMovie]:
    if not movies:
        return None
    # If year provided, prefer exact-year matches; among them pick highest rating, then closest title
    if year:
        same_year = [m for m in movies if m.year == year]
        if same_year:
            return max(same_year, key=lambda m: (m.rating or 0.0, _title_similarity(title, m.title)))
    # Otherwise choose by a blend: highest rating first, then title similarity, then nearest year
    def score(m: YTSMovie) -> Tuple[float, float, float]:
        sim = _title_similarity(title, m.title)
        year_bonus = 0.0
        if year:
            yd = abs(m.year - year) if m.year and year else 9999
            year_bonus = -float(yd)
        return (m.rating or 0.0, sim, year_bonus)

    return max(movies, key=score)


def _imdb_suggest(title: str, timeout: float = 8.0) -> List[Dict]:
    if not title:
        return []
    t = title.strip()
    if not t:
        return []
    first = t[0].lower()
    if not ("a" <= first <= "z"):
        first = "_"
    url = f"{IMDB_SUGGEST_BASE}/{first}/{_up.quote(t)}.json"
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            return []
        data = r.json()
        arr = data.get("d") if isinstance(data, dict) else None
        return arr or []
    except Exception:
        return []


def _pick_best_imdb(cands: List[Dict], want_title: str, want_year: Optional[int]) -> Tuple[str, Optional[int], Optional[str]]:
    # Filter to feature films when possible
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
        wt = set(want_norm.split())
        ct = set(cand_norm.split())
        overlap = len(wt & ct) / max(1.0, len(wt))
        y = year_of(c)
        year_bonus = 0.0
        if want_year is not None and y is not None:
            year_bonus = -abs(want_year - y)
        return (rank, overlap, year_bonus)

    if not pool:
        return want_title, want_year, None
    best = max(pool, key=score)
    best_title = best.get("l") or want_title
    best_year = year_of(best) if year_of(best) is not None else want_year
    imdb_id = best.get("id") or best.get("i") or None  # IMDB suggest sometimes uses 'id'
    return best_title, best_year, imdb_id


def _omdb_lookup(title: str, year: Optional[int], apikey: str, timeout: float = 10.0) -> Tuple[str, Optional[int], Optional[str]]:
    params = {"apikey": apikey, "type": "movie"}
    params["t"] = title
    if year:
        params["y"] = str(year)
    try:
        r = requests.get("https://www.omdbapi.com/", params=params, timeout=timeout)
        data = r.json()
        if data.get("Response") == "True":
            t = data.get("Title") or title
            y = data.get("Year")
            yv = int(y[:4]) if y and y[:4].isdigit() else year
            imdb_id = data.get("imdbID") or None
            return t, yv, imdb_id
        # fallback: search
        params.pop("t", None)
        params.pop("y", None)
        params["s"] = title
        r = requests.get("https://www.omdbapi.com/", params=params, timeout=timeout)
        data = r.json()
        if data.get("Response") == "True":
            candidates = data.get("Search", []) or []
            if year:
                for c in candidates:
                    yy = c.get("Year")
                    if yy and yy[:4].isdigit() and int(yy[:4]) == year:
                        return (c.get("Title") or title, int(yy[:4]), c.get("imdbID") or None)
            if candidates:
                c0 = candidates[0]
                yy = c0.get("Year")
                yv = int(yy[:4]) if yy and yy[:4].isdigit() else year
                return (c0.get("Title") or title, yv, c0.get("imdbID") or None)
    except Exception:
        pass
    return title, year, None


def _tmdb_search(title: str, year: Optional[int], apikey: str, timeout: float = 8.0) -> Tuple[str, Optional[int], Optional[str]]:
    # Search TMDb, pick best result (prefer same year), then fetch IMDb ID from movie details
    import requests as _rq
    params = {"api_key": apikey, "query": title, "include_adult": "false", "language": "en-US", "page": "1"}
    if year is not None:
        params["year"] = str(year)
    try:
        r = _rq.get(f"{TMDB_BASE}/search/movie", params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json() or {}
        results = data.get("results") or []
        if not results:
            return title, year, None
        def rel_year(res) -> Optional[int]:
            rd = res.get("release_date") or ""
            return int(rd[:4]) if len(rd)>=4 and rd[:4].isdigit() else None
        pool = results
        if year is not None:
            same = [res for res in results if rel_year(res) == year]
            if same:
                pool = same
        best = max(pool, key=lambda res: float(res.get("popularity") or 0.0))
        tmdb_id = best.get("id")
        best_title = (best.get("title") or best.get("original_title") or title).strip()
        best_year = rel_year(best) or year
        imdb_id = None
        if tmdb_id:
            try:
                r2 = _rq.get(f"{TMDB_BASE}/movie/{tmdb_id}", params={"api_key": apikey, "language": "en-US"}, timeout=timeout)
                if r2.status_code == 200:
                    dd = r2.json() or {}
                    imdb_id = (dd.get("imdb_id") or "").strip() or None
            except Exception:
                pass
        return best_title, best_year, imdb_id
    except Exception:
        return title, year, None


QUALITY_RANK = {"720p": 1, "1080p": 2, "1440p": 2.5, "2160p": 3, "4k": 3, "uhd": 3}


def _detect_current_quality(name: str) -> float:
    s = name.lower()
    for token, rank in ("2160p", 3), ("4k", 3), ("uhd", 3), ("1440p", 2.5), ("1080p", 2), ("1024p", 1.5), ("720p", 1):
        if token in s:
            return rank
    return 0.0


def _choose_next_quality(match: YTSMovie, cur_rank: float) -> Tuple[str, Optional[Dict]]:
    # Build qualities map -> preferred torrent (prefer bluray type)
    by_quality: Dict[str, List[Dict]] = {}
    for t in match.torrents:
        q = (t.get("quality") or "").lower()
        if not q:
            continue
        by_quality.setdefault(q, []).append(t)
    for q, arr in by_quality.items():
        arr.sort(key=lambda t: 0 if (t.get("type") or "").lower()=="bluray" else 1)

    pref = PREF_QUALITIES_HIGH if match.rating >= RATING_UHD_THRESHOLD else PREF_QUALITIES_DEFAULT
    for want in pref:
        qk = want.lower()
        rank = QUALITY_RANK.get(qk, 0)
        if rank > cur_rank and qk in by_quality:
            return want, by_quality[qk][0]
    # Fallback: highest available above current
    candidates = []
    for qk, arr in by_quality.items():
        rank = QUALITY_RANK.get(qk, 0)
        if rank > cur_rank:
            candidates.append((rank, qk, arr[0]))
    if candidates:
        candidates.sort(reverse=True)
        _, qk, tor = candidates[0]
        return qk, tor
    return "", None


def magnet_from_torrent(title: str, torrent: Dict) -> str:
    from urllib.parse import quote
    name = f"{title}.{torrent.get('quality','')}.{torrent.get('type','')}"
    xt = f"urn:btih:{torrent.get('hash','')}"
    return f"magnet:?xt={xt}&dn={quote(name)}"


def _iter_csv_rows(path: Path) -> Iterable[Dict[str, str]]:
    # Defensive reader: strip NUL bytes and decode with replacement to avoid
    # '_csv.Error: line contains NUL' caused by corrupted CSVs.
    raw = path.read_bytes()
    if b"\x00" in raw:
        raw = raw.replace(b"\x00", b"")
    text = raw.decode("utf-8", errors="replace")
    f = io.StringIO(text)
    r = csv.DictReader(f)
    for row in r:
        yield row


def yts_lookup_from_csv(
    input_csv: Path,
    output_csv: Path,
    is_lost: bool,
    in_place: bool,
    refresh: bool,
    concurrency: int,
    timeout: float,
    retries: int,
    slow_after: float,
    verbose: bool,
    pre_match: str = "tmdb",
    omdb_key: Optional[str] = None,
    tmdb_key: Optional[str] = None,
) -> None:
    # Fallback keys from environment/defaults
    if not tmdb_key:
        tmdb_key = TMDB_KEY_DEFAULT
    if not omdb_key:
        omdb_key = OMDB_KEY_DEFAULT or None
    rows = list(_iter_csv_rows(input_csv))

    # Determine current quality rank if available
    cur_ranks: List[float] = []
    for row in rows:
        src = row.get("path") or row.get("folder_path") or ""
        cur_ranks.append(_detect_current_quality(src))

    def task(row: Dict[str, str]) -> Tuple[Dict[str, str], Optional[YTSMovie]]:
        # Base title/year from CSV or folder path
        base_title = (row.get("title") or row.get("title_guess") or row.get("folder_path") or "").split("/")[-1].strip()
        base_year = row.get("year")
        y = int(base_year) if base_year else None

        # Optional pre-match using OMDb or IMDb Suggest to refine title/year and obtain IMDb ID
        best_title, best_year, imdb_id = base_title, y, None
        mode = (pre_match or "none").lower()
        if mode in ("tmdb", "auto") and (tmdb_key or (mode == "tmdb")):
            try:
                t, yy, iid = _tmdb_search(base_title, y, apikey=(tmdb_key or ""))
                best_title, best_year, imdb_id = t, yy, iid or imdb_id
            except Exception:
                pass
        if mode in ("omdb", "auto") and (omdb_key or (mode == "omdb")) and not imdb_id:
            t, yy, iid = _omdb_lookup(base_title, y, apikey=(omdb_key or ""))
            best_title, best_year, imdb_id = t, yy, iid or imdb_id
        if mode in ("imdb-suggest", "auto") and not imdb_id:
            cands = _imdb_suggest(base_title)
            t, yy, iid = _pick_best_imdb(cands, base_title, y)
            if t and (iid or t.lower() != base_title.lower() or (yy and yy != y)):
                best_title, best_year, imdb_id = t, yy, iid or imdb_id

        movies = yts_search(best_title, best_year, timeout=timeout, retries=retries, slow_after=slow_after, verbose=verbose)
        # If we have an IMDb ID, prefer exact imdb_code match
        if imdb_id:
            for m in movies:
                if (m.imdb_code or "").lower() == str(imdb_id).lower():
                    return row, m
        return row, _best_match(movies, best_title, best_year)

    # Prepare direct in-file rewrite; always add enrichment columns
    add_cols = ["yts_title", "yts_year", "yts_url", "yts_quality_available", "yts_next_quality", "magnet"]
    orig_header = (rows and list(rows[0].keys())) or []
    # ensure header order and presence
    header = orig_header[:]
    for c in add_cols:
        if c not in header:
            header.append(c)

    def process_one(row: Dict[str, str]) -> None:
        title = (row.get("title") or row.get("title_guess") or row.get("folder_path") or "").split("/")[-1].strip()
        year = row.get("year") or ""
        src = row.get("path") or row.get("folder_path") or ""
        cur_rank = _detect_current_quality(src)
        if verbose:
            print(f"[yts] item: src='{src}' title='{title}' year='{year or ''}' cur_rank={cur_rank}")
        # No-op here; row-skipping is handled in the write loop based on yts_next_quality and --refresh
        try:
            _, match = task(row)
        except Exception as e:
            if verbose:
                print(f"{RED}[yts] ERROR item failed: src='{src}' err={e}{RESET}")
            return [src, "", "", "", "", "", ""]

        if match is None:
            if verbose:
                print(f"{RED}[yts] no match: title='{title}' year='{year or ''}'{RESET}")
            return [src, "", "", "", "", "", ""]

        kept_q: List[str] = []
        kept_mag: List[str] = []
        all_q: List[str] = []
        for t in match.torrents:
            q = t.get("quality") or ""
            typ = t.get("type") or ""
            all_q.append(f"{q}.{typ}")
            rank = QUALITY_RANK.get(q.lower(), 0)
            if rank > cur_rank:
                kept_q.append(f"{q}.{typ}")
                kept_mag.append(magnet_from_torrent(match.title, t))
        next_q, next_t = _choose_next_quality(match, cur_rank)
        next_mag = magnet_from_torrent(match.title, next_t) if next_t else ""
        if verbose:
            color = GREEN if kept_q else YELLOW
            print(f"{color}[yts] match: '{match.title}' ({match.year}) rating={match.rating} url={match.url}{RESET}")
            print(f"{color}[yts] torrents: total={len(all_q)} kept_higher={len(kept_q)} next={next_q or '-'}{RESET}")
            if len(all_q) > 0:
                print(f"[yts] all_qualities: {sorted(set(all_q))}")
            if len(kept_q) > 0:
                print(f"[yts] kept_qualities: {sorted(set(kept_q))}")

        combined = [
            src,
            match.title,
            str(match.year),
            match.url,
            "|".join(sorted(set(all_q))),  # yts_quality_available (all qualities)
            next_q,
            next_mag,
        ]
        return combined

    # Direct in-file rewrite with per-row flush; sequential for safety
    with input_csv.open("w", newline="") as f_out:
        w = _csv.DictWriter(f_out, fieldnames=header)
        w.writeheader()
        f_out.flush();
        try:
            os.fsync(f_out.fileno())
        except Exception:
            pass

        for row in rows:
            src = row.get("path") or row.get("folder_path") or ""
            # Decide whether to skip based on existing enrichment unless --refresh
            if not refresh and (row.get("yts_next_quality") or row.get("magnet") or row.get("yts_title")):
                # Normalize: ensure all columns exist even when skipping
                enriched = {
                    "yts_title": row.get("yts_title", ""),
                    "yts_year": row.get("yts_year", ""),
                    "yts_url": row.get("yts_url", ""),
                    "yts_quality_available": row.get("yts_quality_available", ""),
                    "yts_next_quality": row.get("yts_next_quality", ""),
                    "magnet": row.get("magnet", ""),
                }
            else:
                try:
                    combined = process_one(row)
                    # Map combined list back to enrichment dict (drop src)
                    enriched = {
                        "yts_title": combined[1],
                        "yts_year": combined[2],
                        "yts_url": combined[3],
                        "yts_quality_available": combined[4],
                        "yts_next_quality": combined[5],
                        "magnet": combined[6],
                    }
                except KeyboardInterrupt:
                    # Write the current row unmodified to avoid data loss and re-raise
                    enriched = {
                        "yts_title": row.get("yts_title", ""),
                        "yts_year": row.get("yts_year", ""),
                        "yts_url": row.get("yts_url", ""),
                        "yts_quality_available": row.get("yts_quality_available", ""),
                        "yts_next_quality": row.get("yts_next_quality", ""),
                        "magnet": row.get("magnet", ""),
                    }
                    raise
                except Exception as e:
                    if verbose:
                        print(f"{RED}[yts] row error: src='{src}' err={e}{RESET}")
                    enriched = {c: row.get(c, "") for c in add_cols}

            # Compose row for write
            out_row = {k: row.get(k, "") for k in header}
            out_row.update(enriched)
            w.writerow(out_row)
            f_out.flush();
            try:
                os.fsync(f_out.fileno())
            except Exception:
                pass

    print(f"Updated {input_csv}")
