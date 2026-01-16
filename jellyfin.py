from __future__ import annotations

import csv
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import os
import socket
import uuid
from importlib import metadata
import requests


@dataclass
class JFMovie:
    id: str
    name: str
    year: Optional[int]
    critic_rating: Optional[float]  # Jellyfin stores RT as 0-100
    critic_summary: Optional[str]
    max_height: Optional[int]
    imdb_id: Optional[str]
    tmdb_id: Optional[str]


def _headers(api_key: str) -> Dict[str, str]:
    """Build Emby-style authorization headers Jellyfin expects.

    Format: X-Emby-Authorization: MediaBrowser Client="...", Device="...", DeviceId="...", Version="..."
    Also include X-Emby-Token for the API key.
    """
    client = "movie-library-cli"
    device = socket.gethostname() or "cli"
    try:
        version = metadata.version("movie-library-cli")
    except Exception:
        version = "0.1.0"
    # Stable device id derived from host and user
    user = os.getenv("USER") or os.getenv("USERNAME") or "user"
    dev_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{device}-{user}"))
    auth = f'MediaBrowser Client="{client}", Device="{device}", DeviceId="{dev_id}", Version="{version}"'
    return {
        "X-Emby-Authorization": auth,
        "X-Emby-Token": api_key,
    }


def _scale_min_rt(min_rt: float) -> float:
    # Interpret values <= 10 as a 10-point scale and convert to percent
    # e.g., 6.0 -> 60.0; values > 10 are treated as already-percent
    return min_rt * 10.0 if min_rt <= 10.0 else min_rt


def _get_user_id(base_url: str, api_key: str, timeout: float = 10.0) -> str:
    url_base = base_url.rstrip("/")
    session = requests.Session()
    headers = _headers(api_key)
    # Primary attempt: /Users/Me
    r = session.get(f"{url_base}/Users/Me", headers=headers, params={"api_key": api_key}, timeout=timeout)
    if r.status_code == 200:
        data = r.json()
        return data.get("Id") or data.get("id")
    # Fallback: list users and pick the first enabled user
    rf = session.get(f"{url_base}/Users", headers=headers, params={"api_key": api_key}, timeout=timeout)
    rf.raise_for_status()
    users = rf.json() or []
    for u in users:
        if u.get("Id"):
            return u["Id"]
    raise RuntimeError(f"Unable to determine Jellyfin user id (status /Users/Me={r.status_code}; users len={len(users)})")


def _iter_movies(
    base_url: str,
    api_key: str,
    user_id: str,
    fields: Iterable[str],
    page_limit: int = 200,
    verbose: bool = False,
    timeout: float = 15.0,
) -> Iterable[Dict[str, Any]]:
    start_index = 0
    total = None
    params_base = {
        "IncludeItemTypes": "Movie",
        "Recursive": "true",
        "Fields": ",".join(fields),
        "SortBy": "SortName",
        "SortOrder": "Ascending",
    }
    session = requests.Session()
    headers = _headers(api_key)
    while True:
        params = dict(params_base)
        params.update({"StartIndex": start_index, "Limit": page_limit})
        t0 = time.time()
        resp = session.get(
            f"{base_url.rstrip('/')}/Users/{user_id}/Items",
            headers=headers,
            params={**params, "api_key": api_key},
            timeout=timeout,
        )
        dt = time.time() - t0
        resp.raise_for_status()
        payload = resp.json() or {}
        items = payload.get("Items", []) or []
        count = len(items)
        if verbose:
            print(f"JF: fetched {count} items at {start_index} in {dt:.2f}s")
        if total is None:
            total = payload.get("TotalRecordCount", count)
        if count == 0:
            break
        for it in items:
            yield it
        start_index += count
        # stop if we've paged past total or if fewer than page_limit returned
        if (total is not None and start_index >= total) or count < page_limit:
            break


def _extract_movie(item: Dict[str, Any]) -> JFMovie:
    name = item.get("Name") or item.get("name") or ""
    year = item.get("ProductionYear") or item.get("ProductionYear") or item.get("Year")
    # CriticRating is a 0-100 number when available (Rotten Tomatoes)
    critic = item.get("CriticRating")
    critic_summary = item.get("CriticRatingSummary")

    # MediaStreams can be present on list results if Fields included
    max_h = None
    streams = item.get("MediaStreams") or []
    for s in streams:
        if (s.get("Type") or s.get("type")) == "Video":
            h = s.get("Height") or s.get("height")
            if isinstance(h, int):
                max_h = h if max_h is None else max(max_h, h)

    prov = item.get("ProviderIds") or {}
    imdb_id = (prov.get("Imdb") or prov.get("IMDB") or prov.get("imdb") or "").strip() or None
    tmdb_id = None
    try:
        tid = prov.get("Tmdb") or prov.get("TMDB") or prov.get("tmdb")
        if tid is not None:
            tmdb_id = str(tid)
    except Exception:
        tmdb_id = None

    return JFMovie(
        id=item.get("Id") or item.get("id") or "",
        name=name,
        year=int(year) if isinstance(year, int) else (int(year) if isinstance(year, str) and year.isdigit() else None),
        critic_rating=float(critic) if critic is not None else None,
        critic_summary=critic_summary,
        max_height=max_h,
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
    )


def list_lowres_highrt(
    base_url: str,
    api_key: str,
        max_height: int = 719,
        min_rt: float = 6.0,
        page_limit: int = 200,
        out_csv: Optional[str] = None,
        verbose: bool = False,
) -> List[JFMovie]:
    """
    Query Jellyfin for movies where the max video stream height is < (max_height+1)
    and the CriticRating (Rotten Tomatoes, 0-100) exceeds the threshold.

    Note: if min_rt <= 10, it's treated as a 10-point scale and converted to percent.
    """
    threshold = _scale_min_rt(min_rt)
    try:
        user_id = _get_user_id(base_url, api_key)
    except Exception as e:
        raise RuntimeError(f"Jellyfin auth/user lookup failed: {e}")
    fields = [
        "MediaStreams",
        "CriticRating",
        "CriticRatingSummary",
        "ProviderIds",
        # include some common fields for completeness
        "ProductionYear",
        "Path",
    ]

    results: List[JFMovie] = []
    slow_warn = 9.0
    session = requests.Session()
    headers = _headers(api_key)
    url_base = base_url.rstrip("/")
    for raw in _iter_movies(base_url, api_key, user_id, fields=fields, page_limit=page_limit, verbose=verbose):
        movie = _extract_movie(raw)
        # Filter: valid critic rating and max height
        if movie.critic_rating is None:
            continue
        if movie.critic_rating < threshold:
            continue
        # If no height info present, skip (we only want confirmed < 720p)
        if movie.max_height is None and movie.id:
            # Fallback: fetch item details including MediaStreams
            try:
                r = session.get(
                    f"{url_base}/Items/{movie.id}",
                    headers=headers,
                    params={"Fields": "MediaStreams,ProviderIds", "api_key": api_key},
                    timeout=12.0,
                )
                if r.status_code == 200:
                    det = r.json() or {}
                    # ProviderIds fallback
                    prov = det.get("ProviderIds") or {}
                    if not movie.imdb_id:
                        iid = (prov.get("Imdb") or prov.get("IMDB") or prov.get("imdb") or "").strip() or None
                        movie.imdb_id = iid or movie.imdb_id
                    if not movie.tmdb_id:
                        tid = prov.get("Tmdb") or prov.get("TMDB") or prov.get("tmdb")
                        movie.tmdb_id = str(tid) if tid is not None else movie.tmdb_id
                    streams = det.get("MediaStreams") or []
                    for s in streams:
                        if (s.get("Type") or s.get("type")) == "Video":
                            h = s.get("Height") or s.get("height")
                            if isinstance(h, int):
                                movie.max_height = h if movie.max_height is None else max(movie.max_height, h)
                elif verbose:
                    print(f"JF: detail fetch status={r.status_code} for item {movie.id}")
            except Exception as e:
                if verbose:
                    print(f"JF: detail fetch error for item {movie.id}: {e}")
        if movie.max_height is None:
            continue
        if movie.max_height <= max_height:
            results.append(movie)

    # Write CSV if requested
    if out_csv:
        with open(out_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["name", "year", "critic_rating", "critic_summary", "max_height", "jellyfin_id", "imdb_id", "tmdb_id"])
            for m in results:
                w.writerow([
                    m.name,
                    m.year if m.year is not None else "",
                    f"{m.critic_rating:.1f}",
                    m.critic_summary or "",
                    m.max_height if m.max_height is not None else "",
                    m.id,
                    m.imdb_id or "",
                    m.tmdb_id or "",
                ])

    return results
