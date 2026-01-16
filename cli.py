import argparse
import sys
import subprocess
from pathlib import Path

from yts import yts_lookup_from_csv, yts_lookup_from_jf_csv, yts_cli_search
from yts_ui import run_yts_ui
from jellyfin import list_lowres_highrt


def find_repo_root(start: Path) -> Path:
    cur = start
    for _ in range(10):
        if (cur / ".git").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return start


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Jellyfin-driven low-res finder and YTS enrichment (no downloading).",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    # removed legacy scan command (replaced by Jellyfin-driven workflow)

    # yts command
    yp = sub.add_parser("yts", help="Query YTS for items listed in a CSV (legacy low_quality/lost CSVs)")
    yp.add_argument("--from-csv", required=True, type=Path, help="Input CSV from scan phase (will be updated in place)")
    yp.add_argument("--lost", action="store_true", help="Treat input as lost_movies.csv format")
    yp.add_argument("--concurrency", type=int, default=6, help="Parallel requests to YTS")
    yp.add_argument("--sequential", action="store_true", help="Process one movie at a time (sets concurrency=1)")
    yp.add_argument("--refresh", action="store_true", help="Re-run YTS for rows that already have results")
    yp.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout seconds")
    yp.add_argument("--retries", type=int, default=3, help="Retries per YTS query on failure/slow")
    yp.add_argument("--slow-after", type=float, default=9.0, help="Warn/retry if a request exceeds this many seconds")
    yp.add_argument("--verbose", action="store_true", help="Verbose logging for YTS lookups")
    # Pre-match options to improve title/year (and IMDb ID) before YTS
    yp.add_argument(
        "--omdb-key",
        default=None,
        help="OMDb API key (falls back to OMDB_API_KEY env var if omitted)",
    )
    yp.add_argument(
        "--tmdb-key",
        default=None,
        help="TMDb API key (falls back to TMDB_API_KEY env var if omitted)",
    )

    # jellyfin command
    jp = sub.add_parser("jellyfin", help="Query Jellyfin for low-res, high-RT movies (uses env JELLYFIN_BASE_URL/JELLYFIN_API_KEY)")
    jp.add_argument("--max-height", type=int, default=719, help="Maximum video height to include (e.g., 719 for <720p)")
    jp.add_argument("--min-rt", type=float, default=6.0, help="Minimum Rotten Tomatoes critic rating (6→60%; 75→75%)")
    jp.add_argument("--limit", type=int, default=200, help="Pagination size for API calls")
    jp.add_argument("--out-csv", type=Path, default=None, help="Optional CSV output (defaults to data/jf_lowres_rt.csv)")
    jp.add_argument("--verbose", action="store_true", help="Verbose logging of requests and timings")

    # yts for jellyfin CSV
    yj = sub.add_parser("yts-jf", help="Query YTS using Jellyfin CSV (prefers IMDb IDs from Jellyfin)")
    yj.add_argument("--from-csv", type=Path, default=None, help="Input CSV (defaults to data/jf_lowres_rt.csv)")
    yj.add_argument("--out-csv", type=Path, default=None, help="Output CSV (defaults to data/yts_lowq.csv)")
    yj.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout seconds")
    yj.add_argument("--retries", type=int, default=3, help="Retries per YTS query on failure/slow")
    yj.add_argument("--slow-after", type=float, default=9.0, help="Warn/retry if a request exceeds this many seconds")
    yj.add_argument("--verbose", action="store_true", help="Verbose logging for YTS lookups")

    ys = sub.add_parser("yts-search", help="Search YTS by title fragment or movie/IMDb ID")
    grp = ys.add_mutually_exclusive_group(required=True)
    grp.add_argument("--id", help="YTS movie id or IMDb tt id")
    grp.add_argument("--key", help="Title fragment to search")
    ys.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout seconds")
    ys.add_argument("--retries", type=int, default=3, help="Retries per YTS query on failure/slow")
    ys.add_argument("--slow-after", type=float, default=9.0, help="Warn/retry if a request exceeds this many seconds")
    ys.add_argument("--verbose", action="store_true", help="Verbose logging for YTS lookups")

    yui = sub.add_parser("yts-ui", help="Interactive TUI browser for YTS search results")
    yui.add_argument("--key", required=True, help="Title fragment to search and browse")
    yui.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout seconds")
    yui.add_argument("--retries", type=int, default=3, help="Retries per YTS query on failure/slow")
    yui.add_argument("--slow-after", type=float, default=9.0, help="Warn/retry if a request exceeds this many seconds")
    yui.add_argument("--verbose", action="store_true", help="Verbose logging for YTS lookups")

    add = sub.add_parser("add", help="Add magnets from a CSV (magnet column) to Transmission GUI")
    add.add_argument("csv", type=Path, help="CSV file containing a 'magnet' column")

    return p


def main(argv=None) -> int:
    argv = argv or sys.argv[1:]
    args = build_parser().parse_args(argv)

    repo = find_repo_root(Path.cwd())

    # Global default verbosity from env MLM_VERBOSE (1/true to enable)
    import os as _os
    verbose_default = str(_os.getenv("MLM_VERBOSE", "")).lower() in ("1", "true", "yes", "on")

    if args.cmd == "yts":
        omdb_key = args.omdb_key or _os.getenv("OMDB_API_KEY")
        tmdb_key = args.tmdb_key or _os.getenv("TMDB_API_KEY")
        yts_lookup_from_csv(
            input_csv=args.from_csv,
            output_csv=None,
            is_lost=args.lost,
            in_place=True,
            refresh=args.refresh,
            concurrency=(1 if args.sequential else args.concurrency),
            timeout=args.timeout,
            retries=args.retries,
            slow_after=args.slow_after,
            verbose=(args.verbose or verbose_default),
            pre_match="tmdb",
            omdb_key=omdb_key,
            tmdb_key=tmdb_key,
        )
        return 0

    if args.cmd == "jellyfin":
        api_key = _os.getenv("JELLYFIN_API_KEY")
        if not api_key:
            print("Error: set JELLYFIN_API_KEY (e.g., via .envrc/direnv or export).", file=sys.stderr)
            return 2
        base_url = _os.getenv("JELLYFIN_BASE_URL", "http://localhost:8096")
        out_csv = args.out_csv
        if out_csv is None:
            repo = find_repo_root(Path.cwd())
            out_csv = repo / "data" / "jf_lowres_rt.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        list_lowres_highrt(
            base_url=base_url,
            api_key=api_key,
            max_height=args.max_height,
            min_rt=args.min_rt,
            page_limit=args.limit,
            out_csv=out_csv,
            verbose=(args.verbose or verbose_default),
        )
        print(f"Wrote {out_csv}")
        return 0

    if args.cmd == "yts-jf":
        repo = find_repo_root(Path.cwd())
        in_csv = args.from_csv or (repo / "data" / "jf_lowres_rt.csv")
        out_csv = args.out_csv or (repo / "data" / "yts_lowq.csv")
        yts_lookup_from_jf_csv(
            input_csv=in_csv,
            output_csv=out_csv,
            in_place=False,
            timeout=args.timeout,
            retries=args.retries,
            slow_after=args.slow_after,
            verbose=(args.verbose or verbose_default),
        )
        return 0

    if args.cmd == "yts-search":
        yts_cli_search(
            key=args.key,
            identifier=args.id,
            timeout=args.timeout,
            retries=args.retries,
            slow_after=args.slow_after,
            verbose=(args.verbose or verbose_default),
        )
        return 0

    if args.cmd == "yts-ui":
        run_yts_ui(
            key=args.key,
            timeout=args.timeout,
            retries=args.retries,
            slow_after=args.slow_after,
            verbose=(args.verbose or verbose_default),
        )
        return 0

    if args.cmd == "add":
        repo = find_repo_root(Path.cwd())
        script = repo / "scripts" / "transmission_add.sh"
        if not script.exists():
            print(f"Error: missing helper script at {script}", file=sys.stderr)
            return 2
        cmd = [str(script), "-c", str(args.csv)]
        result = subprocess.run(cmd)
        return result.returncode

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
