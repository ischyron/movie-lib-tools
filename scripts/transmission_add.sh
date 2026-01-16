#!/usr/bin/env bash
set -Eeuo pipefail

# transmission_add.sh — add magnet links via Transmission GUI by default (or daemon if --daemon)
# Usage examples:
#   scripts/transmission_add.sh "magnet:?xt=urn:btih:..."
#   scripts/transmission_add.sh --clear -c data/low_quality_movies.csv
#   scripts/transmission_add.sh --daemon -w "$HOME/Downloads/Movies" -f data/selected_magnets.txt
#   scripts/transmission_add.sh --daemon -n user:pass "magnet:?xt=urn:btih:..."

RPC_HOST=${RPC_HOST:-localhost}
RPC_PORT=${RPC_PORT:-9091}
RPC_AUTH=""
DOWNLOAD_DIR="${HOME}/Downloads"
CONF_DIR="${CONF_DIR:-${HOME}/.config/transmission-daemon}"
MAGNET_FILE=""
CSV_FILE=""
USE_DAEMON=false
CLEAR_QUEUE=false
ADD_AFTER_CLEAR=false
TRASH_DATA=false

usage() {
  echo "Usage: $0 [--daemon] [--clear [--add]] [--trash] [-H host] [-p port] [-n user:pass] [-w download_dir] [-f magnets.txt] [-c magnets.csv] [magnet ...]" >&2
  echo "  --clear          Clear existing queue only (default: do NOT add)" >&2
  echo "  --add            Add magnets after --clear (opt-in)" >&2
}

# First pass for long options so we can keep getopts for short ones
_REST=()
while [ $# -gt 0 ]; do
  case "$1" in
    --daemon) USE_DAEMON=true; shift ;;
    --clear) CLEAR_QUEUE=true; shift ;;
    --add) ADD_AFTER_CLEAR=true; shift ;;
    --trash) TRASH_DATA=true; shift ;;
    --) shift; break ;;
    --*) echo "Unknown option $1" >&2; usage; exit 2 ;;
    *) _REST+=("$1"); shift ;;
  esac
done
set -- "${_REST[@]}" "$@"

while getopts ":H:p:n:w:f:c:h" opt; do
  case $opt in
    H) RPC_HOST="$OPTARG" ;;
    p) RPC_PORT="$OPTARG" ;;
    n) RPC_AUTH="$OPTARG" ;;
    w) DOWNLOAD_DIR="$OPTARG" ;;
    f) MAGNET_FILE="$OPTARG" ;;
    c) CSV_FILE="$OPTARG" ;;
    h) usage; exit 0 ;;
    :) echo "Missing argument for -$OPTARG" >&2; usage; exit 2 ;;
    \?) echo "Unknown option -$OPTARG" >&2; usage; exit 2 ;;
  esac
done
shift $((OPTIND-1))

# Build transmission-remote base args
TR=(transmission-remote "$RPC_HOST":"$RPC_PORT")
if [ -n "$RPC_AUTH" ]; then TR+=( -n "$RPC_AUTH" ); fi
if [ -n "$DOWNLOAD_DIR" ]; then TR+=( -w "$DOWNLOAD_DIR" ); fi

# If using daemon mode, ensure daemon is reachable (start if needed)
if [ "$USE_DAEMON" = true ]; then
  if ! "${TR[@]}" -si >/dev/null 2>&1; then
    echo "[transmission] RPC not reachable; starting local transmission-daemon..."
    mkdir -p "$CONF_DIR" "$DOWNLOAD_DIR"
    transmission-daemon -g "$CONF_DIR" -w "$DOWNLOAD_DIR" >/dev/null 2>&1 || true
    for i in {1..20}; do
      if "${TR[@]}" -si >/dev/null 2>&1; then break; fi
      sleep 0.5
    done
    if ! "${TR[@]}" -si >/dev/null 2>&1; then
      echo "[transmission] ERROR: Unable to contact RPC at ${RPC_HOST}:${RPC_PORT}" >&2
      exit 1
    fi
  fi
fi

# Collect magnet links from CLI, file, or CSV
MAGNETS=()
for m in "$@"; do
  if [[ "$m" == magnet:* ]]; then MAGNETS+=("$m"); fi
done

if [ -n "$MAGNET_FILE" ]; then
  while IFS= read -r line; do
    [[ -n "$line" && "$line" == magnet:* ]] && MAGNETS+=("$line")
  done < "$MAGNET_FILE"
fi

if [ -n "$CSV_FILE" ]; then
  # Extract 'magnet' column robustly. Handle NUL bytes and encoding issues.
  while IFS= read -r m; do
    [ -n "$m" ] && MAGNETS+=("$m")
  done < <(python3 - "$CSV_FILE" <<'PY'
import csv, sys, io
path=sys.argv[1]
raw=open(path,'rb').read()
if b"\x00" in raw:
  raw=raw.replace(b"\x00", b"")
text=raw.decode('utf-8','replace')
f=io.StringIO(text)
r=csv.DictReader(f)
if not r.fieldnames:
  raise SystemExit(0)
col='magnet' if 'magnet' in r.fieldnames else None
if not col:
  for c in ('magnets','Magnet','MAGNET'):
    if c in r.fieldnames:
      col=c; break
if not col:
  raise SystemExit(0)
for row in r:
  val=(row.get(col) or '').strip()
  if val.startswith('magnet:'):
    parts=[p.strip() for p in val.split('|')]
    for p in parts:
      if p.startswith('magnet:'):
        print(p)
PY
  )
fi

# It's valid to have no magnets when using --clear (clear-only mode)
if [ ${#MAGNETS[@]} -eq 0 ] && [ "$CLEAR_QUEUE" != true ]; then
  echo "[transmission] No magnets provided (and --clear not set)." >&2
  usage; exit 2
fi

if [ "$USE_DAEMON" = true ]; then
  if [ "$CLEAR_QUEUE" = true ]; then
    echo "[transmission] Clearing daemon queue..."
    IDS=$("${TR[@]}" -l | awk 'NR>1 && $1 ~ /^[0-9]+/ {print $1}')
    for id in $IDS; do
      if [ "$TRASH_DATA" = true ]; then
        "${TR[@]}" -t "$id" -rad >/dev/null || true
      else
        "${TR[@]}" -t "$id" -r >/dev/null || true
      fi
    done
    if [ "$ADD_AFTER_CLEAR" != true ]; then
      echo "[transmission] --clear requested without --add; not adding magnets. Done."
      exit 0
    fi
  fi
  echo "[transmission] Adding ${#MAGNETS[@]} magnet(s) to daemon ${RPC_HOST}:${RPC_PORT} -> $DOWNLOAD_DIR"
  for m in "${MAGNETS[@]}"; do
    echo "  + $m"
    "${TR[@]}" -a "$m"
  done
  echo "[transmission] Current daemon torrents:"
  "${TR[@]}" -l || true
else
  # GUI mode
  if [ "$CLEAR_QUEUE" = true ]; then
    if command -v osascript >/dev/null 2>&1; then
      echo "[transmission] Clearing GUI queue via AppleScript..."
      osascript <<'OSA'
tell application "Transmission"
  try
    repeat with t in transfers
      remove t
    end repeat
  end try
end tell
OSA
    else
      echo "[transmission] CLEAR requested, but GUI clear is only implemented for macOS (skipping)"
    fi
    if [ "$ADD_AFTER_CLEAR" != true ]; then
      echo "[transmission] --clear requested without --add; not adding magnets. Done."
      exit 0
    fi
  fi
  echo "[transmission] Adding ${#MAGNETS[@]} magnet(s) to Transmission GUI"
  for m in "${MAGNETS[@]}"; do
    if command -v open >/dev/null 2>&1; then
      open -a "Transmission" "$m" || open "$m"
    elif command -v transmission-gtk >/dev/null 2>&1; then
      transmission-gtk "$m" &>/dev/null &
    elif command -v transmission-qt >/dev/null 2>&1; then
      transmission-qt "$m" &>/dev/null &
    elif command -v xdg-open >/dev/null 2>&1; then
      xdg-open "$m" >/dev/null 2>&1 &
    else
      echo "[transmission] No suitable GUI opener found for magnets on this system" >&2
      exit 1
    fi
    sleep 0.2
  done
  echo "[transmission] Done. Check the Transmission app window."
fi
