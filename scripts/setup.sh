#!/usr/bin/env bash

# Detect if sourced (so we avoid exiting the parent shell on errors)
is_sourced=false
if [ -n "${ZSH_EVAL_CONTEXT:-}" ]; then
  case $ZSH_EVAL_CONTEXT in *:file) is_sourced=true;; esac
elif [ -n "${BASH_VERSION:-}" ]; then
  if [ "${BASH_SOURCE[0]}" != "$0" ]; then is_sourced=true; fi
fi

# Safer strictness: only enforce -e/-u when not sourced
if [ "$is_sourced" = true ]; then
  set -o pipefail
else
  set -Eeuo pipefail
fi

die() {
  echo "[setup] Error: $*" >&2
  if [ "$is_sourced" = true ]; then
    return 1
  else
    exit 1
  fi
}

# Move to repo root (script lives in scripts/)
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-${0}}")" &>/dev/null && pwd -P)
cd "$SCRIPT_DIR/.." || die "cannot cd to repo root"

# Choose a Python interpreter (env override or best available)
if [ -n "${PYTHON:-}" ]; then
  PY="$PYTHON"
else
  PY=""
  for c in python3 python3.12 python3.11 python3.10 python3.9; do
    if command -v "$c" >/dev/null 2>&1; then PY="$c"; break; fi
  done
fi
VENV_DIR=${VENV_DIR:-.venv}

echo "[setup] Using Python: $([ -n "$PY" ] && $PY -V 2>/dev/null || echo not found)"

[ -n "$PY" ] || die "python3 not found on PATH; install via Homebrew: brew install python"
command -v "$PY" >/dev/null 2>&1 || die "python not executable: $PY"

if [ ! -d "$VENV_DIR" ]; then
  echo "[setup] Creating venv at $VENV_DIR"
  "$PY" -m venv "$VENV_DIR" || die "failed to create venv"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate" || die "failed to activate venv"

# Use the venv's interpreter for all operations (explicit path avoids 'python' missing on some macOS)
PY_VENV="$VENV_DIR/bin/python"

# Ensure a python3 shim exists inside venv to avoid alias conflicts (alias python=python3)
if [ ! -x "$VENV_DIR/bin/python3" ] && [ -x "$VENV_DIR/bin/python" ]; then
  ln -sf "python" "$VENV_DIR/bin/python3" || true
fi

# Ensure venv bin is at front of PATH for this shell
case ":$PATH:" in
  *":$VENV_DIR/bin:"*) :;;
  *) export PATH="$VENV_DIR/bin:$PATH";;
esac

# Ensure pip exists inside the venv (some systems build venvs without pip)
if ! $PY_VENV -m pip --version >/dev/null 2>&1; then
  echo "[setup] Bootstrapping pip in venv"
  if ! $PY_VENV -m ensurepip --upgrade >/dev/null 2>&1; then
    # Fallback: get-pip.py (for Python builds without ensurepip)
    curl -fsSL https://bootstrap.pypa.io/get-pip.py | "$PY_VENV" - || die "pip bootstrap failed (ensurepip+get-pip)"
  fi
fi

$PY_VENV -m pip install --upgrade pip wheel setuptools >/dev/null || die "pip bootstrap failed"
$PY_VENV -m pip install -e . || { die "project install failed"; return 1; }

echo "[setup] Installed project into $VENV_DIR"

# Ensure requests is present (handles edge-cases where editable install didn’t pull deps)
if ! $PY_VENV -c "import requests" >/dev/null 2>&1; then
  echo "[setup] Installing runtime dependency: requests"
  $PY_VENV -m pip install "requests>=2.31.0" || die "failed to install requests"
fi

# Sanity check: core modules present
if ! $PY_VENV - <<'PY'
import sys
print('[setup] Python:', sys.executable)
import requests
print('[setup] Verify: requests', requests.__version__)
import cli, yts
print('[setup] Verify: project modules import OK')
PY
then
  die "verify failed (module import). Try: unalias python python3; hash -r; then source $VENV_DIR/bin/activate"
fi

if [ "$is_sourced" = true ]; then
  echo "[setup] Virtualenv is active in this shell."
else
  echo "[setup] To activate later, run: source $VENV_DIR/bin/activate"
fi

echo "[setup] Try the CLI: python -m cli jellyfin --min-rt 6 --verbose"
echo "[setup] Then enrich via YTS: python -m cli yts-jf --verbose"

# Create short CLI shims inside the venv (ml, mlm)
for name in ml mlm; do
  cat >"$VENV_DIR/bin/$name" <<'SH'
#!/usr/bin/env bash
set -e
VENV_BIN_DIR="$(cd -- "$(dirname -- "$0")" && pwd -P)"
PY="$VENV_BIN_DIR/python"
if [ "$1" = "yts" ]; then
  ARGS=("$@")
  have_seq=0
  for a in "$@"; do
    [ "$a" = "--sequential" ] && have_seq=1 && break
  done
  [ $have_seq -eq 0 ] && ARGS=("${ARGS[@]}" --sequential)
  exec "$PY" -m cli "${ARGS[@]}"
fi
exec "$PY" -m cli "$@"
SH
  chmod +x "$VENV_DIR/bin/$name" || true
done

echo "[setup] Short commands available: ml, mlm (may need: rehash)"

# Refresh command hash for current shell
hash -r 2>/dev/null || true
command -v rehash >/dev/null 2>&1 && rehash
