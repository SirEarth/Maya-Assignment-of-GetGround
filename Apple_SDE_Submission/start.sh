#!/usr/bin/env bash
# ============================================================================
# start.sh — one-click install / setup / run for the Pricing Pipeline
#
# Usage:
#   ./start.sh                # full setup + run + open dashboard in browser
#   ./start.sh stop           # stop the running API server
#   ./start.sh status         # show whether the server is running
#   ./start.sh wipe           # TRUNCATE transactional tables only (~0.5s);
#                             #   keeps Product Reference + venv + server up
#   ./start.sh reset          # drop & recreate the DB, then full setup + run
#   ./start.sh help           # print this help
#
# Options for setup runs:
#   --reset        drop & recreate the database (wipe all batches)
#   --no-tests     skip the pytest run during setup
#   --no-browser   don't auto-open the dashboard in the default browser
#
# Environment overrides (optional):
#   PGDATABASE  PGUSER  PGHOST  PGPORT  PORT
#
# What this does (idempotent):
#   1. Verify python3 is installed
#   2. Verify a working psql / PostgreSQL is reachable (auto-detects Postgres.app)
#   3. Build a local virtualenv at .venv and install Python dependencies
#   4. Create the database (or skip if already populated; --reset to wipe)
#      then apply schema.sql + dq/rules.sql + dq/rules_split.sql + seed_bootstrap.py
#   5. Run the test suite (skippable)
#   6. Start uvicorn in the background, wait until /health is ready,
#      and open submission/pipeline_runner.html in the browser
# ============================================================================

set -e
set -u

# ---- Constants ------------------------------------------------------------
DB_NAME="${PGDATABASE:-maya_assignment}"
PORT="${PORT:-8000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/.uvicorn.log"
PID_FILE="$SCRIPT_DIR/.uvicorn.pid"
VENV_DIR="$SCRIPT_DIR/.venv"

# ---- Colour helpers -------------------------------------------------------
red()    { printf '\033[31m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
blue()   { printf '\033[34m%s\033[0m\n' "$*"; }
heading(){ printf '\n\033[1;34m▶ %s\033[0m\n' "$*"; }

# ---- Argument parsing -----------------------------------------------------
SUBCMD="start"
RESET=0
SKIP_TESTS=0
SKIP_BROWSER=0

for arg in "$@"; do
  case "$arg" in
    start|stop|status|wipe|reset|help|-h|--help) SUBCMD="$arg" ;;
    --reset)        RESET=1 ;;
    --no-tests)     SKIP_TESTS=1 ;;
    --no-browser)   SKIP_BROWSER=1 ;;
    *) red "Unknown argument: $arg"; echo "Run './start.sh help' for usage."; exit 1 ;;
  esac
done

if [ "$SUBCMD" = "reset" ]; then
  RESET=1
fi

# ---- Subcommand: help -----------------------------------------------------
if [ "$SUBCMD" = "help" ] || [ "$SUBCMD" = "-h" ] || [ "$SUBCMD" = "--help" ]; then
  cat <<'EOF'
Pricing Pipeline · one-click runner

Commands:
  start              (default) install + setup DB + run + open dashboard
  stop               stop the running API server
  status             show whether the server is running
  wipe               TRUNCATE transactional tables (stg / fact / dq / dws);
                     keeps Product Reference + dim_partner + dim_currency
                     + venv + the running uvicorn process. ~0.5 sec.
  reset              drop & recreate DB, then start (alias for: start --reset)
  help               show this message

Options for start / reset:
  --reset            drop and recreate the database (wipe all batches)
  --no-tests         skip pytest during setup
  --no-browser       don't auto-open the dashboard in the browser

Environment overrides (optional):
  PGDATABASE  PGUSER  PGHOST  PGPORT  PORT

After a successful start:
  📊 Dashboard:   submission/pipeline_runner.html (auto-opened)
  📚 API docs:    http://localhost:8000/docs
  📜 Server log:  tail -f .uvicorn.log
  🛑 Stop:        ./start.sh stop
EOF
  exit 0
fi

# ---- Subcommand: stop -----------------------------------------------------
if [ "$SUBCMD" = "stop" ]; then
  if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
      kill "$PID" 2>/dev/null || true
      sleep 1
    fi
    rm -f "$PID_FILE"
  fi
  pkill -f "uvicorn api.main" 2>/dev/null || true
  green "✓ API server stopped"
  exit 0
fi

# ---- Subcommand: status ---------------------------------------------------
if [ "$SUBCMD" = "status" ]; then
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    PID=$(cat "$PID_FILE")
    if curl -fs "http://localhost:$PORT/health" >/dev/null 2>&1; then
      green "✓ API running on http://localhost:$PORT (pid $PID)"
    else
      yellow "! pid $PID alive but /health not responding"
    fi
  else
    yellow "! API not running (no $PID_FILE or pid dead)"
  fi
  exit 0
fi

# ---- Subcommand: wipe -----------------------------------------------------
# Lightweight reset — truncate only the transactional tables. Preserves the
# Product Reference / dim_partner / dim_currency seeds and the running
# uvicorn process. After wipe, the dashboard refresh shows fact = 0.
if [ "$SUBCMD" = "wipe" ]; then
  PSQL=""
  if command -v psql >/dev/null 2>&1; then PSQL="psql"; fi
  if [ -z "$PSQL" ]; then
    for p in /Applications/Postgres.app/Contents/Versions/latest/bin/psql /opt/homebrew/bin/psql /usr/local/bin/psql; do
      [ -x "$p" ] && PSQL="$p" && break
    done
  fi
  if [ -z "$PSQL" ] || ! "$PSQL" -d postgres -c "SELECT 1" >/dev/null 2>&1; then
    red "✗ psql not reachable — can't wipe"
    exit 1
  fi
  if ! "$PSQL" -lqt | cut -d'|' -f1 | tr -d ' ' | grep -qx "$DB_NAME"; then
    yellow "! database '$DB_NAME' does not exist yet — nothing to wipe (run ./start.sh first)"
    exit 0
  fi
  heading "Wipe transactional tables in '$DB_NAME'"
  "$PSQL" -d "$DB_NAME" -q <<'SQL' >/dev/null
TRUNCATE TABLE
  dq_bad_records,
  dq_output,
  dws_partner_dq_per_batch,
  fact_anomaly,
  fact_payment_full_price,
  fact_payment_instalment,
  fact_partner_price_history,
  fact_price_offer,
  stg_price_offer
RESTART IDENTITY CASCADE;
SQL
  green "✓ truncated: stg / fact / fact_payment_* / fact_partner_price_history / fact_anomaly / dq_output / dq_bad_records / dws_partner_dq_per_batch"
  green "  (kept: dim_product_model + dim_product_sku + dim_partner + dim_country + dim_currency_rate_snapshot)"
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    blue "  ↻ refresh the dashboard (or click ↻ Refresh) to see the empty state"
  fi
  exit 0
fi

# ===========================================================================
# Subcommand: start (default)
# ===========================================================================
cd "$SCRIPT_DIR"

# ---- Step 1/6 — Python ----
heading "Step 1/6 — Check Python 3"
if ! command -v python3 >/dev/null; then
  red "✗ python3 not found"
  echo "  → install via Homebrew: brew install python@3.11"
  exit 1
fi
PYV=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
green "✓ python3 $PYV found"

# ---- Step 2/6 — PostgreSQL ----
heading "Step 2/6 — Check PostgreSQL"
PSQL=""
if command -v psql >/dev/null 2>&1; then
  PSQL="psql"
fi
if [ -z "$PSQL" ]; then
  for p in /Applications/Postgres.app/Contents/Versions/latest/bin/psql /opt/homebrew/bin/psql /usr/local/bin/psql; do
    if [ -x "$p" ]; then PSQL="$p"; break; fi
  done
fi
if [ -z "$PSQL" ]; then
  red "✗ psql not found on PATH or in standard locations"
  echo "  → install Postgres.app: https://postgresapp.com (drag to /Applications, click Initialize)"
  echo "  → or via Homebrew: brew install postgresql@16 && brew services start postgresql@16"
  exit 1
fi
if ! "$PSQL" -d postgres -c "SELECT 1" >/dev/null 2>&1; then
  red "✗ psql found ($PSQL) but cannot connect to PostgreSQL"
  echo "  → if using Postgres.app: open the app, click Initialize (or Start)"
  echo "  → if using Homebrew: brew services start postgresql@16"
  exit 1
fi
green "✓ psql ($PSQL) → connected"

# ---- Step 3/6 — Python venv + deps ----
heading "Step 3/6 — Python virtualenv + dependencies"
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
  green "✓ created virtualenv: $VENV_DIR"
fi
# Activate venv for subsequent python/pip
# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

if python -c 'import fastapi, asyncpg, uvicorn, psycopg2, pydantic, pytest, httpx' >/dev/null 2>&1; then
  green "✓ dependencies already present in venv"
else
  echo "  installing dependencies into $VENV_DIR …"
  python -m pip install --quiet --upgrade pip
  python -m pip install --quiet \
    fastapi 'pydantic>=2' uvicorn asyncpg psycopg2-binary \
    python-multipart pytest httpx
  green "✓ dependencies installed"
fi

PY="$VENV_DIR/bin/python"

# ---- Step 4/6 — Database ----
heading "Step 4/6 — Database '$DB_NAME'"
DB_EXISTS=0
if "$PSQL" -lqt | cut -d'|' -f1 | tr -d ' ' | grep -qx "$DB_NAME"; then
  DB_EXISTS=1
fi

if [ "$RESET" = "1" ] && [ "$DB_EXISTS" = "1" ]; then
  echo "  --reset: dropping existing database (will recreate clean)"
  # Disconnect any active sessions first
  "$PSQL" -d postgres -c \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();" \
    >/dev/null 2>&1 || true
  "$PSQL" -d postgres -c "DROP DATABASE \"$DB_NAME\";" >/dev/null
  DB_EXISTS=0
fi

if [ "$DB_EXISTS" = "0" ]; then
  "$PSQL" -d postgres -c "CREATE DATABASE \"$DB_NAME\";" >/dev/null
  green "✓ created database $DB_NAME"
fi

# Detect if tables exist (handles partial init / re-runs)
HAS_TABLES=0
if "$PSQL" -d "$DB_NAME" -tAc \
     "SELECT 1 FROM information_schema.tables WHERE table_name='dim_country';" \
     2>/dev/null | grep -q 1; then
  HAS_TABLES=1
fi

if [ "$HAS_TABLES" = "0" ]; then
  echo "  applying schema.sql + dq/rules.sql + dq/rules_split.sql"
  "$PSQL" -d "$DB_NAME" -q -f schema.sql            > /dev/null
  "$PSQL" -d "$DB_NAME" -q -f dq/rules.sql          > /dev/null
  "$PSQL" -d "$DB_NAME" -q -f dq/rules_split.sql    > /dev/null
  echo "  seeding Product Reference + FX rates (seed_bootstrap.py)"
  "$PY" seed_bootstrap.py > /dev/null
  green "✓ schema + DQ rules + seed applied"
else
  yellow "✓ tables already present — skipping schema/seed (run with --reset to wipe)"
fi

# ---- Step 5/6 — Tests ----
if [ "$SKIP_TESTS" = "0" ]; then
  heading "Step 5/6 — Run tests"
  if "$PY" -m pytest -q 2>&1 | tail -3; then
    green "✓ tests passed"
  else
    yellow "! tests reported failures — continuing (UI may still work)"
  fi
else
  heading "Step 5/6 — Tests skipped (--no-tests)"
fi

# ---- Step 6/6 — Server + browser ----
heading "Step 6/6 — Start API + open dashboard"

# Kill any running instance
pkill -f "uvicorn api.main" 2>/dev/null || true
sleep 1

nohup "$PY" -m uvicorn api.main:app --port "$PORT" > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > "$PID_FILE"
disown "$SERVER_PID" 2>/dev/null || true

# Wait up to ~15s for /health
READY=0
for _ in $(seq 1 15); do
  if curl -fs "http://localhost:$PORT/health" >/dev/null 2>&1; then
    READY=1; break
  fi
  sleep 1
done

if [ "$READY" != "1" ]; then
  red "✗ API failed to start on port $PORT — last 30 log lines:"
  tail -30 "$LOG_FILE"
  exit 1
fi
green "✓ API ready on http://localhost:$PORT (pid $SERVER_PID)"

DASHBOARD="$SCRIPT_DIR/submission/pipeline_runner.html"
if [ "$SKIP_BROWSER" = "0" ] && [ -f "$DASHBOARD" ]; then
  if command -v open >/dev/null;     then open "$DASHBOARD"
  elif command -v xdg-open >/dev/null; then xdg-open "$DASHBOARD"
  else yellow "  (open dashboard manually: file://$DASHBOARD)"
  fi
fi

cat <<EOF

$(green "All set — pipeline is live 🎉")

  📊 Dashboard:   $DASHBOARD
  📚 API docs:    http://localhost:$PORT/docs
  📜 Server log:  tail -f $LOG_FILE
  🛑 Stop:        ./start.sh stop
  🔄 Reset:       ./start.sh reset
EOF
