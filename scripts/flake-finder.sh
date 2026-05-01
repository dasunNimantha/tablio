#!/usr/bin/env bash
# Flake finder — run the fast test suites N times back-to-back and fail
# the job if ANY single iteration is non-zero. Surfaces flakes before
# they pollute PR signal.
#
# Defaults (override via env):
#   ITERATIONS    number of repeated runs per suite (default 20)
#   LOG_DIR       per-iteration logs are written here (default flake-finder-logs)
#   SKIP_FRONTEND set to 1 to skip the npm test loop (e.g. when no node tools)
#   SKIP_BACKEND  set to 1 to skip the cargo test loop
#
# Usage (locally):
#   ITERATIONS=5 ./scripts/flake-finder.sh
#
# Usage (CI):
#   bash scripts/flake-finder.sh
#
# The runner must have:
#   - rust toolchain (cargo)
#   - node (npm)        — only when SKIP_FRONTEND != 1
# installed up-front. We don't bootstrap them here so this stays a thin
# loop that's quick to debug.

set -uo pipefail

ITERATIONS="${ITERATIONS:-20}"
LOG_DIR="${LOG_DIR:-flake-finder-logs}"
SKIP_FRONTEND="${SKIP_FRONTEND:-0}"
SKIP_BACKEND="${SKIP_BACKEND:-0}"

mkdir -p "$LOG_DIR"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

declare -A SUITE_FAILURES
declare -A SUITE_TOTALS

run_loop() {
    local suite="$1"
    local cmd="$2"
    local fails=0

    echo "::group::$suite x$ITERATIONS"
    for i in $(seq 1 "$ITERATIONS"); do
        local log="$LOG_DIR/${suite}-${i}.log"
        local ts_start
        ts_start=$(date -u +%s)
        if eval "$cmd" >"$log" 2>&1; then
            local elapsed=$(( $(date -u +%s) - ts_start ))
            echo "  [$suite] iter $i: PASS (${elapsed}s)"
        else
            local elapsed=$(( $(date -u +%s) - ts_start ))
            echo "  [$suite] iter $i: FAIL (${elapsed}s) — see $log"
            fails=$(( fails + 1 ))
        fi
    done
    echo "::endgroup::"

    SUITE_FAILURES["$suite"]="$fails"
    SUITE_TOTALS["$suite"]="$ITERATIONS"
}

if [ "$SKIP_BACKEND" != "1" ]; then
    run_loop "cargo-test-lib" \
        "cargo test --manifest-path src-tauri/Cargo.toml --lib --quiet"
fi

if [ "$SKIP_FRONTEND" != "1" ]; then
    if ! command -v npm >/dev/null 2>&1; then
        echo "::warning::npm not found, skipping frontend flake loop"
    else
        # Reuse the same install across iterations — npm ci once, vitest
        # uses --run which exits 0/non-zero per run.
        if [ ! -d node_modules ]; then
            npm ci --no-audit --no-fund
        fi
        run_loop "vitest-run" "npx vitest run --reporter=dot"
    fi
fi

# ---- summary ---------------------------------------------------------------
total_fail=0
{
    echo "## Flake finder results"
    echo
    echo "| Suite | Iterations | Failures |"
    echo "| --- | ---: | ---: |"
    for suite in "${!SUITE_TOTALS[@]}"; do
        fails=${SUITE_FAILURES[$suite]}
        total=${SUITE_TOTALS[$suite]}
        echo "| \`$suite\` | $total | $fails |"
        total_fail=$(( total_fail + fails ))
    done
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"

if [ "$total_fail" -gt 0 ]; then
    echo "::error::flake-finder detected $total_fail failing iteration(s)"
    exit 1
fi
echo "All iterations passed."
