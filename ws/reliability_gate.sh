#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-full}"
LOOP_COUNT="${LOOP_COUNT:-20}"
STMT_CORE_PATTERN='TestStmtDisconnectFixedBehavior|TestStmtResponseBeforeServerClose|TestReconnectHealthyReplacementShortCircuit|TestReconnectDeadReplacementDoesNotShortCircuit|TestReconnectFailureClosesMatchedFailedConn|TestReconnectFailureDoesNotCloseActiveReplacement'

cd "${ROOT_DIR}"

run_pkg_by_pkg() {
  go test -race ./ws/client -count=1
  go test -race ./ws/stmt -run "${STMT_CORE_PATTERN}" -count=1
  go test -race ./ws/schemaless -count=1
  go test -race ./ws/tmq -count=1
}

run_core_reconnect_regressions() {
  go test -race ./ws/tmq -run 'TestReconnectStaleFailureDoesNotClearActiveClient|TestReconnectDeadReplacementDoesNotShortCircuit' -count=1
  go test -race ./ws/schemaless -run 'TestReconnectStaleFailureDoesNotClearActiveClient|TestReconnectDeadReplacementDoesNotShortCircuit' -count=1
  go test -race ./ws/stmt -run 'TestReconnectHealthyReplacementShortCircuit|TestReconnectDeadReplacementDoesNotShortCircuit|TestReconnectFailureClosesMatchedFailedConn|TestReconnectFailureDoesNotCloseActiveReplacement' -count=1
}

run_core_reconnect_regressions_loop() {
  go test -race ./ws/tmq -run 'TestReconnectStaleFailureDoesNotClearActiveClient|TestReconnectDeadReplacementDoesNotShortCircuit' -count="${LOOP_COUNT}"
  go test -race ./ws/schemaless -run 'TestReconnectStaleFailureDoesNotClearActiveClient|TestReconnectDeadReplacementDoesNotShortCircuit' -count="${LOOP_COUNT}"
  go test -race ./ws/stmt -run 'TestReconnectHealthyReplacementShortCircuit|TestReconnectDeadReplacementDoesNotShortCircuit|TestReconnectFailureClosesMatchedFailedConn|TestReconnectFailureDoesNotCloseActiveReplacement' -count="${LOOP_COUNT}"
}

run_deterministic_full() {
  go test -race ./ws/client ./ws/internal/reconnect ./ws/schemaless ./ws/tmq -count=1
  go test -race ./ws/stmt -run "${STMT_CORE_PATTERN}" -count=1
}

case "${MODE}" in
  quick)
    run_core_reconnect_regressions
    ;;
  loop)
    run_core_reconnect_regressions_loop
    ;;
  loop-full)
    go test -race ./ws/tmq ./ws/schemaless -count="${LOOP_COUNT}"
    ;;
  full)
    run_deterministic_full
    run_pkg_by_pkg
    run_core_reconnect_regressions
    run_core_reconnect_regressions_loop
    ;;
  full-integration)
    go test -race ./ws/... -count=1
    run_pkg_by_pkg
    run_core_reconnect_regressions
    run_core_reconnect_regressions_loop
    ;;
  *)
    echo "usage: ws/reliability_gate.sh [quick|loop|loop-full|full|full-integration]" >&2
    exit 1
    ;;
esac
