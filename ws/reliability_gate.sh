#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-full}"
LOOP_COUNT="${LOOP_COUNT:-20}"
STMT_CORE_PATTERN='TestStmtDisconnectFixedBehavior|TestStmtResponseBeforeServerClose|TestSTMTReconnect'
STMT_RECONNECT_PATTERN='TestSTMTReconnect'
SCHEMALESS_RECONNECT_PATTERN='TestSchemalessReconnect'
TMQ_RECONNECT_PATTERN='TestReconnectStaleFailureDoesNotClearActiveClient|TestReconnectDeadReplacementDoesNotShortCircuit'

# unified cross-failover suite
CROSS_FAILOVER_TESTS=(
  "TestUnifiedCrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedCrossConcurrentSendFailoverAndSwitchBack"
  "TestUnifiedCrossMultiNodeFailoverChainUnderConcurrency"
  "TestUnifiedCrossDualNodeJitterWithConcurrentSchemalessWrites"
  "TestUnifiedTMQCrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedTMQCrossConcurrentPollFailoverAndSwitchBack"
  "TestUnifiedTMQCrossMultiNodeFailoverChainUnderConcurrency"
  "TestUnifiedTMQCrossDualNodeJitterWithConcurrentPoll"
  "TestUnifiedQueryCrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedQueryResultStatefulFetchNoReconnectOnDisconnect"
  "TestUnifiedQueryCrossConcurrentExecFailoverAndSwitchBack"
  "TestUnifiedQueryCrossMultiNodeFailoverChainUnderConcurrency"
  "TestUnifiedQueryCrossDualNodeJitterWithConcurrentExec"
  "TestUnifiedStmtCrossFailoverDisconnectDetectionAndImmediateReconnect"
  "TestUnifiedStmtCrossConcurrentExecFailoverAndSwitchBack"
  "TestUnifiedStmtCrossMultiNodeFailoverChainUnderConcurrency"
  "TestUnifiedStmtCrossDualNodeJitterWithConcurrentExec"
)

LOOP_TESTS=(
  "TestUnifiedCrossDualNodeJitterLoop"
  "TestUnifiedTMQCrossDualNodeJitterLoop"
  "TestUnifiedQueryCrossDualNodeJitterLoop"
  "TestUnifiedStmtCrossDualNodeJitterLoop"
)

cd "${ROOT_DIR}"

join_by_pipe() {
  local out=""
  local item
  for item in "$@"; do
    if [[ -z "${out}" ]]; then
      out="${item}"
    else
      out="${out}|${item}"
    fi
  done
  printf '%s' "${out}"
}

run_unified_cross_failover_smoke() {
  go test ./ws/unified -run "TestUnifiedCrossFailoverDisconnectDetectionAndImmediateReconnect|TestUnifiedTMQCrossFailoverDisconnectDetectionAndImmediateReconnect|TestUnifiedQueryCrossFailoverDisconnectDetectionAndImmediateReconnect|TestUnifiedStmtCrossFailoverDisconnectDetectionAndImmediateReconnect" -count=1
}

run_unified_cross_failover_once() {
  local pattern
  pattern="$(join_by_pipe "${CROSS_FAILOVER_TESTS[@]}")"
  go test ./ws/unified -run "${pattern}" -count=1
}

run_unified_cross_failover_loop() {
  local pattern
  pattern="$(join_by_pipe "${LOOP_TESTS[@]}")"
  LOOP_COUNT="${LOOP_COUNT}" go test ./ws/unified -run "${pattern}" -count=1
}

run_pkg_by_pkg() {
  go test -race ./ws/client -count=1
  go test -race ./ws/stmt -run "${STMT_CORE_PATTERN}" -count=1
  go test -race ./ws/schemaless -count=1
  go test -race ./ws/tmq -count=1
}

run_core_reconnect_regressions() {
  go test -race ./ws/tmq -run "${TMQ_RECONNECT_PATTERN}" -count=1
  go test -race ./ws/schemaless -run "${SCHEMALESS_RECONNECT_PATTERN}" -count=1
  go test -race ./ws/stmt -run "${STMT_RECONNECT_PATTERN}" -count=1
}

run_core_reconnect_regressions_loop() {
  go test -race ./ws/tmq -run "${TMQ_RECONNECT_PATTERN}" -count="${LOOP_COUNT}"
  go test -race ./ws/schemaless -run "${SCHEMALESS_RECONNECT_PATTERN}" -count="${LOOP_COUNT}"
  go test -race ./ws/stmt -run "${STMT_RECONNECT_PATTERN}" -count="${LOOP_COUNT}"
}

run_deterministic_full() {
  go test -race ./ws/client ./ws/internal/reconnect ./ws/schemaless ./ws/tmq ./ws/stmt -run "${STMT_CORE_PATTERN}" -count=1
}

run_full_base() {
  run_deterministic_full
  run_pkg_by_pkg
  run_core_reconnect_regressions
  run_core_reconnect_regressions_loop
  run_unified_cross_failover_once
}

run_full_integration() {
  go test -race ./ws/... -count=1
  run_full_base
  run_unified_cross_failover_loop
}

run_loop_full() {
  go test -race ./ws/tmq ./ws/schemaless ./ws/stmt -count="${LOOP_COUNT}"
  run_unified_cross_failover_loop
}

run_cross_mode() {
  local mode="${1}"
  case "${mode}" in
    cross-smoke)
      run_unified_cross_failover_smoke
      ;;
    cross-full)
      run_unified_cross_failover_once
      ;;
    cross-loop)
      run_unified_cross_failover_loop
      ;;
    cross-full-loop)
      run_unified_cross_failover_once
      run_unified_cross_failover_loop
      ;;
    *)
      echo "invalid cross mode: ${mode}" >&2
      return 1
      ;;
  esac
}

case "${MODE}" in
  cross-smoke|cross-full|cross-loop|cross-full-loop)
    run_cross_mode "${MODE}"
    exit 0
    ;;
esac

case "${MODE}" in
  quick)
    run_core_reconnect_regressions
    run_unified_cross_failover_smoke
    ;;
  loop)
    run_core_reconnect_regressions_loop
    run_unified_cross_failover_loop
    ;;
  loop-full)
    run_loop_full
    ;;
  full)
    run_full_base
    ;;
  full-integration)
    run_full_integration
    ;;
  *)
    echo "usage: ws/reliability_gate.sh [quick|loop|loop-full|full|full-integration|cross-smoke|cross-full|cross-loop|cross-full-loop]" >&2
    exit 1
    ;;
esac
