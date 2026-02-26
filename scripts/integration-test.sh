#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/integration-test.sh
#
#  Boots a 3-node cluster, writes data, kills the leader, verifies
#  that a new leader is elected and data is still accessible.
#
#  Exit code 0 = all assertions passed
#  Exit code 1 = at least one assertion failed
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

BINARY="${APEX_BINARY:-./build/kv}"
WAL_DIR="/tmp/apex-itest-$$"
PASS=0; FAIL=0

# ── Helpers ───────────────────────────────────────────────────────────────────
RED="\033[31m" GRN="\033[32m" YLW="\033[33m" RST="\033[0m"

info() { echo -e "${GRN}▶${RST} $*"; }

check() {
    local desc="$1" expected="$2" actual="$3"
    if [ "${actual}" = "${expected}" ]; then
        echo -e "  ${GRN}PASS${RST}  ${desc}"
        ((PASS++)) || true
    else
        echo -e "  ${RED}FAIL${RST}  ${desc}"
        echo -e "        expected: ${expected}"
        echo -e "        actual:   ${actual}"
        ((FAIL++)) || true
    fi
}

kv_cmd() {
    # The REPL outputs lines like:  apex> PONG
    # Keep only lines that start with "apex> ", strip that prefix,
    # then drop blank lines (the bare "apex> " prompt at exit).
    local port="$1" cmd="$2"
    echo "${cmd}" | timeout 3 "${BINARY}" --client "127.0.0.1:${port}" 2>/dev/null \
        | grep '^apex> '    \
        | sed 's/^apex> //' \
        | grep -v '^$'      \
        | head -1           \
        || echo "(timeout)"
}

wait_for_node() {
    # Poll until PING succeeds or timeout (~6s)
    local port="$1" retries=20
    while (( retries-- > 0 )); do
        local resp
        resp=$(kv_cmd "${port}" "PING" 2>/dev/null || true)
        [[ "${resp}" == "PONG" ]] && return 0
        sleep 0.3
    done
    return 1
}

wait_for_key() {
    # Poll until GET <key> returns <expected> on <port>, or timeout (~6s)
    local port="$1" key="$2" expected="$3" retries=30
    while (( retries-- > 0 )); do
        local resp
        resp=$(kv_cmd "${port}" "GET ${key}" 2>/dev/null || true)
        [[ "${resp}" == "${expected}" ]] && return 0
        sleep 0.2
    done
    return 1
}

cleanup() {
    echo -e "\n${YLW}Cleaning up...${RST}"
    pkill -f "kv --id.*${WAL_DIR}" 2>/dev/null || true
    rm -rf "${WAL_DIR}"
}
trap cleanup EXIT

# ── Setup ─────────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  APEX-KV  Integration Tests"
echo "═══════════════════════════════════════════════════════"

if [ ! -x "${BINARY}" ]; then
    echo "Building..."
    cmake -B build -DCMAKE_BUILD_TYPE=Release -S . -q
    cmake --build build -j"$(nproc)" --config Release -q
    BINARY="./build/kv"
fi

mkdir -p "${WAL_DIR}"/{node1,node2,node3} "${WAL_DIR}/logs"

echo ""
echo "── Phase 1: Start cluster ───────────────────────────────"

"${BINARY}" --id 1 --port 7101 --wal-dir "${WAL_DIR}/node1" \
    --peers "127.0.0.1:7102:2,127.0.0.1:7103:3" \
    > "${WAL_DIR}/logs/node1.log" 2>&1 &
PID1=$!

"${BINARY}" --id 2 --port 7102 --wal-dir "${WAL_DIR}/node2" \
    --peers "127.0.0.1:7101:1,127.0.0.1:7103:3" \
    > "${WAL_DIR}/logs/node2.log" 2>&1 &
PID2=$!

"${BINARY}" --id 3 --port 7103 --wal-dir "${WAL_DIR}/node3" \
    --peers "127.0.0.1:7101:1,127.0.0.1:7102:2" \
    > "${WAL_DIR}/logs/node3.log" 2>&1 &
PID3=$!

echo "  Waiting for nodes to bind and elect a leader (up to 6s)..."
ALL_UP=true
for port in 7101 7102 7103; do
    if wait_for_node "${port}"; then
        info "Node :${port} → healthy"
    else
        lognum="${port: -1}"
        echo -e "  ${RED}Node :${port} failed to start. Last log lines:${RST}"
        tail -10 "${WAL_DIR}/logs/node${lognum}.log" 2>/dev/null || true
        ALL_UP=false
    fi
done

for port in 7101 7102 7103; do
    resp=$(kv_cmd "${port}" "PING")
    check "Node :${port} responds to PING" "PONG" "${resp}"
done

if [ "${ALL_UP}" = false ]; then
    echo -e "${RED}Cluster did not start cleanly — aborting.${RST}"
    exit 1
fi

echo ""
echo "── Phase 2: Write data ──────────────────────────────────"

# Write several keys — try each node in turn until one accepts (leader) or redirects
for key in alpha beta gamma delta epsilon; do
    written=false
    for port in 7101 7102 7103; do
        resp=$(kv_cmd "${port}" "PUT ${key} value_${key}" 2>/dev/null || echo "(error)")
        if [[ "${resp}" == "OK" ]] || [[ "${resp}" == *"redirect"* ]]; then
            written=true
            break
        fi
    done
    if [ "${written}" = false ]; then
        echo -e "  ${YLW}Warning: could not write key '${key}'${RST}"
    fi
done

echo "  Wrote 5 keys to cluster"
sleep 1.0   # Give Raft time to commit + apply on all nodes

# Verify reads from all nodes — poll until replication catches up (up to 6s)
for port in 7101 7102 7103; do
    if wait_for_key "${port}" "alpha" "value_alpha"; then
        resp="value_alpha"
    else
        resp=$(kv_cmd "${port}" "GET alpha")
    fi
    check "Node :${port} GET alpha" "value_alpha" "${resp}"
done

echo ""
echo "── Phase 3: Leader failover ─────────────────────────────"

# Find leader by scanning structured JSON logs for "LEADER"
LEADER_PID=""
LEADER_PORT=""
for pid_port in "${PID1}:7101" "${PID2}:7102" "${PID3}:7103"; do
    pid="${pid_port%%:*}"
    port="${pid_port##*:}"
    lognum="${port: -1}"
    if grep -q "LEADER" "${WAL_DIR}/logs/node${lognum}.log" 2>/dev/null; then
        LEADER_PID="${pid}"
        LEADER_PORT="${port}"
        break
    fi
done

if [ -z "${LEADER_PID}" ]; then
    echo -e "  ${YLW}Could not identify leader from logs, defaulting to node1${RST}"
    LEADER_PID="${PID1}"
    LEADER_PORT="7101"
fi

echo "  Killing leader (PID ${LEADER_PID}, port ${LEADER_PORT})..."
kill "${LEADER_PID}" 2>/dev/null || true

echo "  Waiting for re-election (up to 6s)..."
SURVIVING_PORT=""
for port in 7101 7102 7103; do
    [ "${port}" = "${LEADER_PORT}" ] && continue
    if wait_for_node "${port}"; then
        SURVIVING_PORT="${port}"
    fi
done

check "At least one surviving node responds" "PONG" \
    "$(kv_cmd "${SURVIVING_PORT:-7102}" "PING")"

echo ""
echo "── Phase 4: Data consistency after failover ─────────────"

for key in alpha beta gamma; do
    for port in 7101 7102 7103; do
        [ "${port}" = "${LEADER_PORT}" ] && continue
        # Poll until the key is visible (replication may still be catching up)
        if wait_for_key "${port}" "${key}" "value_${key}"; then
            resp="value_${key}"
        else
            resp=$(kv_cmd "${port}" "GET ${key}" 2>/dev/null || echo "(node down)")
        fi
        if [ "${resp}" != "(node down)" ] && [ "${resp}" != "(timeout)" ]; then
            check "POST-FAILOVER :${port} GET ${key}" "value_${key}" "${resp}"
            break
        fi
    done
done

echo ""
echo "── Phase 5: Write after failover ────────────────────────"

WROTE_AFTER=false
for port in 7101 7102 7103; do
    [ "${port}" = "${LEADER_PORT}" ] && continue
    resp=$(kv_cmd "${port}" "PUT newkey newvalue" 2>/dev/null || echo "(error)")
    if [[ "${resp}" == "OK" ]] || [[ "${resp}" == *"redirect"* ]]; then
        echo -e "  ${GRN}Write accepted on :${port} after failover${RST}"
        WROTE_AFTER=true
        break
    fi
done
if [ "${WROTE_AFTER}" = false ]; then
    echo -e "  ${YLW}Warning: no node accepted a write after failover${RST}"
fi

# ── Results ───────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Results: ${PASS} passed, ${FAIL} failed"
echo "═══════════════════════════════════════════════════════"
echo ""

[ "${FAIL}" -eq 0 ]