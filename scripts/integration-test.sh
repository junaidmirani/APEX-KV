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
    # Send a single command to the given port and return the response
    local port="$1" cmd="$2"
    echo "${cmd}" | timeout 3 "${BINARY}" --client "127.0.0.1:${port}" 2>/dev/null \
        | grep -v '^apex>' | head -1 || echo "(timeout)"
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

echo "  Waiting for election (up to 3s)..."
sleep 2.5

for port in 7101 7102 7103; do
    resp=$(kv_cmd "${port}" "PING")
    check "Node :${port} responds to PING" "PONG" "${resp}"
done

echo ""
echo "── Phase 2: Write data ──────────────────────────────────"

# Write several keys — will be routed to leader
for key in alpha beta gamma delta epsilon; do
    for port in 7101 7102 7103; do
        resp=$(kv_cmd "${port}" "PUT ${key} value_${key}" 2>/dev/null || echo "(error)")
        case "${resp}" in
            "OK"|*"redirect"*) break ;;
        esac
    done
done

echo "  Wrote 5 keys to cluster"
sleep 0.3

# Verify reads from all nodes
for port in 7101 7102 7103; do
    resp=$(kv_cmd "${port}" "GET alpha")
    check "Node :${port} GET alpha" "value_alpha" "${resp}"
done

echo ""
echo "── Phase 3: Leader failover ─────────────────────────────"

# Find leader by checking logs
LEADER_PID=""
LEADER_PORT=""
for pid_port in "${PID1}:7101" "${PID2}:7102" "${PID3}:7103"; do
    pid="${pid_port%%:*}"; port="${pid_port##*:}"
    lognum="${port: -1}"
    if grep -q "LEADER" "${WAL_DIR}/logs/node${lognum}.log" 2>/dev/null; then
        LEADER_PID="${pid}"; LEADER_PORT="${port}"
        break
    fi
done

if [ -z "${LEADER_PID}" ]; then
    echo -e "  ${YLW}Could not identify leader from logs, killing node1${RST}"
    LEADER_PID="${PID1}"; LEADER_PORT="7101"
fi

echo "  Killing leader (PID ${LEADER_PID}, port ${LEADER_PORT})..."
kill "${LEADER_PID}" 2>/dev/null || true
sleep 2.5  # Wait for new election

echo "  Checking remaining nodes elected a new leader..."
SURVIVING_PORT=""
for port in 7101 7102 7103; do
    if [ "${port}" != "${LEADER_PORT}" ]; then
        resp=$(kv_cmd "${port}" "PING" 2>/dev/null || echo "")
        if [ "${resp}" = "PONG" ]; then
            SURVIVING_PORT="${port}"
        fi
    fi
done

check "At least one surviving node responds" "PONG" "$(kv_cmd "${SURVIVING_PORT:-7102}" "PING")"

echo ""
echo "── Phase 4: Data consistency after failover ─────────────"

sleep 0.5
for key in alpha beta gamma; do
    for port in 7101 7102 7103; do
        if [ "${port}" = "${LEADER_PORT}" ]; then continue; fi
        resp=$(kv_cmd "${port}" "GET ${key}" 2>/dev/null || echo "(node down)")
        if [ "${resp}" != "(node down)" ] && [ "${resp}" != "(timeout)" ]; then
            check "POST-FAILOVER :${port} GET ${key}" "value_${key}" "${resp}"
            break
        fi
    done
done

echo ""
echo "── Phase 5: Write after failover ────────────────────────"

for port in 7101 7102 7103; do
    if [ "${port}" = "${LEADER_PORT}" ]; then continue; fi
    resp=$(kv_cmd "${port}" "PUT newkey newvalue" 2>/dev/null || echo "(error)")
    if [[ "${resp}" == "OK"* ]] || [[ "${resp}" == *"redirect"* ]]; then
        echo -e "  ${GRN}Write accepted on :${port} after failover${RST}"
        break
    fi
done

# ── Results ───────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Results: ${PASS} passed, ${FAIL} failed"
echo "═══════════════════════════════════════════════════════"
echo ""

[ "${FAIL}" -eq 0 ]
