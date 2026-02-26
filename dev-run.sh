#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# dev-run.sh  —  One-command 3-node APEX-KV cluster on localhost
#
#   ./dev-run.sh          # build + start 3 nodes + tail logs
#   ./dev-run.sh clean    # kill all nodes + remove WAL files
#   ./dev-run.sh bench    # run benchmark after starting cluster
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

BINARY="${APEX_BINARY:-./build/kv}"
WAL_DIR="${WAL_DIR:-/tmp/apex-kv}"
LOG_DIR="${LOG_DIR:-/tmp/apex-kv/logs}"

RED="\033[31m" GRN="\033[32m" YLW="\033[33m" CYN="\033[36m" RST="\033[0m"

banner() { echo -e "\n${CYN}══ $* ══${RST}\n"; }
info()   { echo -e "${GRN}▶${RST} $*"; }
warn()   { echo -e "${YLW}⚠${RST}  $*"; }
die()    { echo -e "${RED}✗${RST} $*" >&2; exit 1; }

# ── Commands ──────────────────────────────────────────────────────────────────
cmd_clean() {
    info "Killing any running apex-kv nodes..."
    pkill -f "kv --id" 2>/dev/null || true
    rm -rf "${WAL_DIR}"
    info "Done."
}

cmd_build() {
    banner "Building APEX-KV"
    if [ ! -f CMakeLists.txt ]; then
        die "Run from the apex-kv repository root"
    fi
    cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-O3 -march=native" -S . -q
    cmake --build build -j"$(nproc)" --config Release
    info "Binary: $(du -sh build/kv | cut -f1)  build/kv"
    BINARY="./build/kv"
}

cmd_start() {
    banner "Starting 3-node APEX-KV cluster"

    if [ ! -x "${BINARY}" ]; then
        warn "Binary not found, building first..."
        cmd_build
    fi

    mkdir -p "${WAL_DIR}" "${LOG_DIR}"

    # Node 1
    info "Starting node 1 on :7001"
    "${BINARY}" \
        --id 1 --port 7001 \
        --wal-dir "${WAL_DIR}/node1" \
        --peers "127.0.0.1:7002:2,127.0.0.1:7003:3" \
        > "${LOG_DIR}/node1.log" 2>&1 &
    echo $! > "${WAL_DIR}/node1.pid"

    # Node 2
    info "Starting node 2 on :7002"
    "${BINARY}" \
        --id 2 --port 7002 \
        --wal-dir "${WAL_DIR}/node2" \
        --peers "127.0.0.1:7001:1,127.0.0.1:7003:3" \
        > "${LOG_DIR}/node2.log" 2>&1 &
    echo $! > "${WAL_DIR}/node2.pid"

    # Node 3
    info "Starting node 3 on :7003"
    "${BINARY}" \
        --id 3 --port 7003 \
        --wal-dir "${WAL_DIR}/node3" \
        --peers "127.0.0.1:7001:1,127.0.0.1:7002:2" \
        > "${LOG_DIR}/node3.log" 2>&1 &
    echo $! > "${WAL_DIR}/node3.pid"

    echo ""
    info "Waiting for cluster to elect a leader..."
    sleep 1.5

    # Health check
    for port in 7001 7002 7003; do
        if "${BINARY}" --client "127.0.0.1:${port}" <<< "PING" 2>/dev/null | grep -q "PONG"; then
            info "Node on :${port} → ${GRN}healthy${RST}"
        else
            warn "Node on :${port} may still be starting up"
        fi
    done

    echo ""
    echo -e "  ${GRN}Cluster ready!${RST}"
    echo ""
    echo -e "  Interactive client:  ${CYN}${BINARY} --client 127.0.0.1:7001${RST}"
    echo -e "  Benchmark:          ${CYN}${BINARY} --bench 127.0.0.1:7001 --threads 8 --ops 100000${RST}"
    echo -e "  Unit tests:         ${CYN}${BINARY} --test${RST}"
    echo -e "  Logs:               ${CYN}tail -f ${LOG_DIR}/node*.log${RST}"
    echo -e "  Stop:               ${CYN}./dev-run.sh clean${RST}"
    echo ""
}

cmd_bench() {
    banner "Running Benchmark"
    cmd_start
    sleep 0.5
    info "Benchmarking with 8 threads, 100k ops..."
    "${BINARY}" --bench 127.0.0.1:7001 --threads 8 --ops 100000
    python3 scripts/bench-graph.py --results /tmp/apex_bench.json 2>/dev/null || true
}

cmd_logs() {
    tail -f "${LOG_DIR}"/node*.log 2>/dev/null | \
        python3 -c "
import sys, json
for line in sys.stdin:
    try:
        d = json.loads(line.strip())
        lvl = d.get('level','?').upper()
        color = '\033[32m' if lvl=='INFO' else '\033[33m' if lvl=='WARN' else '\033[31m' if lvl=='ERROR' else '\033[90m'
        print(f\"{color}[n{d.get('node','?')}][{lvl}]\033[0m {d.get('msg','')}\")
    except:
        print(line, end='')
" 2>/dev/null || tail -f "${LOG_DIR}"/node*.log
}

# ── Entry point ───────────────────────────────────────────────────────────────
banner "APEX-KV  dev-run.sh"

case "${1:-start}" in
    clean)  cmd_clean ;;
    build)  cmd_build ;;
    start)  cmd_start ;;
    bench)  cmd_bench ;;
    logs)   cmd_logs  ;;
    *)
        echo "Usage: $0 {start|clean|build|bench|logs}"
        echo ""
        echo "  start  — build (if needed) + launch 3-node cluster (default)"
        echo "  clean  — kill all nodes and remove WAL directories"
        echo "  build  — compile binary only"
        echo "  bench  — start cluster + run benchmark"
        echo "  logs   — pretty-print structured logs"
        exit 1
        ;;
esac
