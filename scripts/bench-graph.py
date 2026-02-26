#!/usr/bin/env python3
"""
scripts/bench-graph.py  —  Parse APEX-KV benchmark output and render ASCII graphs.

Usage:
    ./kv --bench 127.0.0.1:7001 --threads 8 --ops 100000 | python3 scripts/bench-graph.py

Or with saved results:
    ./kv --bench 127.0.0.1:7001 > /tmp/bench.txt 2>&1
    python3 scripts/bench-graph.py --file /tmp/bench.txt
"""
import sys, re, argparse, json
from typing import Optional

def parse_bench_output(text: str) -> dict:
    r = {}
    m = re.search(r'Throughput:\s+([\d,]+)\s+ops/sec', text)
    if m: r['throughput'] = int(m.group(1).replace(',', ''))
    for pct in ['p50', 'p95', 'p99', 'p99.9']:
        label = pct.replace('.', '_')
        m = re.search(rf'Latency {pct}:\s+(\d+)\s+µs', text, re.IGNORECASE)
        if m: r[label] = int(m.group(1))
    return r

def bar(value: int, max_val: int, width: int = 40, char: str = '█') -> str:
    if max_val == 0: return ''
    filled = int(width * value / max_val)
    return char * filled + '░' * (width - filled)

def render_latency_chart(results: dict):
    labels = [('p50', 'p50  '), ('p95', 'p95  '), ('p99', 'p99  '), ('p99_9', 'p99.9')]
    values = [(l, results[k]) for k, l in labels if k in results]
    if not values: return
    max_v = max(v for _, v in values)

    print('\n  Latency Distribution (µs)')
    print('  ' + '─' * 52)
    for label, v in values:
        print(f'  {label}  {bar(v, max_v, 36)}  {v:>6} µs')
    print()

def render_throughput(throughput: Optional[int]):
    if throughput is None: return
    tiers = [
        (1_000_000, "★★★ World-class (>1M ops/s)"),
        (  500_000, "★★  Excellent (>500k ops/s)"),
        (  100_000, "★   Good (>100k ops/s)"),
        (        0, "    Needs tuning (<100k ops/s)"),
    ]
    tier = next(label for thresh, label in tiers if throughput >= thresh)
    bar_w = min(50, throughput // 20000)
    print(f'\n  Throughput: {throughput:>10,} ops/sec')
    print(f'  {"█" * bar_w}')
    print(f'  {tier}\n')

def compare_results(baseline: dict, current: dict):
    """Show delta table vs baseline."""
    print('\n  Comparison vs Baseline')
    print('  ' + '─' * 42)
    for key in ['throughput', 'p50', 'p95', 'p99', 'p99_9']:
        b = baseline.get(key); c = current.get(key)
        if b is None or c is None: continue
        pct = (c - b) / b * 100
        arrow = '▲' if pct > 0 else '▼'
        color_on  = '\033[31m' if (key == 'throughput' and pct < 0) or (key != 'throughput' and pct > 0) else '\033[32m'
        color_off = '\033[0m'
        label = key.replace('_', '.')
        print(f'  {label:<12} {b:>8,}  →  {c:>8,}  {color_on}{arrow}{abs(pct):5.1f}%{color_off}')
    print()

def export_json(results: dict, path: str):
    with open(path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f'  Results saved: {path}')

def main():
    p = argparse.ArgumentParser(description='APEX-KV benchmark visualiser')
    p.add_argument('--file',     help='Read from file instead of stdin')
    p.add_argument('--baseline', help='JSON file from a previous run for comparison')
    p.add_argument('--results',  help='Save current results as JSON')
    args = p.parse_args()

    if args.file:
        text = open(args.file).read()
    else:
        text = sys.stdin.read()

    results = parse_bench_output(text)
    if not results:
        print('  No benchmark data found. Run: ./kv --bench <host:port>', file=sys.stderr)
        sys.exit(1)

    print('\n╔══════════════════════════════════════════════╗')
    print('║  APEX-KV  Benchmark Results                 ║')
    print('╚══════════════════════════════════════════════╝')

    render_throughput(results.get('throughput'))
    render_latency_chart(results)

    if args.baseline:
        try:
            baseline = json.load(open(args.baseline))
            compare_results(baseline, results)
        except Exception as e:
            print(f'  Warning: could not load baseline: {e}')

    if args.results:
        export_json(results, args.results)

if __name__ == '__main__':
    main()
