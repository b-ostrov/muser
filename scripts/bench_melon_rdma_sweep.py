#!/usr/bin/env python3
"""muser disaggregated context sweep, one transport per run.

Reproduces the column set of docs/benchmarks/melon-rdma-vs-tcp-context-sweep.csv
so a new run drops straight into the same comparison. MUSER_TRANSPORT is a
per-process switch on the receiver, so each transport gets its own server.

Token fixtures are drawn deterministically from the alphabet of the node's own
smoke fixture: real vocabulary ids, identical bytes for both transports, so the
only difference between the two arms is the wire.
"""
from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--transport", choices=("tcp", "rdma"), required=True)
    p.add_argument("--depths", required=True, help="comma-separated prompt depths")
    p.add_argument("--reps", type=int, default=5)
    p.add_argument("--warmups", type=int, default=1)
    p.add_argument("--decode-tokens", type=int, default=32)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--server-binary", required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--cluster-config", required=True)
    p.add_argument("--alphabet-fixture", required=True, type=Path)
    p.add_argument("--port", type=int, default=28771)
    p.add_argument("--request-timeout", type=int, default=1800)
    p.add_argument("--server-log", type=Path, required=True)
    p.add_argument("--unique-per-rep", action="store_true",
                   help="give every repetition its own fixture, so every request is a "
                        "cold handoff instead of a producer-side prefix reuse hit")
    return p.parse_args()


def fixture_for(depth: int, alphabet: list[int], variant: int = 0) -> list[int]:
    # Seeded per (depth, variant), so both transports see byte-identical
    # prompts and a rerun of one arm reproduces itself. A distinct variant is
    # a prompt the producer has never prefilled, which is the only way to get
    # a second cold handoff at the same depth.
    rng = random.Random(0xB0B0 + depth + 1_000_003 * variant)
    return [alphabet[0]] + [rng.choice(alphabet[1:]) for _ in range(depth - 1)]


def wait_for_health(port: int, deadline_s: int, proc: subprocess.Popen) -> None:
    started = time.monotonic()
    while time.monotonic() - started < deadline_s:
        if proc.poll() is not None:
            raise SystemExit(f"server exited early with code {proc.returncode}")
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/health")
            if conn.getresponse().status == 200:
                conn.close()
                return
            conn.close()
        except OSError:
            pass
        time.sleep(2)
    raise SystemExit("server never became healthy")


def served_model_id(port: int) -> str:
    """The served id is whatever the loaded model registered itself as; a
    guessed name comes back as a 404 model_not_found after the load has
    already cost a couple of minutes."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("GET", "/v1/models")
    payload = json.loads(conn.getresponse().read())
    conn.close()
    models = payload.get("data") or []
    if not models:
        raise SystemExit("/v1/models is empty; the server loaded no model")
    return models[0]["id"]


def one_request(port: int, model: str, tokens: list[int], decode_tokens: int,
                timeout: int) -> dict:
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": "context-sweep"}],
        "max_tokens": decode_tokens,
        "temperature": 0.0,
        "stream": False,
        "muser_prompt_token_ids": tokens,
    }, separators=(",", ":")).encode()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    started = time.perf_counter()
    conn.request("POST", "/v1/chat/completions", body,
                 {"Content-Type": "application/json"})
    response = conn.getresponse()
    raw = response.read()
    wall = time.perf_counter() - started
    conn.close()
    if response.status != 200:
        raise SystemExit(f"HTTP {response.status}: {raw[:400]!r}")
    payload = json.loads(raw)
    timings = payload.get("timings") or {}
    usage = payload.get("usage") or {}
    return {
        "wall_s": wall,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "prefill_s": timings.get("prompt_ms", 0.0) / 1000.0,
        "prefill_tok_s": timings.get("prompt_per_second", 0.0),
        "decode_s": timings.get("predicted_ms", 0.0) / 1000.0,
        "decode_tok_s": timings.get("predicted_per_second", 0.0),
    }


def main() -> int:
    args = parse_args()
    depths = [int(d) for d in args.depths.split(",")]
    alphabet = sorted({int(line) for line in
                       args.alphabet_fixture.read_text().splitlines() if line.strip()})
    if len(alphabet) < 2:
        raise SystemExit("alphabet fixture needs at least two distinct token ids")

    env = os.environ.copy()
    env["MUSER_TRANSPORT"] = args.transport
    if args.transport == "rdma":
        env.setdefault("MUSER_RDMA_DEV", "mlx5_0")
        # -1 asks the pipe to take whichever provider slot this process owns.
        env.setdefault("MUSER_RDMA_GID", "-1")

    command = [
        args.server_binary, "serve", "--host", "127.0.0.1", "--port", str(args.port),
        "--model", args.model, "--backend", "metal", "--prefix-cache", "off",
        # The model's own ceiling is 131072; asking for more is refused outright.
        "--max-context", str(min(131072, max(depths) + args.decode_tokens + 64)),
        "--prefill", "remote", "--cluster-config", args.cluster_config,
    ]
    log = args.server_log.open("w")
    print(f"[{args.transport}] starting server: {' '.join(command)}", flush=True)
    proc = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
    rows = []
    try:
        wait_for_health(args.port, 900, proc)
        model_id = served_model_id(args.port)
        print(f"[{args.transport}] server healthy, serving {model_id!r}", flush=True)
        for depth in depths:
            shared = fixture_for(depth, alphabet)
            for index in range(args.warmups + args.reps):
                warmup = index < args.warmups
                tokens = fixture_for(depth, alphabet, index + 1) if args.unique_per_rep else shared
                result = one_request(args.port, model_id, tokens,
                                     args.decode_tokens, args.request_timeout)
                row = {
                    "transport": args.transport,
                    "context_requested": depth,
                    "prompt_tokens": result["prompt_tokens"],
                    "decode_tokens": result["completion_tokens"],
                    "prefill_s": result["prefill_s"],
                    "prefill_tok_s": result["prefill_tok_s"],
                    "decode_s": result["decode_s"],
                    "decode_tok_s": result["decode_tok_s"],
                    "ttft_s": result["prefill_s"],
                    "wall_s": result["wall_s"],
                    "warmup": warmup,
                    "rep_index": -1 if warmup else index - args.warmups,
                }
                rows.append(row)
                print(f"[{args.transport}] depth={depth} "
                      f"{'warmup' if warmup else 'rep ' + str(row['rep_index'])} "
                      f"ttft={row['ttft_s']:.3f}s wall={row['wall_s']:.3f}s",
                      flush=True)
                # Flush after every request so a long sweep is never lost.
                with args.out.open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                    writer.writeheader()
                    writer.writerows(rows)
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
