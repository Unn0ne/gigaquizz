#!/usr/bin/env python3
"""Bounded local journal profile with aggregate process samples and optional broker crash."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / ".local/kafka-lab"


def utc():
    return datetime.now(timezone.utc).isoformat()


def cpu_seconds(value):
    days, value = value.split("-", 1) if "-" in value else ("0", value)
    result = 0.0
    for part in value.split(":"):
        result = result * 60 + float(part)
    return int(days) * 86400 + result


def sample(bench_pid):
    meta = json.loads((LAB / "metadata.json").read_text())
    pids = {bench_pid: "go_benchmark_and_service"}
    for key, node in meta["nodes"].items():
        if node.get("pid"):
            pids[node["pid"]] = "kafka_node_" + key
    output = subprocess.run(["ps", "-axo", "pid,time,rss"], capture_output=True, text=True, timeout=3, check=True).stdout
    processes = {}
    for line in output.splitlines()[1:]:
        fields = line.split()
        if len(fields) == 3 and int(fields[0]) in pids:
            processes[pids[int(fields[0])]] = {"pid": int(fields[0]), "cpu_seconds": cpu_seconds(fields[1]), "rss_bytes": int(fields[2]) * 1024}
    return {"at": utc(), "processes": processes, "free_disk_bytes": shutil.disk_usage(LAB).free}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--benchmark", choices=("logbench", "framebench"), default="logbench", help="per-attempt legacy journal or packed frames")
    parser.add_argument("--cpu-profile", action="store_true", help="sample CPU of the combined Go generator/service during workload, excluding audit")
    parser.add_argument("--fault-node", type=int, choices=(1, 2, 3), nargs="+", help="one or two owned brokers to SIGKILL")
    parser.add_argument("--fault-after", type=float, default=40)
    parser.add_argument("--fault-duration", type=float, default=10)
    parser.add_argument("bench_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.cpu_profile and args.benchmark != "logbench":
        parser.error("CPU pprof is currently supported only by logbench")
    if not re.fullmatch(r"[a-z0-9_]{1,70}", args.label):
        parser.error("label must be a short lowercase identifier")
    if not 20 <= args.fault_after <= 65 or not 1 <= args.fault_duration <= 20:
        parser.error("fault timing exceeds local bounds")
    if args.fault_node and (len(args.fault_node) > 2 or len(set(args.fault_node)) != len(args.fault_node)):
        parser.error("choose one or two distinct owned brokers")
    if (LAB / ".gigaquizz-kafka-lab").read_text() != "gigaquizz native Kafka lab v1\n":
        parser.error("owned lab marker required")
    profile_guard = (LAB / "profile.lock").open("a")
    try:
        fcntl.flock(profile_guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error("another owned log profile is active")
    if shutil.disk_usage(LAB).free < 8 * 1024**3:
        parser.error("less than 8GiB free; preserve existing data")
    meta = json.loads((LAB / "metadata.json").read_text())
    if any(not n.get("pid") for n in meta["nodes"].values()):
        parser.error("start all owned brokers before profiling")
    destination = LAB / "profiles"
    destination.mkdir(mode=0o700, exist_ok=True)
    paths = {suffix: destination / (args.label + suffix) for suffix in (".json", "_telemetry.json", ".stderr")}
    if any(p.exists() for p in paths.values()):
        parser.error("profile label already exists")
    bench_args = args.bench_args[1:] if args.bench_args[:1] == ["--"] else args.bench_args
    command = [str(ROOT / "bin" / args.benchmark), *bench_args]
    child_env = os.environ.copy()
    child_env.pop("LOGBENCH_CPU_PROFILE", None)
    if args.cpu_profile:
        profile_path = destination / (args.label + ".cpu.pprof")
        if profile_path.exists():
            parser.error("CPU profile already exists")
        child_env["LOGBENCH_CPU_PROFILE"] = str(profile_path.relative_to(ROOT))
    telemetry = {"command": ["bin/" + args.benchmark, *bench_args], "started_at": utc(), "binary_sha256": hashlib.file_digest(Path(command[0]).open("rb"), "sha256").hexdigest(), "environment": {"kafka_version": meta["version"], "java_version": meta["java_version"], "same_host": True, "cpu_profile_enabled": args.cpu_profile, "GOGC": child_env.get("GOGC", "default"), "GOMEMLIMIT": child_env.get("GOMEMLIMIT", "default"), "GOMAXPROCS": child_env.get("GOMAXPROCS", "default")}, "samples": [], "faults": []}
    started = time.monotonic()
    stopped = False
    injected = False
    stopped_at = 0.0

    def node_action(action):
        for node in args.fault_node:
            call = [sys.executable, str(ROOT / "scripts/kafka_lab.py"), action, "--node", str(node)]
            if action == "stop-node":
                call.append("--hard")
            event = {"action": action, "node": node, "started_at": utc()}
            result = subprocess.run(call, capture_output=True, text=True, timeout=60)
            event.update(finished_at=utc(), exit_code=result.returncode)
            telemetry["faults"].append(event)
            if result.returncode:
                raise RuntimeError(f"owned broker {action} failed: {result.stderr[-1500:]}")

    with paths[".json"].open("x") as output, paths[".stderr"].open("x") as error:
        output_path = paths[".json"]
        output_path.chmod(0o600)
        paths[".stderr"].chmod(0o600)
        process = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=error, env=child_env)
        try:
            while process.poll() is None:
                elapsed = time.monotonic() - started
                if elapsed > 270:
                    raise RuntimeError("bounded benchmark deadline exceeded")
                point = sample(process.pid)
                telemetry["samples"].append(point)
                if point["free_disk_bytes"] < 8 * 1024**3:
                    raise RuntimeError("free disk crossed 8GiB reserve; profile is incomplete")
                if args.fault_node and not injected and elapsed >= args.fault_after:
                    stopped = True  # restoration also attempted if CLI exits after signaling
                    node_action("stop-node")
                    injected = True
                    stopped_at = time.monotonic()
                if stopped and time.monotonic() - stopped_at >= args.fault_duration:
                    node_action("start-node")
                    stopped = False
                time.sleep(1)
            telemetry["exit_code"] = process.returncode
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            if stopped:
                node_action("start-node")
            telemetry["finished_at"] = utc()
            paths["_telemetry.json"].write_text(json.dumps(telemetry, indent=2) + "\n")
            paths["_telemetry.json"].chmod(0o600)
    report = json.loads(paths[".json"].read_text())
    print(json.dumps({"report": str(paths[".json"].relative_to(ROOT)), "errors": report.get("errors"), "workload": report.get("workload", {}).get("counts"), "reconciliation_correct": report.get("reconciliation", {}).get("correct")}, indent=2))
    return process.returncode


if __name__ == "__main__":
    raise SystemExit(main())
