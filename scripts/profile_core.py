#!/usr/bin/env python3
"""Run one bounded, volatile CPU benchmark and capture aggregate process telemetry."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / ".local/core-profiles"


def utc():
    return datetime.now(timezone.utc).isoformat()


def command_output(args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=5)
    return {"exit_code": result.returncode, "stdout": result.stdout.strip()}


def cpu_seconds(value):
    days, value = value.split("-", 1) if "-" in value else ("0", value)
    total = 0.0
    for part in value.split(":"):
        total = total * 60 + float(part)
    return int(days) * 86400 + total


def sample(pid):
    r = subprocess.run(["ps", "-p", str(pid), "-o", "pid=,time=,rss="], capture_output=True, text=True, timeout=3)
    fields = r.stdout.split()
    result = {"at": utc(), "disk_free_bytes": shutil.disk_usage(ROOT).free}
    if len(fields) == 3:
        result.update(pid=int(fields[0]), cpu_seconds=cpu_seconds(fields[1]), rss_bytes=int(fields[2]) * 1024)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("bench_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9_]{1,70}", args.label):
        parser.error("label must be a short lowercase identifier")
    DEST.mkdir(mode=0o700, parents=True, exist_ok=True)
    guard = (DEST / "profile.lock").open("a")
    try:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error("another core profile is active")
    # The existing Kafka profile uses this same lock. Preserve its old files.
    if (ROOT / ".local/kafka-lab").is_dir():
        kafka_guard = (ROOT / ".local/kafka-lab/profile.lock").open("a")
        try:
            fcntl.flock(kafka_guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("another Kafka profile is active")
    if shutil.disk_usage(ROOT).free < 8 * 1024**3:
        parser.error("less than 8GiB free; existing data is preserved")
    paths = {suffix: DEST / (args.label + suffix) for suffix in (".json", "_telemetry.json", ".stderr")}
    if any(p.exists() for p in paths.values()):
        parser.error("profile label already exists")
    bench_args = args.bench_args[1:] if args.bench_args[:1] == ["--"] else args.bench_args
    binary = ROOT / "bin/corebench"
    env = os.environ.copy()
    env["GOMEMLIMIT"] = "10GiB"
    telemetry = {
        "command": ["bin/corebench", *bench_args], "started_at": utc(),
        "binary_sha256": hashlib.file_digest(binary.open("rb"), "sha256").hexdigest(),
        "environment": {"platform": platform.platform(), "go": command_output(["go", "version"]),
                        "hardware": command_output(["sysctl", "machdep.cpu.brand_string", "hw.logicalcpu", "hw.memsize"]),
                        **{key: env.get(key, "default") for key in ("GOGC", "GOMEMLIMIT", "GOMAXPROCS")}},
        "memory_before": command_output(["memory_pressure", "-Q"]),
        "swap_before": command_output(["sysctl", "vm.swapusage"]),
        "samples": [], "error": None,
        "scope": "Generator and volatile core in one Go process; no HTTP, Kafka, disk or durability on admission path.",
    }
    kafka_meta = ROOT / ".local/kafka-lab/metadata.json"
    if kafka_meta.exists():
        telemetry["owned_kafka_nodes_at_start"] = {
            key: {"pid": node.get("pid")} for key, node in json.loads(kafka_meta.read_text())["nodes"].items()
        }
    started = time.monotonic()
    with paths[".json"].open("x") as output, paths[".stderr"].open("x") as error:
        paths[".json"].chmod(0o600)
        paths[".stderr"].chmod(0o600)
        process = subprocess.Popen([str(binary), *bench_args], cwd=ROOT, stdout=output, stderr=error, env=env)
        try:
            while process.poll() is None:
                if time.monotonic() - started > 300:
                    raise RuntimeError("300-second watchdog exceeded; incomplete profile")
                point = sample(process.pid)
                telemetry["samples"].append(point)
                if point["disk_free_bytes"] < 8 * 1024**3:
                    raise RuntimeError("free disk crossed 8GiB reserve; incomplete profile")
                time.sleep(1)
        except (Exception, KeyboardInterrupt) as exc:
            telemetry["error"] = str(exc) or type(exc).__name__
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            telemetry.update(exit_code=process.returncode, finished_at=utc(),
                             memory_after=command_output(["memory_pressure", "-Q"]),
                             swap_after=command_output(["sysctl", "vm.swapusage"]))
            with paths["_telemetry.json"].open("x") as f:
                paths["_telemetry.json"].chmod(0o600)
                json.dump(telemetry, f, indent=2)
                f.write("\n")
    try:
        report = json.loads(paths[".json"].read_text())
    except json.JSONDecodeError:
        report = {"errors": ["no complete JSON report; inspect stderr and telemetry"]}
    print(json.dumps({"report": str(paths[".json"].relative_to(ROOT)), "exit_code": process.returncode,
                      "harness_error": telemetry["error"], "errors": report.get("errors"),
                      "counts": report.get("workload", {}).get("counts"),
                      "reconciliation": report.get("reconciliation")}, indent=2))
    return 1 if telemetry["error"] or report.get("errors") else process.returncode


if __name__ == "__main__":
    raise SystemExit(main())
