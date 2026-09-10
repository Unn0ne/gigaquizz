#!/usr/bin/env python3
"""Profile one bounded filebench child; preserve journals and previous reports.

Example: GOMEMLIMIT=6GiB python3 scripts/profile_file.py --label files_1700k -- -rate 1700000 -allow-large
"""

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / ".local/file-profiles"
RESERVE_BYTES = 8 * 1024**3
DIRECTORY_LIMIT = 1024**3
REPORT_LIMIT = 64 * 1024**2
STDERR_LIMIT = 16 * 1024**2
TELEMETRY_LIMIT = 4 * 1024**2
PROFILE_RESERVATION = REPORT_LIMIT + STDERR_LIMIT + TELEMETRY_LIMIT
WATCHDOG_SECONDS = 300


def utc():
    return datetime.now(timezone.utc).isoformat()


def command_output(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=5)
        return {"exit_code": result.returncode, "stdout": result.stdout.strip()[:4096]}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"error": str(error)}


def cpu_seconds(value):
    days, value = value.split("-", 1) if "-" in value else ("0", value)
    total = 0.0
    for field in value.split(":"):
        total = total * 60 + float(field)
    return int(days) * 86400 + total


def retained_bytes():
    total, count = 0, 0
    for directory, dirs, files in os.walk(DEST, followlinks=False):
        for name in [*dirs, *files]:
            path = Path(directory) / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise RuntimeError("profile directory contains a symlink; existing files preserved")
            count += 1
            if count > 4096:
                raise RuntimeError("profile directory exceeds the bounded 4096-entry inventory")
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total


def acquire_lock(stack, path):
    if path.is_symlink():
        raise RuntimeError(f"refusing symlinked profile lock: {path.relative_to(ROOT)}")
    lock = stack.enter_context(path.open("a"))
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError(f"another profile holds {path.relative_to(ROOT)}") from None


def sample(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "pid=,time=,rss="],
                            capture_output=True, text=True, timeout=3)
    fields = result.stdout.split()
    point = {"at": utc(), "free_disk_bytes": shutil.disk_usage(ROOT).free,
             "profile_artifact_bytes": retained_bytes()}
    if len(fields) == 3:
        if int(fields[0]) != pid:
            raise RuntimeError("unexpected process in telemetry sample")
        point["processes"] = {"filebench": {"pid": pid, "cpu_seconds": cpu_seconds(fields[1]),
                                           "rss_bytes": int(fields[2]) * 1024}}
    elif result.returncode not in (0, 1):
        raise RuntimeError("process telemetry command failed")
    else:
        point["processes"] = {}
    return point


def write_all(destination, data):
    remaining = memoryview(data)
    while remaining:
        written = destination.write(remaining)
        if not written:
            raise OSError("captured output write made no progress")
        remaining = remaining[written:]


def capture(source, destination, limit, name, errors, failed):
    """Bound only captured output, never the child's journal file sizes."""
    size = 0
    try:
        with source:
            while chunk := source.read(64 * 1024):
                allowed = min(len(chunk), limit - size)
                if allowed:
                    write_all(destination, chunk[:allowed])
                    size += allowed
                if allowed != len(chunk):
                    errors.append(f"{name} exceeds its {limit}-byte capture bound; profile incomplete")
                    failed.set()
                    return
    except Exception as error:
        errors.append(f"{name} capture failed: {type(error).__name__}: {error}")
        failed.set()
    finally:
        destination.flush()


def stop_child(process, telemetry):
    if process.poll() is not None:
        return
    telemetry["termination"] = {"sigterm_at": utc()}
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        telemetry["termination"]["sigkill_at"] = utc()
        process.kill()
        process.wait(timeout=5)


def environment(child_env):
    info = {"platform": platform.platform(), "system": platform.system(),
            "machine": platform.machine(), "logical_cpus": os.cpu_count(),
            "python": platform.python_version(), "go": command_output(["go", "version"]),
            **{key: child_env.get(key, "default") for key in ("GOGC", "GOMEMLIMIT", "GOMAXPROCS")}}
    if sys.platform == "darwin":
        info["hardware"] = command_output(["sysctl", "machdep.cpu.brand_string", "hw.logicalcpu", "hw.memsize"])
    else:
        try:
            info["physical_memory_bytes"] = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        except (ValueError, OSError):
            pass
    return info


def run(args, parser):
    binary = ROOT / "bin/filebench"
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error("build the executable bin/filebench before profiling")
    if DEST.is_symlink():
        parser.error("refusing symlinked file-profiles directory")
    DEST.mkdir(mode=0o700, parents=True, exist_ok=True)
    paths = {suffix: DEST / (args.label + suffix) for suffix in (".json", ".stderr", "_telemetry.json")}
    bench_args = args.bench_args[1:] if args.bench_args[:1] == ["--"] else args.bench_args
    if len(bench_args) > 64 or any(len(value) > 4096 for value in bench_args):
        parser.error("benchmark argument list exceeds local bounds")

    with ExitStack() as stack:
        acquire_lock(stack, DEST / "profile.lock")
        # Cooperate with previous wrappers; no broker/controller command is run.
        for common in (ROOT / ".local/kafka-lab/profile.lock", ROOT / ".local/core-profiles/profile.lock"):
            if common.exists() or common.is_symlink():
                acquire_lock(stack, common)
        if any(path.exists() or path.is_symlink() for path in paths.values()):
            parser.error("profile label already exists; previous artifacts are preserved")
        if retained_bytes() + PROFILE_RESERVATION > DIRECTORY_LIMIT:
            parser.error("profiles would exceed the 1GiB aggregate bound; existing files preserved")
        free_before = shutil.disk_usage(ROOT).free
        if free_before < RESERVE_BYTES + PROFILE_RESERVATION:
            parser.error("need 8GiB free reserve plus bounded report space before starting")
        with binary.open("rb") as executable:
            binary_sha256 = hashlib.file_digest(executable, "sha256").hexdigest()
        child_env = os.environ.copy()
        child_env.setdefault("GOMEMLIMIT", "6GiB")
        telemetry = {
            "command": ["bin/filebench", *bench_args], "started_at": utc(),
            "binary_sha256": binary_sha256, "environment": environment(child_env),
            "free_disk_bytes_before": free_before, "samples": [], "errors": [],
            "limits": {"watchdog_seconds": WATCHDOG_SECONDS, "sample_interval_seconds": 1,
                       "free_disk_reserve_bytes": RESERVE_BYTES, "aggregate_profiles_bytes": DIRECTORY_LIMIT,
                       "report_bytes": REPORT_LIMIT, "stderr_bytes": STDERR_LIMIT},
            "scope": "One local Go filebench process includes generator, file journal and audit; no HTTP or replicated-cluster performance is inferred.",
            "sampling_limits": "One-second observations are a watchdog, not a filesystem quota. CPU is cumulative process time; sampled RSS may miss shorter peaks. The wrapper never reads or copies private ledgers.",
        }
        output = stack.enter_context(paths[".json"].open("xb", buffering=0))
        stderr = stack.enter_context(paths[".stderr"].open("xb", buffering=0))
        # Reserve every artifact before the process starts; never overwrite an old label.
        meta_output = stack.enter_context(paths["_telemetry.json"].open("xb", buffering=0))
        interrupted, capture_failed = threading.Event(), threading.Event()
        received_signal = []

        def interrupt(signum, _frame):
            received_signal.append(signal.Signals(signum).name)
            interrupted.set()

        old_signals = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGINT, signal.SIGTERM)}
        process, threads = None, []
        started = time.monotonic()
        try:
            process = subprocess.Popen([str(binary), *bench_args], cwd=ROOT, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=child_env,
                                       start_new_session=True, bufsize=0)
            telemetry["child_pid"] = process.pid
            for source, destination, bound, name in ((process.stdout, output, REPORT_LIMIT, "stdout"),
                                                     (process.stderr, stderr, STDERR_LIMIT, "stderr")):
                thread = threading.Thread(target=capture, args=(source, destination, bound, name,
                                                               telemetry["errors"], capture_failed), daemon=True)
                thread.start()
                threads.append(thread)
            next_sample = started
            while process.poll() is None:
                if interrupted.is_set():
                    raise RuntimeError("harness interrupted by " + ", ".join(received_signal))
                if capture_failed.is_set():
                    raise RuntimeError("bounded output capture failed")
                if time.monotonic() - started >= WATCHDOG_SECONDS:
                    raise RuntimeError("300-second watchdog exceeded; profile incomplete")
                point = sample(process.pid)
                telemetry["samples"].append(point)
                if point["free_disk_bytes"] < RESERVE_BYTES:
                    raise RuntimeError("free disk crossed the 8GiB reserve; profile incomplete")
                if point["profile_artifact_bytes"] > DIRECTORY_LIMIT:
                    raise RuntimeError("aggregate profile files exceed 1GiB; profile incomplete")
                next_sample += 1
                interrupted.wait(max(0, min(1, next_sample - time.monotonic())))
        except (Exception, KeyboardInterrupt) as error:
            telemetry["errors"].append(str(error) or type(error).__name__)
        finally:
            if process is not None:
                try:
                    stop_child(process, telemetry)
                except (OSError, subprocess.TimeoutExpired) as error:
                    telemetry["errors"].append(f"owned child termination failed: {error}")
                for thread in threads:
                    thread.join(timeout=3)
                    if thread.is_alive():
                        telemetry["errors"].append("output capture did not finish within its bounded join")
            for sig, handler in old_signals.items():
                signal.signal(sig, handler)
            telemetry.update(exit_code=process.returncode if process is not None else None,
                             finished_at=utc(), elapsed_seconds=time.monotonic() - started,
                             free_disk_bytes_after=shutil.disk_usage(ROOT).free)
            if telemetry["free_disk_bytes_after"] < RESERVE_BYTES:
                telemetry["errors"].append("final free disk is below the 8GiB reserve")

        try:
            report = json.loads(paths[".json"].read_text())
            if not isinstance(report, dict):
                raise ValueError("expected a JSON object")
        except (OSError, UnicodeError, ValueError) as error:
            report = {}
            telemetry["errors"].append(f"no complete JSON report: {error}")
        telemetry["report_complete"] = bool(report)
        encoded = (json.dumps(telemetry, indent=2) + "\n").encode()
        if len(encoded) > TELEMETRY_LIMIT:
            telemetry["samples"] = []
            telemetry["errors"].append("telemetry exceeds its capture bound; samples omitted")
            encoded = (json.dumps(telemetry, indent=2) + "\n").encode()
        write_all(meta_output, encoded)
        workload = report.get("workload")
        print(json.dumps({"report": str(paths[".json"].relative_to(ROOT)), "exit_code": telemetry["exit_code"],
                          "harness_errors": telemetry["errors"], "errors": report.get("errors"),
                          "counts": workload.get("counts") if isinstance(workload, dict) else None,
                          "reconciliation": report.get("reconciliation", report.get("audit"))}, indent=2))
        return 1 if telemetry["errors"] or report.get("errors") or telemetry["exit_code"] != 0 else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("bench_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9_]{1,70}", args.label):
        parser.error("label must be 1..70 lowercase letters, digits or underscores")
    os.umask(0o077)
    try:
        return run(args, parser)
    except (OSError, RuntimeError) as error:
        print(f"profile_file: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
