#!/usr/bin/env python3
"""Owned, loopback-only, three-node native Kafka experiment; never a production launcher."""
import argparse
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request
import uuid

REPO = Path(__file__).resolve().parents[1]
LAB = REPO / ".local/kafka-lab"
MARKER = LAB / ".gigaquizz-kafka-lab"
VERSION = "4.3.1"
ARCHIVE = f"kafka_2.13-{VERSION}.tgz"
DOWNLOAD = f"https://downloads.apache.org/kafka/{VERSION}/{ARCHIVE}"
SHA512 = "c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c"
KAFKA = LAB / f"kafka_2.13-{VERSION}"
BOOTSTRAP = ",".join(f"127.0.0.1:{19091+i}" for i in range(1, 4))
HEALTH_TOPIC = "gigaquizz_lab_health"
MIN_FREE_BYTES = 8 * 1024**3
# Packed-frame experiments can retain a full ~100M-attempt journal alongside
# previous profiles. The independent 8GiB free-space reserve remains mandatory.
MAX_LAB_BYTES = 26 * 1024**3


def write_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2) + "\n")
    temp.chmod(0o600)
    temp.replace(path)


def owner(create=False):
    if LAB.is_symlink() or MARKER.is_symlink():
        raise RuntimeError("refusing symlinked lab/marker")
    if LAB.exists() and not MARKER.is_file():
        raise RuntimeError(f"refusing unowned directory {LAB}")
    if not LAB.exists():
        if not create:
            raise RuntimeError("lab not created; run up")
        LAB.mkdir(parents=True, mode=0o700)
        MARKER.write_text("gigaquizz native Kafka lab v1\n")
    if MARKER.read_text() != "gigaquizz native Kafka lab v1\n":
        raise RuntimeError("unexpected ownership marker")


def allocated_bytes(path):
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file() and not p.is_symlink())


def disk_guard():
    free = shutil.disk_usage(LAB).free
    used = allocated_bytes(LAB)
    if free < MIN_FREE_BYTES or used > MAX_LAB_BYTES:
        raise RuntimeError(f"disk guard: free={free}, lab_allocated={used}; preserve data and stop adding load")
    return {"free_bytes": free, "lab_allocated_bytes": used}


def java_home():
    candidates = [os.environ.get("GIGAQUIZZ_KAFKA_JAVA_HOME", ""),
                  "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
                  "/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home"]
    found = subprocess.run(["/usr/libexec/java_home", "-v", "21+"], capture_output=True, text=True)
    if found.returncode == 0:
        candidates.append(found.stdout.strip())
    for value in candidates:
        if value and (Path(value) / "bin/java").is_file():
            result = subprocess.run([str(Path(value) / "bin/java"), "-version"], capture_output=True, text=True, timeout=10)
            major = re.search(r'version "(\d+)', result.stderr)
            if result.returncode == 0 and major and int(major[1]) >= 21:
                return str(Path(value).resolve()), result.stderr.strip()
    raise RuntimeError("JDK 21+ unavailable; set GIGAQUIZZ_KAFKA_JAVA_HOME to an installed JDK (no global install is performed)")


def install():
    if (KAFKA / "bin/kafka-storage.sh").is_file():
        return
    dl = LAB / "downloads"
    dl.mkdir(mode=0o700, exist_ok=True)
    archive = dl / ARCHIVE
    sums = urllib.request.urlopen(DOWNLOAD + ".sha512", timeout=30).read().decode()
    expected = "".join(re.findall(r"\b[0-9a-fA-F]{8}\b", sums)).lower()
    if expected != SHA512:
        raise RuntimeError("published checksum differs from pinned SHA512")
    if not archive.is_file() or hashlib.file_digest(archive.open("rb"), "sha512").hexdigest() != SHA512:
        partial = archive.with_suffix(".partial")
        started = time.monotonic()
        with urllib.request.urlopen(DOWNLOAD, timeout=30) as response, partial.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                if output.tell() > 200 * 1024**2 or time.monotonic() - started > 900:
                    raise RuntimeError("download exceeds bounded size/time")
        if hashlib.file_digest(partial.open("rb"), "sha512").hexdigest() != SHA512:
            raise RuntimeError("archive checksum mismatch")
        partial.replace(archive)
    (dl / (ARCHIVE + ".sha512")).write_text(sums)
    write_json(dl / "verification.json", {"url": DOWNLOAD, "checksum_url": DOWNLOAD + ".sha512", "algorithm": "SHA512", "digest": SHA512, "bytes": archive.stat().st_size})
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            if not (LAB / member.name).resolve().is_relative_to(KAFKA) or member.issym() or member.islnk():
                raise RuntimeError("unexpected archive path or link")
        tar.extractall(LAB, filter="data")


def metadata():
    value = json.loads((LAB / "metadata.json").read_text())
    if value["lab_dir"] != str(LAB) or value["kafka_home"] != str(KAFKA) or value["version"] != VERSION:
        raise RuntimeError("unexpected lab metadata")
    return value


def env(meta, node=None):
    result = os.environ.copy()
    # Do not inherit arbitrary Java agents, JMX listeners, or external log paths.
    for name in ("KAFKA_OPTS", "KAFKA_JMX_OPTS", "JMX_PORT", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "KAFKA_JVM_PERFORMANCE_OPTS", "KAFKA_GC_LOG_OPTS"):
        result.pop(name, None)
    result.update(JAVA_HOME=meta["java_home"], KAFKA_HEAP_OPTS="-Xms64m -Xmx128m",
                  KAFKA_LOG4J_OPTS=f"-Dlog4j2.configurationFile={LAB / 'log4j2.properties'}",
                  LOG_DIR=str(LAB / "cli-logs"))
    if node:
        result["KAFKA_HEAP_OPTS"] = "-Xms512m -Xmx768m"
        result["LOG_DIR"] = str(LAB / f"node{node}" / "logs")
        result["KAFKA_GC_LOG_OPTS"] = f"-Xlog:gc*:file={LAB / f'node{node}' / 'logs/gc.log'}:time,tags:filecount=3,filesize=16M"
    return result


def cli(meta, script, args, timeout=20, check=True):
    result = subprocess.run([str(KAFKA / "bin" / script), *args], env=env(meta), capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{script}: {result.stderr[-3000:]} {result.stdout[-3000:]}")
    return result


def init():
    install()
    if (LAB / "metadata.json").exists():
        return metadata()
    java, version = java_home()
    new_uuid = lambda: base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
    meta = {"version": VERSION, "lab_dir": str(LAB), "kafka_home": str(KAFKA), "java_home": java,
            "java_version": version, "bootstrap_servers": BOOTSTRAP, "cluster_id": new_uuid(),
            "nodes": {str(i): {"node_id": i, "port": 19091+i, "controller_port": 19191+i, "directory_id": new_uuid()} for i in range(1, 4)},
            "replication_factor": 3, "min_insync_replicas": 2, "eligible_leader_replicas_version": 0,
            "heap": "-Xms512m -Xmx768m", "retention_ms": 86400000, "segment_bytes": 67108864}
    write_json(LAB / "metadata.json", meta)
    return meta


def setup(meta):
    # Bound diagnostic logs to four 16 MB files per process; records are never logged.
    (LAB / "log4j2.properties").write_text("""status=warn
name=GigaquizzKafkaLab
appender.file.type=RollingFile
appender.file.name=File
appender.file.fileName=${sys:kafka.logs.dir}/server.log
appender.file.filePattern=${sys:kafka.logs.dir}/server-%i.log.gz
appender.file.layout.type=PatternLayout
appender.file.layout.pattern=[%d] %-5p %c - %m%n
appender.file.policies.type=Policies
appender.file.policies.size.type=SizeBasedTriggeringPolicy
appender.file.policies.size.size=16MB
appender.file.strategy.type=DefaultRolloverStrategy
appender.file.strategy.max=3
rootLogger.level=WARN
rootLogger.appenderRef.file.ref=File
""")
    (LAB / "client.properties").write_text("request.timeout.ms=5000\ndefault.api.timeout.ms=10000\n")
    controllers = ",".join(f"127.0.0.1:{n['controller_port']}" for n in meta["nodes"].values())
    initial = ",".join(f"{n['node_id']}@127.0.0.1:{n['controller_port']}:{n['directory_id']}" for n in meta["nodes"].values())
    for key, n in meta["nodes"].items():
        node = LAB / f"node{key}"
        node.mkdir(mode=0o700, exist_ok=True)
        (node / "logs").mkdir(mode=0o700, exist_ok=True)
        props = {"process.roles": "broker,controller", "node.id": key,
                 "controller.quorum.bootstrap.servers": controllers,
                 "listeners": f"PLAINTEXT://127.0.0.1:{n['port']},CONTROLLER://127.0.0.1:{n['controller_port']}",
                 "advertised.listeners": f"PLAINTEXT://127.0.0.1:{n['port']}",
                 "listener.security.protocol.map": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
                 "controller.listener.names": "CONTROLLER", "inter.broker.listener.name": "PLAINTEXT",
                 "log.dirs": str(node / "data"), "default.replication.factor": 3,
                 "min.insync.replicas": 2, "unclean.leader.election.enable": "false",
                 "auto.create.topics.enable": "false", "num.partitions": 32,
                 "offsets.topic.replication.factor": 3, "offsets.topic.num.partitions": 16,
                 "transaction.state.log.replication.factor": 3, "transaction.state.log.min.isr": 2,
                 "transaction.state.log.num.partitions": 16,
                 "share.coordinator.state.topic.replication.factor": 3,
                 "share.coordinator.state.topic.min.isr": 2,
                 "log.segment.bytes": 67108864, "log.retention.ms": 86400000,
                 "log.retention.bytes": 1073741824, "log.cleanup.policy": "delete",
                 "log.retention.check.interval.ms": 60000, "log.preallocate": "false",
                 "log.index.size.max.bytes": 1048576,
                 "metadata.log.segment.bytes": 16777216, "metadata.max.retention.bytes": 67108864,
                 "num.network.threads": 3, "num.io.threads": 4, "num.replica.fetchers": 2,
                 "socket.request.max.bytes": 16777216, "queued.max.requests": 500,
                 "group.initial.rebalance.delay.ms": 0}
        config = node / "server.properties"
        config.write_text("\n".join(f"{k}={v}" for k, v in props.items()) + "\n")
        if not (node / "data/meta.properties").exists():
            cli(meta, "kafka-storage.sh", ["format", "-t", meta["cluster_id"], "-c", str(config),
                                         "--initial-controllers", initial, "--feature", "eligible.leader.replicas.version=0"], timeout=30)


def ps(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "lstart=", "-o", "stat=", "-o", "command="], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ""


def owned_process(n):
    if not n.get("pid"):
        return False
    line = ps(n["pid"])
    expected = str(LAB / f"node{n['node_id']}" / "server.properties")
    if not line:
        return False
    if line[24:].lstrip().startswith("Z"):
        return False
    if not line.startswith(n.get("process_started", "MISSING")) or expected not in line or "kafka.Kafka" not in line:
        raise RuntimeError(f"PID {n['pid']} no longer identifies owned Kafka node; refusing signal")
    return True


def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def start(meta, key):
    n = meta["nodes"][str(key)]
    if owned_process(n):
        return
    for port in (n["port"], n["controller_port"]):
        if port_open(port):
            raise RuntimeError(f"refusing occupied loopback port {port}")
    disk_guard()
    node = LAB / f"node{key}"
    # Java/log4j writes the bounded log above. Keep launcher stderr separately.
    stderr_path = node / "launcher.log"
    if stderr_path.exists() and stderr_path.stat().st_size > 16*1024**2:
        raise RuntimeError("launcher log exceeds bound; inspect it before starting")
    with stderr_path.open("ab") as output:
        proc = subprocess.Popen([str(KAFKA / "bin/kafka-server-start.sh"), str(node / "server.properties")],
                                env=env(meta, key), stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                                start_new_session=True)
    # Persist PID before waiting, so even a slow or failed launcher remains visible.
    n["pid"] = proc.pid
    n["process_started"] = ps(proc.pid)[:24]
    write_json(LAB / "metadata.json", meta)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"node {key} exited {proc.returncode}; inspect {stderr_path}")
        if "kafka.Kafka" in ps(proc.pid):
            return
        time.sleep(0.1)
    raise RuntimeError(f"node {key} did not exec Kafka within 10 seconds")


def stop(meta, key, hard=False):
    n = meta["nodes"][str(key)]
    if not owned_process(n):
        return
    os.kill(n["pid"], signal.SIGKILL if hard else signal.SIGTERM)
    deadline = time.monotonic() + (10 if hard else 30)
    while time.monotonic() < deadline:
        # A killed process can briefly show '(java)' before becoming a zombie.
        # No more signals are sent here, so PID reuse/exit means our target is gone.
        line = ps(n["pid"])
        gone = not line or not line.startswith(n["process_started"]) or "kafka.Kafka" not in line or line[24:].lstrip().startswith("Z")
        if gone and not port_open(n["port"]) and not port_open(n["controller_port"]):
            n.pop("pid", None)
            n.pop("process_started", None)
            write_json(LAB / "metadata.json", meta)
            return
        time.sleep(0.2)
    raise RuntimeError(f"node {key} did not exit; inspect before explicit --hard")


def topic_status(meta, create=False):
    args = ["--bootstrap-server", BOOTSTRAP, "--command-config", str(LAB / "client.properties")]
    if create:
        cli(meta, "kafka-topics.sh", [*args, "--create", "--if-not-exists", "--topic", HEALTH_TOPIC,
                                     "--partitions", "1", "--replication-factor", "3", "--config", "min.insync.replicas=2"], timeout=15)
    result = cli(meta, "kafka-topics.sh", [*args, "--describe", "--topic", HEALTH_TOPIC], timeout=15, check=False)
    match = re.search(r"\bIsr:\s*([0-9,]+)", result.stdout)
    isr = sorted(int(x) for x in match[1].split(",")) if match else []
    return {"isr": isr, "description": result.stdout.strip(), "query_ok": result.returncode == 0}


def ready(meta):
    deadline = time.monotonic() + 90
    last = ""
    while time.monotonic() < deadline:
        if not all(owned_process(n) for n in meta["nodes"].values()):
            raise RuntimeError("Kafka node exited before readiness; inspect node logs")
        if all(port_open(n["port"]) for n in meta["nodes"].values()):
            try:
                result = topic_status(meta, create=True)
                if result["isr"] == [1, 2, 3]:
                    return
                last = str(result)
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                last = str(error)
        time.sleep(1)
    raise RuntimeError(f"lab not ready within bounded startup: {last}")


def status(meta):
    nodes = [{"id": int(key), "pid": n.get("pid"), "owned_process_running": owned_process(n),
              "broker_listening": port_open(n["port"]), "controller_listening": port_open(n["controller_port"])}
             for key, n in meta["nodes"].items()]
    health = {"isr": [], "query_ok": False}
    if sum(n["broker_listening"] for n in nodes) >= 2:
        try:
            health = topic_status(meta)
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            health["error"] = str(error)
    return {"version": VERSION, "bootstrap_servers": BOOTSTRAP, "nodes": nodes,
            "ready": all(n["owned_process_running"] for n in nodes) and health["isr"] == [1, 2, 3],
            "health_topic": health, "free_bytes": shutil.disk_usage(LAB).free,
            "lab_allocated_bytes": allocated_bytes(LAB),
            "guarantee": "replicated Kafka ACK; no per-record fsync guarantee; shared host and disk"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("up", "status", "stop", "stop-node", "start-node"))
    parser.add_argument("--node", type=int, choices=(1, 2, 3))
    parser.add_argument("--hard", action="store_true", help="SIGKILL owned node(s), for crash experiments")
    args = parser.parse_args()
    if args.action in ("stop-node", "start-node") and args.node is None:
        parser.error("--node is required")
    if args.node is not None and args.action not in ("stop-node", "start-node"):
        parser.error("--node only applies to stop-node/start-node")
    if args.hard and args.action not in ("stop", "stop-node"):
        parser.error("--hard only applies to stop/stop-node")
    os.umask(0o077)
    owner(create=args.action == "up")
    with (LAB / "operation.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.action == "up":
            disk_guard()
            meta = init()
            setup(meta)
            for key in meta["nodes"]:
                start(meta, key)
            ready(meta)
        else:
            meta = metadata()
            if args.action == "start-node":
                start(meta, args.node)
                if all(owned_process(n) for n in meta["nodes"].values()):
                    ready(meta)
            elif args.action in ("stop", "stop-node"):
                for key in ([str(args.node)] if args.node else list(meta["nodes"])):
                    stop(meta, key, args.hard)
        print(json.dumps(status(meta), indent=2))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
        print(f"kafka_lab: {error}", file=sys.stderr)
        sys.exit(1)
