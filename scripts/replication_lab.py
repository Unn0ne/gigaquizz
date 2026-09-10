#!/usr/bin/env python3
"""Own and control an isolated, Unix-socket-only PostgreSQL replication lab.

This is a local test fixture, not a production failover controller. It never
reinitializes an existing PGDATA or starts a retired primary automatically.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import time
from urllib.parse import urlencode


PROJECT = Path(__file__).resolve().parents[1]
ROOT = PROJECT / ".local" / "replication-lab"
NODES = {"primary": 55440, "standby_a": 55441, "standby_b": 55442}
REPLICA_NAMES = ["gigaquizz_ha_a", "gigaquizz_ha_b"]
SYNC_NAMES = "ANY 1 (gigaquizz_ha_a,gigaquizz_ha_b)"
FORMAT = 1


class LabError(Exception):
    pass


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def run(args: list[str], *, stdin: str | None = None, timeout: float = 15,
        check: bool = True) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    # All endpoints and identities are explicit. Do not inherit a different
    # database service, injected session settings or a caller's password file.
    for key in list(env):
        if key.startswith("PG"):
            env.pop(key)
    env.update({"LC_ALL": "C", "PGCONNECT_TIMEOUT": "3"})
    try:
        result = subprocess.run(args, input=stdin, capture_output=True, text=True,
                                timeout=timeout, env=env, check=False)
    except subprocess.TimeoutExpired as exc:
        raise LabError(f"{Path(args[0]).name} exceeded {timeout:g}s; inspect lab status before retrying") from exc
    if check and result.returncode:
        detail = (result.stderr or result.stdout).strip()[-2000:]
        raise LabError(f"{Path(args[0]).name} failed ({result.returncode}): {detail}")
    return result


def no_symlink(path: Path) -> None:
    if path.is_symlink():
        raise LabError(f"refusing symlink in managed path: {path}")


def private_dir(path: Path) -> None:
    no_symlink(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.stat().st_uid != os.getuid():
        raise LabError(f"managed directory is not owned by this user: {path}")
    path.chmod(0o700)


def write_file(path: Path, content: str) -> None:
    no_symlink(path)
    temporary = path.with_name(path.name + ".tmp." + secrets.token_hex(4))
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def write_json(path: Path, value: dict) -> None:
    write_file(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def read_json(path: Path) -> dict:
    no_symlink(path)
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise LabError(f"cannot read valid ownership/state file: {path}") from exc


def discover_bin(preferred: str | None = None) -> tuple[Path, int]:
    override = os.environ.get("GIGAQUIZZ_PG_BIN") or os.environ.get("PG_BIN")
    candidates = []
    if override:
        candidates.append(Path(override))
    elif preferred:
        candidates.append(Path(preferred))
    else:
        located = shutil.which("pg_ctl")
        if located:
            candidates.append(Path(located).resolve().parent)
        candidates += [Path("/Applications/Postgres.app/Contents/Versions/16/bin"),
                       Path("/Applications/Postgres.app/Contents/Versions/latest/bin")]
    for candidate in candidates:
        required = ("initdb", "pg_ctl", "pg_basebackup", "psql", "pg_controldata")
        if not all((candidate / name).is_file() and os.access(candidate / name, os.X_OK) for name in required):
            continue
        result = run([str(candidate / "pg_ctl"), "--version"], timeout=5)
        match = re.search(r"PostgreSQL\) (\d+)", result.stdout)
        if match and int(match.group(1)) >= 16:
            return candidate.resolve(), int(match.group(1))
    raise LabError("PostgreSQL 16+ native tools not found; set GIGAQUIZZ_PG_BIN to their bin directory")


def endpoint(node: str, database: str = "gigaquizz", application: str | None = None) -> str:
    options = {"host": str(ROOT / "socket"), "port": str(NODES[node]), "sslmode": "disable"}
    if application:
        options["application_name"] = application
    return "postgresql://gigaquizz@/" + database + "?" + urlencode(options)


def literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def lsn_value(value: str | None) -> int:
    if not value:
        return 0
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


class Lab:
    def __init__(self, create: bool = False):
        no_symlink(PROJECT / ".local")
        no_symlink(ROOT)
        marker = ROOT / "ownership.json"
        if not marker.exists():
            if not create:
                raise LabError("lab is not initialized; run replication_lab.py up")
            if ROOT.exists() and any(path.name != "control.lock" for path in ROOT.iterdir()):
                raise LabError("lab directory is nonempty without an ownership marker; nothing was changed")
            private_dir(ROOT)
            pg_bin, major = discover_bin()
            owner = {"format": FORMAT, "lab_id": secrets.token_hex(16),
                     "root": str(ROOT), "project": str(PROJECT), "uid": os.getuid()}
            write_json(marker, owner)
            self.state = {**owner, "pg_bin": str(pg_bin), "pg_major": major,
                          "created_at": now(), "initialized": False,
                          "current_primary": "primary", "epoch": 1, "retired_nodes": [],
                          "nodes": {name: {"port": port, "data_directory": str(ROOT / name),
                                           "url": endpoint(name), "initialized": False,
                                           "application_name": {"standby_a": REPLICA_NAMES[0],
                                                                "standby_b": REPLICA_NAMES[1]}.get(name)}
                                    for name, port in NODES.items()}}
            self.save()
        else:
            owner = read_json(marker)
            expected = (FORMAT, str(ROOT), str(PROJECT), os.getuid())
            actual = tuple(owner.get(key) for key in ("format", "root", "project", "uid"))
            if actual != expected:
                raise LabError("ownership marker does not belong to this project/user")
            self.state = read_json(ROOT / "metadata.json")
            if self.state.get("lab_id") != owner.get("lab_id"):
                raise LabError("metadata and ownership marker disagree")
            pg_bin, major = discover_bin(self.state.get("pg_bin"))
            if major != self.state.get("pg_major"):
                raise LabError("selected PostgreSQL major version differs from the lab's data files")
            self.state["pg_bin"] = str(pg_bin)
        self.pg_bin = Path(self.state["pg_bin"])
        private_dir(ROOT)
        private_dir(ROOT / "socket")
        for name in NODES:
            no_symlink(ROOT / name)

    def save(self) -> None:
        leader = self.state["current_primary"]
        self.state["primary_url"] = endpoint(leader)
        self.state["database_url"] = endpoint(leader)
        self.state["durability_policy"] = {
            "synchronous_commit": "on", "synchronous_standby_names": SYNC_NAMES,
            "minimum_remote_flush": 1, "expected_standby_names": REPLICA_NAMES,
            "physical_replication_slots": True, "fsync": True, "full_page_writes": True,
            "failure_domain": "one local computer; no zone/region isolation",
        }
        for info in self.state["nodes"].values():
            info["slot"] = info.get("application_name")
        self.state["updated_at"] = now()
        write_json(ROOT / "metadata.json", self.state)

    def tool(self, name: str) -> str:
        return str(self.pg_bin / name)

    def sql(self, node: str, query: str, *, database: str = "postgres", timeout: float = 8) -> str:
        arguments = [self.tool("psql"), "-X", "-A", "-t", "-q", "-w", "-v", "ON_ERROR_STOP=1",
                     "-h", str(ROOT / "socket"), "-p", str(NODES[node]), "-U", "gigaquizz", "-d", database]
        return run(arguments, stdin="SET statement_timeout='5s'; SET lock_timeout='2s';\n" + query,
                   timeout=timeout).stdout.strip()

    def owned_node(self, node: str) -> Path:
        directory = ROOT / node
        no_symlink(directory)
        marker = read_json(directory / ".gigaquizz-node.json")
        if marker != {"lab_id": self.state["lab_id"], "node": node}:
            raise LabError(f"{node}: node ownership marker does not match")
        if (directory / "PG_VERSION").read_text().strip() != str(self.state["pg_major"]):
            raise LabError(f"{node}: incompatible PG_VERSION")
        return directory

    def mark_node(self, node: str) -> None:
        write_json(ROOT / node / ".gigaquizz-node.json", {"lab_id": self.state["lab_id"], "node": node})
        self.state["nodes"][node]["initialized"] = True
        self.save()

    def running(self, node: str) -> bool:
        if not self.state["nodes"][node]["initialized"]:
            return False
        directory = self.owned_node(node)
        result = run([self.tool("pg_ctl"), "-D", str(directory), "status"], check=False, timeout=5)
        if result.returncode == 3:
            return False
        if result.returncode != 0:
            raise LabError(f"{node}: cannot determine pg_ctl status")
        lines = (directory / "postmaster.pid").read_text().splitlines()
        if len(lines) < 2 or Path(lines[1]).resolve() != directory:
            raise LabError(f"{node}: postmaster PID file points outside its managed directory")
        command = run(["ps", "-p", lines[0], "-o", "command="], timeout=5, check=False).stdout
        if "postgres" not in command or str(directory) not in command:
            raise LabError(f"{node}: refusing to control a PID whose command does not identify this PGDATA")
        return True

    def configure(self, node: str) -> None:
        directory = self.owned_node(node)
        leader = self.state["current_primary"]
        enabled = self.state["initialized"]
        settings = {
            "listen_addresses": "", "port": NODES[node],
            "unix_socket_directories": str(ROOT / "socket"), "unix_socket_permissions": "0700",
            "max_connections": 200, "shared_buffers": "128MB", "work_mem": "4MB",
            "maintenance_work_mem": "64MB", "max_wal_senders": 10, "max_replication_slots": 4,
            "wal_level": "replica", "hot_standby": "on", "fsync": "on", "full_page_writes": "on",
            "synchronous_commit": "on", "synchronous_standby_names": SYNC_NAMES if enabled else "",
            "wal_keep_size": "256MB", "max_slot_wal_keep_size": "512MB",
            "max_wal_size": "1GB", "min_wal_size": "80MB", "checkpoint_timeout": "5min",
            "wal_receiver_timeout": "5s", "wal_sender_timeout": "5s", "wal_retrieve_retry_interval": "1s",
            "logging_collector": "off", "log_destination": "stderr", "log_min_messages": "fatal",
            "log_statement": "none", "log_min_error_statement": "panic", "log_duration": "off",
            "log_min_duration_statement": -1, "log_min_duration_sample": -1,
            "log_statement_sample_rate": 0, "log_parameter_max_length": 0,
            "log_parameter_max_length_on_error": 0, "log_connections": "off",
            "log_disconnections": "off", "log_replication_commands": "off",
            "log_autovacuum_min_duration": -1, "log_checkpoints": "off", "log_line_prefix": "%m [%p] ",
        }
        content = "# Managed only by scripts/replication_lab.py; no user data in logs.\n"
        content += "\n".join(f"{key} = {literal(str(value))}" for key, value in settings.items()) + "\n"
        write_file(directory / "postgresql.conf", content)
        write_file(directory / "pg_hba.conf", "local all gigaquizz trust\nlocal replication gigaquizz trust\n")
        recovery = "# Managed recovery settings; no passwords.\n"
        if node != leader:
            name = self.state["nodes"][node]["application_name"]
            if name not in REPLICA_NAMES:
                raise LabError(f"{node}: no permitted replication identity assigned")
            recovery += "primary_conninfo = " + literal(endpoint(leader, "postgres", name)) + "\n"
            recovery += "primary_slot_name = " + literal(name) + "\nrecovery_target_timeline = 'latest'\n"
            write_file(directory / "standby.signal", "")
        elif (directory / "standby.signal").exists():
            raise LabError(f"{node}: expected primary still has standby.signal; promotion must finish first")
        write_file(directory / "postgresql.auto.conf", recovery)

    def initialize_primary(self) -> None:
        node = "primary"
        directory = ROOT / node
        if directory.exists() and any(directory.iterdir()):
            raise LabError("unmarked/partial primary directory exists; no automatic reset is permitted")
        progress("Initializing isolated primary (synchronous remote wait disabled only during bootstrap)")
        run([self.tool("initdb"), "-D", str(directory), "-U", "gigaquizz", "--encoding=UTF8",
             "--locale=C", "--auth-local=trust", "--auth-host=reject", "--no-instructions"], timeout=45)
        self.mark_node(node)

    def slot_exists(self, leader: str, name: str) -> bool:
        value = self.sql(leader, "SELECT slot_type FROM pg_replication_slots WHERE slot_name=" + literal(name) + ";")
        if value and value != "physical":
            raise LabError(f"slot {name} exists but is not physical")
        return bool(value)

    def ensure_slot(self, leader: str, name: str) -> None:
        if not self.slot_exists(leader, name):
            self.sql(leader, "SELECT slot_name FROM pg_create_physical_replication_slot(" + literal(name) + ", true);")

    def basebackup(self, node: str) -> None:
        leader = self.state["current_primary"]
        directory = ROOT / node
        if directory.exists() and any(directory.iterdir()):
            raise LabError(f"{node}: refusing base backup over a nonempty directory")
        name = self.state["nodes"][node]["application_name"]
        arguments = [self.tool("pg_basebackup"), "-D", str(directory), "-d", endpoint(leader, "postgres", name),
                     "-X", "stream", "-R", "-S", name, "-c", "fast", "--no-password"]
        if not self.slot_exists(leader, name):
            arguments.append("-C")
        progress(f"Creating {node} from {leader} with physical slot {name}")
        run(arguments, timeout=45)
        self.mark_node(node)

    def start(self, node: str, *, preserve_config: bool = False) -> None:
        if node in self.state["retired_nodes"]:
            raise LabError(f"{node} is a retired branch; use explicit rebuild-node, never start old primary data")
        if self.running(node):
            return
        self.owned_node(node)
        if not preserve_config:
            self.configure(node)
        progress(f"Starting {node} on Unix socket port {NODES[node]}")
        log = ROOT / f"{node}.log"
        if not log.exists():
            write_file(log, "")
        no_symlink(log)
        run([self.tool("pg_ctl"), "-D", str(ROOT / node), "-l", str(log), "-w", "-t", "15", "start"], timeout=20)

    def stop(self, node: str, mode: str = "fast") -> None:
        if self.running(node):
            progress(f"Stopping {node} ({mode})")
            run([self.tool("pg_ctl"), "-D", str(ROOT / node), "-m", mode, "-w", "-t", "15", "stop"], timeout=20)

    def reload(self, node: str) -> None:
        run([self.tool("pg_ctl"), "-D", str(ROOT / node), "reload"], timeout=5)

    def node_snapshot(self, node: str) -> dict:
        result = {"running": self.running(node), "retired": node in self.state["retired_nodes"],
                  "url": endpoint(node), "application_name": self.state["nodes"][node]["application_name"]}
        if not result["running"]:
            return result
        query = """SELECT json_build_object(
            'in_recovery', pg_is_in_recovery(), 'server_version', current_setting('server_version'),
            'system_identifier', (pg_control_system()).system_identifier::text,
            'timeline', CASE WHEN pg_is_in_recovery() THEN (pg_control_checkpoint()).timeline_id
                ELSE ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::bigint END,
            'receive_lsn', pg_last_wal_receive_lsn()::text,
            'replay_lsn', pg_last_wal_replay_lsn()::text,
            'flush_lsn', CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_flush_lsn() END::text,
            'synchronous_commit', current_setting('synchronous_commit'),
            'synchronous_standby_names', current_setting('synchronous_standby_names'),
            'fsync', current_setting('fsync'), 'full_page_writes', current_setting('full_page_writes'),
            'listen_addresses', current_setting('listen_addresses'), 'data_directory', current_setting('data_directory'));
        """
        try:
            result.update(json.loads(self.sql(node, query)))
        except LabError:
            result["sql_ready"] = False
            return result
        result["sql_ready"] = True
        if Path(result["data_directory"]).resolve() != ROOT / node:
            raise LabError(f"{node}: SQL endpoint identifies a different data directory")
        return result

    def replicas(self, leader: str) -> list[dict]:
        query = """SELECT coalesce(json_agg(json_build_object(
            'application_name', r.application_name, 'state', r.state, 'sync_state', r.sync_state,
            'flush_lsn', r.flush_lsn::text, 'replay_lsn', r.replay_lsn::text,
            'slot_name', s.slot_name, 'slot_type', s.slot_type, 'slot_active', s.active)), '[]'::json)
            FROM pg_stat_replication r LEFT JOIN pg_replication_slots s ON s.active_pid = r.pid;"""
        return json.loads(self.sql(leader, query))

    def status(self) -> dict:
        result = {"lab_root": str(ROOT), "metadata_path": str(ROOT / "metadata.json"),
                  "current_primary": self.state["current_primary"], "primary_url": self.state["primary_url"],
                  "epoch": self.state["epoch"], "bootstrap_complete": self.state["initialized"],
                  "pending_promotion": self.state.get("pending_promotion"),
                  "durability_policy": self.state["durability_policy"], "nodes": {}}
        for node in NODES:
            result["nodes"][node] = self.node_snapshot(node)
        leader = result["nodes"][self.state["current_primary"]]
        result["replicas"] = []
        result["ready"] = False
        if leader.get("sql_ready") and not leader["in_recovery"]:
            result["replicas"] = self.replicas(self.state["current_primary"])
            healthy = {item["application_name"] for item in result["replicas"]
                       if item["application_name"] in REPLICA_NAMES and item["state"] == "streaming"
                       and item["sync_state"] == "quorum" and item["slot_type"] == "physical"
                       and item["slot_active"] and item["slot_name"] == item["application_name"]
                       and item["flush_lsn"]}
            verified_nodes = {self.state["nodes"][name]["application_name"]
                              for name, snapshot in result["nodes"].items()
                              if not snapshot["retired"] and snapshot.get("sql_ready")
                              and snapshot.get("in_recovery") and snapshot.get("fsync") == "on"
                              and snapshot.get("full_page_writes") == "on"
                              and snapshot.get("system_identifier") == leader.get("system_identifier")
                              and not snapshot.get("listen_addresses")}
            healthy &= verified_nodes
            result["ready"] = bool(self.state["initialized"] and not self.state.get("pending_promotion")
                                   and leader["synchronous_standby_names"] == SYNC_NAMES
                                   and leader["synchronous_commit"] == "on" and leader["fsync"] == "on"
                                   and leader["full_page_writes"] == "on" and not leader["listen_addresses"]
                                   and len(healthy) >= 1)
            result["eligible_remote_replicas"] = len(healthy)
        return result

    def wait_streaming(self, expected: int, *, require_quorum: bool, timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        leader = self.state["current_primary"]
        while time.monotonic() < deadline:
            replicas = self.replicas(leader)
            names = {item["application_name"] for item in replicas
                     if item["application_name"] in REPLICA_NAMES and item["state"] == "streaming"
                     and item["slot_type"] == "physical" and item["slot_active"]
                     and item["slot_name"] == item["application_name"] and item["flush_lsn"]
                     and (not require_quorum or item["sync_state"] == "quorum")}
            if len(names) >= expected:
                return
            time.sleep(0.2)
        raise LabError(f"expected {expected} streaming physical standbys did not become ready within {timeout:g}s")

    def verify_write(self) -> None:
        leader = self.state["current_primary"]
        self.sql(leader, "INSERT INTO replication_lab.probe (id, value) VALUES (1, 'ready') "
                 "ON CONFLICT (id) DO UPDATE SET value = excluded.value;", database="gigaquizz")
        current = self.node_snapshot(leader)
        self.state["current_primary_system_identifier"] = current["system_identifier"]
        self.state["current_primary_timeline"] = current["timeline"]
        self.state["last_verified_write_at"] = now()
        self.save()

    def up(self) -> None:
        if self.state.get("pending_promotion"):
            self.promote(self.state["pending_promotion"]["new_primary"], emit=False)
        if not self.state["initialized"] and not self.state["nodes"]["primary"]["initialized"]:
            self.initialize_primary()
        leader = self.state["current_primary"]
        self.configure(leader)
        self.start(leader)
        self.reload(leader)
        if not self.state["initialized"]:
            exists = self.sql(leader, "SELECT 1 FROM pg_database WHERE datname='gigaquizz';")
            if exists != "1":
                self.sql(leader, "CREATE DATABASE gigaquizz;")
            self.sql(leader, "CREATE SCHEMA IF NOT EXISTS replication_lab; "
                     "CREATE TABLE IF NOT EXISTS replication_lab.probe (id integer PRIMARY KEY, value text NOT NULL);",
                     database="gigaquizz")
        followers = [node for node in NODES if node != leader and node not in self.state["retired_nodes"]]
        for node in followers:
            if not self.state["nodes"][node]["initialized"]:
                self.basebackup(node)
            else:
                self.ensure_slot(leader, self.state["nodes"][node]["application_name"])
            self.configure(node)
            self.start(node)
            self.reload(node)
        if not followers:
            raise LabError("no eligible standby remains; rebuild a retired node before declaring ready")
        self.wait_streaming(len(followers), require_quorum=self.state["initialized"])
        if not self.state["initialized"]:
            self.state["initialized"] = True
            self.save()  # Never silently disable remote waiting on a later up.
            for node in [leader] + followers:
                self.configure(node)
                self.reload(node)
            self.wait_streaming(len(followers), require_quorum=True)
        self.verify_write()
        result = self.status()
        if not result["ready"]:
            raise LabError("lab started but replication policy is not ready")
        print(json.dumps(result, indent=2))

    def promote(self, candidate: str, *, emit: bool = True) -> None:
        pending = self.state.get("pending_promotion")
        old = self.state["current_primary"]
        if pending:
            if pending["new_primary"] != candidate:
                raise LabError("another promotion is pending; resume its named candidate")
            old = pending["old_primary"]
        elif candidate == old or candidate in self.state["retired_nodes"]:
            raise LabError("candidate must be a non-retired standby")
        if self.running(old):
            raise LabError("stop the current primary before promotion; the lab will not fence a running primary implicitly")
        if pending and not self.running(candidate):
            # A crash may happen before or after pg_ctl actually promoted this
            # node. Its existing standby.signal distinguishes those states.
            # Reconfiguring from still-old metadata would incorrectly put an
            # already promoted branch back into standby mode.
            self.start(candidate, preserve_config=True)
        target = self.node_snapshot(candidate)
        if not target.get("sql_ready"):
            raise LabError("promotion candidate must be running and SQL-ready")
        if pending and pending.get("candidate_system_identifier") not in (None, target["system_identifier"]):
            raise LabError("pending promotion candidate belongs to a different database system")
        required_lsn = lsn_value(pending.get("candidate_flush_lsn")) if pending else 0
        deadline = time.monotonic() + 15
        while max(lsn_value(target.get("flush_lsn")), lsn_value(target.get("replay_lsn"))) < required_lsn:
            if time.monotonic() >= deadline:
                raise LabError("restarted candidate did not recover the durable WAL recorded by promotion intent")
            time.sleep(0.2)
            target = self.node_snapshot(candidate)
            if not target.get("sql_ready"):
                raise LabError("promotion candidate stopped while recovering its recorded WAL")
        for node in NODES:
            if node in (old, candidate) or node in self.state["retired_nodes"]:
                continue
            if pending and not self.running(node):
                self.owned_node(node)
                if not (ROOT / node / "standby.signal").exists():
                    raise LabError(f"{node} is unexpectedly configured as another primary; refusing to start it")
                self.start(node, preserve_config=True)
            other = self.node_snapshot(node)
            if not other.get("sql_ready"):
                raise LabError("all non-retired standbys must be readable to compare received WAL before promotion")
            if not other.get("in_recovery"):
                raise LabError(f"{node} is unexpectedly another primary; promotion is refused")
            if other["system_identifier"] != target["system_identifier"]:
                raise LabError("standbys belong to different database systems")
            # After a stopped standby starts without its old upstream, its
            # receiver LSN may be NULL while replay has recovered durable WAL
            # from its local files. The old primary is fenced above.
            target_lsn = max(lsn_value(target.get("flush_lsn")), lsn_value(target.get("replay_lsn")))
            other_lsn = max(lsn_value(other.get("flush_lsn")), lsn_value(other.get("replay_lsn")))
            if other_lsn > target_lsn:
                raise LabError(f"{candidate} has less available WAL than {node}; choose the more advanced standby")
        if not pending:
            if not target["in_recovery"]:
                raise LabError("candidate is not a standby")
            self.state["pending_promotion"] = {"old_primary": old, "new_primary": candidate, "started_at": now(),
                                               "candidate_system_identifier": target["system_identifier"],
                                               "candidate_flush_lsn": target.get("flush_lsn") or target.get("replay_lsn")}
            self.state["retired_nodes"].append(old)
            self.save()  # Fence the old branch in persistent control state first.
        if target["in_recovery"]:
            progress(f"Promoting {candidate}; {old} is persistently retired")
            run([self.tool("pg_ctl"), "-D", str(ROOT / candidate), "-w", "-t", "15", "promote"], timeout=20)
        self.state["nodes"][old]["application_name"] = self.state["nodes"][candidate]["application_name"]
        self.state["nodes"][candidate]["application_name"] = None
        self.state["current_primary"] = candidate
        self.state["epoch"] += 1
        self.state.pop("pending_promotion", None)
        self.save()
        self.configure(candidate)
        self.reload(candidate)
        followers = [node for node in NODES if node != candidate and node not in self.state["retired_nodes"]]
        for node in followers:
            self.ensure_slot(candidate, self.state["nodes"][node]["application_name"])
            self.configure(node)
            if self.running(node):
                self.reload(node)
            else:
                self.start(node)
        if followers:
            self.wait_streaming(len(followers), require_quorum=True)
            self.verify_write()
        if emit:
            print(json.dumps(self.status(), indent=2))

    def rebuild(self, node: str) -> None:
        if self.state.get("pending_promotion"):
            raise LabError("finish pending promotion before rebuilding")
        leader = self.state["current_primary"]
        if node == leader:
            raise LabError("cannot rebuild the current primary")
        if self.running(node):
            raise LabError("stop the node before its explicit rebuild")
        if not self.node_snapshot(leader).get("sql_ready"):
            raise LabError("current primary must be running for base backup")
        name = self.state["nodes"][node]["application_name"]
        if name not in REPLICA_NAMES:
            raise LabError("no permitted standby identity available for this node")
        self.owned_node(node)
        archive = ROOT / "retired-data"
        private_dir(archive)
        target = archive / f"{node}-epoch{self.state['epoch']}-{secrets.token_hex(4)}"
        os.rename(ROOT / node, target)
        self.state["nodes"][node]["initialized"] = False
        self.state["nodes"][node]["archived_data"] = str(target)
        self.save()
        if self.slot_exists(leader, name):
            active = self.sql(leader, "SELECT active FROM pg_replication_slots WHERE slot_name=" + literal(name) + ";")
            if active != "f":
                raise LabError("replication slot is still active; no slot was dropped")
            self.sql(leader, "SELECT pg_drop_replication_slot(" + literal(name) + ");")
        self.basebackup(node)
        if node in self.state["retired_nodes"]:
            self.state["retired_nodes"].remove(node)
        self.save()
        self.configure(node)
        self.start(node)
        expected = sum(n != leader and n not in self.state["retired_nodes"] for n in NODES)
        self.wait_streaming(expected, require_quorum=True)
        self.verify_write()
        print(json.dumps(self.status(), indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("up", help="bootstrap once, then start/reconcile owned nodes without resetting data")
    sub.add_parser("status", help="emit live state as JSON")
    stop = sub.add_parser("stop", help="stop only this lab's owned nodes")
    stop.add_argument("--mode", choices=("fast", "immediate"), default="fast")
    for command in ("start-node", "stop-node", "promote", "rebuild-node"):
        child = sub.add_parser(command)
        child.add_argument("node", choices=tuple(NODES))
        if command == "stop-node":
            child.add_argument("--mode", choices=("fast", "immediate"), default="fast")
    args = parser.parse_args()
    os.umask(0o077)
    try:
        no_symlink(PROJECT / ".local")
        no_symlink(ROOT)
        if not ROOT.exists():
            if args.command != "up":
                raise LabError("lab is not initialized; run replication_lab.py up")
            private_dir(ROOT)
        elif not (ROOT / "ownership.json").exists() and any(path.name != "control.lock" for path in ROOT.iterdir()):
            raise LabError("lab directory is nonempty without an ownership marker; nothing was changed")
        no_symlink(ROOT / "control.lock")
        with (ROOT / "control.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise LabError("another lab control command is running") from exc
            lab = Lab(create=args.command == "up")
            if args.command == "up":
                lab.up()
            elif args.command == "status":
                print(json.dumps(lab.status(), indent=2))
            elif args.command == "stop":
                leader = lab.state["current_primary"]
                for node in [leader] + [n for n in NODES if n != leader]:
                    lab.stop(node, args.mode)
                print(json.dumps(lab.status(), indent=2))
            elif args.command == "promote":
                lab.promote(args.node)
            elif args.command == "rebuild-node":
                lab.rebuild(args.node)
            else:
                if lab.state.get("pending_promotion"):
                    raise LabError("resume the pending promotion before individual node operations")
                if args.command == "start-node":
                    lab.start(args.node)
                    leader = lab.state["current_primary"]
                    if lab.running(leader):
                        expected = sum(node != leader and node not in lab.state["retired_nodes"]
                                       and lab.running(node) for node in NODES)
                        if expected:
                            lab.wait_streaming(expected, require_quorum=lab.state["initialized"])
                else:
                    lab.stop(args.node, args.mode)
                print(json.dumps(lab.status(), indent=2))
        return 0
    except (LabError, OSError) as exc:
        print(f"replication lab: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
