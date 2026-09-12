#!/usr/bin/env python3
"""Aggregate file stat for one private HTTP poll's three local Kafka replicas.

This never connects to a broker, reads segment contents, mutates a file, or
enumerates a broker's data directory. Run after an independent CLOSED audit.
The result is filesystem accounting, not vote payload or an ACK durability proof.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
from urllib.parse import urlsplit


MARKER = b"gigaquizz native Kafka lab v1\n"
TOPIC = re.compile(r"gqlog_app_([0-9a-f]{32})(?:_[0-9a-f]{32})?\Z")
CATEGORIES = (".log", ".index", ".timeindex", ".txnindex", "other")


class InspectionError(Exception):
    """Messages contain no paths, poll IDs, topic names or file contents."""


def check(condition, message):
    if not condition:
        raise InspectionError(message)


def absolute(path):
    # Resolve neither symlinks nor shell expressions. Each directory component
    # is subsequently opened with O_NOFOLLOW, starting at the filesystem root.
    return Path(os.path.abspath(os.path.expanduser(str(path))))


@contextmanager
def directory(path):
    check(hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY"),
          "platform lacks required no-follow directory support")
    path = absolute(path)
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in path.parts[1:]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor
    finally:
        os.close(descriptor)


def read_small(path, limit, private=False):
    path = absolute(path)
    with directory(path.parent) as parent:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=parent)
        with os.fdopen(descriptor, "rb") as source:
            info = os.fstat(source.fileno())
            check(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid(),
                  "configuration/ownership metadata must be a regular owned file")
            check(info.st_size <= limit and info.st_nlink == 1,
                  "configuration/ownership metadata size or link bound failed")
            if private:
                check(stat.S_IMODE(info.st_mode) == 0o600,
                      "private journal configuration must have mode 0600")
            data = source.read(limit + 1)
            check(len(data) <= limit, "configuration/ownership metadata exceeds read bound")
            return data


def decode_json(data):
    try:
        value = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        raise InspectionError("configuration/ownership JSON is invalid") from None
    check(isinstance(value, dict), "configuration/ownership JSON must be an object")
    return value


def loopback_brokers(values):
    check(isinstance(values, list) and len(values) == 3,
          "expected exactly three configured loopback brokers")
    addresses = set()
    for value in values:
        check(isinstance(value, str) and not any(c.isspace() for c in value),
              "invalid broker address")
        try:
            parsed = urlsplit("//" + value)
            ip = ipaddress.ip_address(parsed.hostname)
            valid = (ip.is_loopback and parsed.port is not None and
                     parsed.username is None and parsed.password is None and
                     not parsed.path and not parsed.query and not parsed.fragment)
        except (ValueError, TypeError):
            raise InspectionError("broker must be a numeric loopback address") from None
        check(valid and 1 <= parsed.port <= 65535, "invalid loopback broker address")
        addresses.add((str(ip), parsed.port))
    check(len(addresses) == 3, "configured broker addresses must be distinct")
    return addresses


def poll_window(config):
    try:
        starts = datetime.fromisoformat(config["StartsAt"].replace("Z", "+00:00"))
        ends = datetime.fromisoformat(config["EndsAt"].replace("Z", "+00:00"))
    except (KeyError, ValueError, TypeError, AttributeError):
        raise InspectionError("invalid journal admission window") from None
    check(starts.tzinfo is not None and ends.tzinfo is not None and
          (ends - starts).total_seconds() == 60,
          "journal must describe one timezone-aware 60-second poll")
    check(ends <= datetime.now(timezone.utc), "poll admission window has not ended")


def validate(runtime_root, config_path):
    lab = absolute(runtime_root) / ".local/kafka-lab"
    check(read_small(lab / ".gigaquizz-kafka-lab", 128) == MARKER,
          "unexpected Kafka lab ownership marker")
    metadata = decode_json(read_small(lab / "metadata.json", 65536))
    check(metadata.get("lab_dir") == str(lab) and metadata.get("replication_factor") == 3,
          "Kafka lab metadata does not match the selected runtime or three replicas")
    nodes = metadata.get("nodes")
    check(isinstance(nodes, dict) and set(nodes) == {"1", "2", "3"},
          "Kafka lab must contain exactly three known nodes")
    expected = set()
    for node in range(1, 4):
        entry = nodes[str(node)]
        check(isinstance(entry, dict) and entry.get("node_id") == node and
              type(entry.get("port")) is int and 1 <= entry["port"] <= 65535,
              "invalid owned Kafka node metadata")
        expected.add(("127.0.0.1", entry["port"]))
    bootstrap = metadata.get("bootstrap_servers")
    check(isinstance(bootstrap, str), "missing Kafka lab bootstrap metadata")
    check(loopback_brokers(bootstrap.split(",")) == expected,
          "Kafka bootstrap metadata differs from owned node ports")
    config = decode_json(read_small(config_path, 65536, private=True))
    check(config.get("AllowRemoteBrokers", False) is False and
          loopback_brokers(config.get("Brokers")) == expected,
          "journal brokers do not match the owned loopback lab")
    topic = config.get("Topic")
    match = TOPIC.fullmatch(topic) if isinstance(topic, str) else None
    poll_id = config.get("PollID")
    check(match is not None and isinstance(poll_id, list) and len(poll_id) == 16 and
          all(type(value) is int and 0 <= value <= 255 for value in poll_id) and
          any(poll_id) and bytes(poll_id).hex() == match[1],
          "journal topic is not owned by its complete HTTP poll ID")
    partitions = config.get("Partitions")
    check(type(partitions) is int and 1 <= partitions <= 32,
          "HTTP journal partition count exceeds the bound")
    poll_window(config)
    return lab, topic, partitions


def inspect(runtime_root, config_path, max_files=20000, timeout=10):
    check(type(max_files) is int and 1 <= max_files <= 100000,
          "file limit must be between 1 and 100000")
    check(isinstance(timeout, (int, float)) and 0 < timeout <= 30,
          "inspection deadline must be between 0 and 30 seconds")
    started = time.monotonic()
    snapshot_at = datetime.now(timezone.utc).isoformat()
    lab, topic, partitions = validate(runtime_root, config_path)
    totals = {name: {"files": 0, "logical_bytes": 0, "allocated_bytes": 0}
              for name in CATEGORIES}
    files = 0
    directory_bytes = 0
    for node in range(1, 4):
        for partition in range(partitions):
            check(time.monotonic() - started < timeout, "inspection deadline exceeded")
            # Construct only the exact selected topic directory. Never list the
            # broker data directory, which also contains unrelated topics.
            selected = lab / f"node{node}" / "data" / f"{topic}-{partition}"
            with directory(selected) as descriptor:
                folder = os.fstat(descriptor)
                check(folder.st_uid == os.getuid() and hasattr(folder, "st_blocks"),
                      "selected topic directory is not owned or lacks block accounting")
                directory_bytes += folder.st_blocks * 512
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        check(time.monotonic() - started < timeout, "inspection deadline exceeded")
                        check(files < max_files, "selected topic file count exceeds the bound")
                        info = entry.stat(follow_symlinks=False)
                        check(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and
                              info.st_nlink == 1 and hasattr(info, "st_blocks"),
                              "selected topic contains a non-regular, linked or unowned file")
                        suffix = Path(entry.name).suffix
                        category = suffix if suffix in CATEGORIES else "other"
                        totals[category]["files"] += 1
                        totals[category]["logical_bytes"] += info.st_size
                        totals[category]["allocated_bytes"] += info.st_blocks * 512
                        files += 1
    return {
        "mode": "one-http-poll-kafka-replica-file-stat",
        "snapshot_started_at": snapshot_at,
        "wall_seconds": time.monotonic() - started,
        "partitions": partitions,
        "replicas_per_partition": 3,
        "replica_directories_present": partitions * 3,
        "files": files,
        "logical_file_bytes": sum(value["logical_bytes"] for value in totals.values()),
        "allocated_file_bytes": sum(value["allocated_bytes"] for value in totals.values()),
        "allocated_directory_bytes": directory_bytes,
        "by_extension": totals,
        "after_admission_deadline": True,
        "closed_barriers_verified": False,
        "limits": {"maximum_files": max_files, "deadline_seconds": timeout},
        "scope": "All regular files currently retained in this poll's exact partition directories, summed across three local replicas. Metadata-only stat; no record contents read.",
        "limitations": [
            "Requires a separately successful independent CLOSED journal audit; elapsed admission time and present replica directories do not prove CLOSED, ISR health or ACK durability.",
            "This is a non-atomic filesystem snapshot. Retention, background writes or replica movement can change it; it does not prove the original event's peak or cumulative bytes.",
            ".log includes Kafka record/batch overhead, transaction markers and any retained aborted data. Indexes may be preallocated; other includes producer snapshots, partition metadata and temporary/deletion files.",
            "Bytes are already summed across three replicas: do not multiply them by RF3 again. Shared controller/transaction topics, broker logs and checkpoints outside these topic directories are excluded.",
            "Allocated bytes are st_blocks multiplied by 512. Filesystem compression or shared/cloned extents may prevent interpreting their sum as exclusively owned physical device space.",
            "No payload-byte, committed-record-count or vote-count claim is inferred from these file sizes."
        ]
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", required=True,
                        help="existing root checkout containing the owned .local/kafka-lab")
    parser.add_argument("--kafka-config", required=True,
                        help="private mode-0600 journal-config.json for one completed HTTP poll")
    parser.add_argument("--max-files", type=int, default=20000)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args(argv)
    try:
        result = inspect(args.runtime_root, args.kafka_config, args.max_files, args.timeout)
    except InspectionError as error:
        print("kafka_http_disk: " + str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        # Avoid exception details: filesystem and JSON errors can embed the
        # private config path, topic/poll ID, or individual file names.
        print("kafka_http_disk: filesystem or configuration inspection failed", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
