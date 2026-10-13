#!/usr/bin/env python3
"""Validate an independently linked benchmark's accounting against a local peer."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from integration import Peer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench", required=True, type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="zbperf-bench-test-") as temp:
        corpus = Path(temp) / "records.jsonl"
        corpus.write_text('{"producer":0,"seq":0,"pad":"small"}\n'
                          '{"producer":1,"seq":1,"pad":"larger record"}\n')
        for queue, batch, streams, connections, workers in (
                ("mutex", "single", 1, 1, 1),
                ("atomic", "auto", 2, 1, 1),
                ("atomic", "explicit", 2, 2, 1),
                ("mutex", "auto", 2, 2, 2)):
            peer = Peer()
            try:
                env = dict(os.environ, ZB_TOKEN="fixture-secret-must-not-appear")
                command = [str(args.bench), "--host", "localhost", "--port", str(peer.port),
                           "--table", "test.default.events", "--input", str(corpus),
                           "--test-plaintext", "--queue", queue, "--batch", batch,
                           "--producers", "4", "--streams", str(streams),
                           "--connections", str(connections), "--io-threads", str(workers),
                           "--warmup", "0.15", "--duration", "0.25", "--drain-timeout", "15",
                           "--buffer-bytes", "1048576", "--queue-capacity", "128",
                           "--max-inflight", "128", "--batch-records", "16",
                           "--batch-bytes", "65536", "--sample-every", "1"]
                run = subprocess.run(command, env=env, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, universal_newlines=True, timeout=40)
                assert run.returncode == 0, run.stdout + run.stderr
                assert env["ZB_TOKEN"] not in run.stdout + run.stderr
                result = json.loads(run.stdout)
                assert result["ok"]
                measured = result["measurement"]
                assert measured["cohort_acked_records"] == measured["cohort_admitted_records"]
                assert measured["cohort_acked_records"] > 0
                assert result["warmup_acked_records"] > 0
                assert len(peer.records) == result["warmup_acked_records"] + measured["cohort_acked_records"]
                assert measured["acked_records_in_window"] <= measured["cohort_acked_records"]
                assert measured["latency"]["samples"] > 0
                assert measured["window_seconds"] >= 0.25
                assert measured["admission_and_drain_seconds"] >= measured["window_seconds"]
                diagnostics = measured["transport_diagnostics"]
                assert diagnostics["peer_initial_stream_window_min"] == 65535
                assert diagnostics["peer_max_frame_size_min"] == 16384
                flow_seconds = diagnostics["flow_control_seconds_in_window_sum"]
                assert 0 <= flow_seconds <= connections * measured["window_seconds"]
                assert diagnostics["flow_control_seconds_including_drain_sum"] >= flow_seconds
                if diagnostics["stream_window_updates"]:
                    assert 0 < diagnostics["min_stream_window_update_lifetime"] <= diagnostics["max_stream_window_update_lifetime"]
                assert peer.connection_count == connections
                assert peer.stream_count == streams
                assert not peer.errors, "\n".join(peer.errors)
                print("PASS benchmark accounting", queue, batch, streams, connections, workers, flush=True)
            finally:
                peer.close()


if __name__ == "__main__":
    main()
