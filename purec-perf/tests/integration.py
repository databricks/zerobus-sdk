#!/usr/bin/env python3
"""Loopback HTTP/2 peer for public-API correctness tests, never a perf oracle.

Requires Python h2>=4,<5 and openssl. All certificates and outputs are temporary.
The server decodes the small protobuf surface independently of the SDK encoder.
"""
import argparse
import collections
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import traceback

import h2.config
import h2.connection
import h2.events
import h2.settings


def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def pb_bytes(field, data):
    return varint(field * 8 + 2) + varint(len(data)) + data


def pb_int(field, value):
    return varint(field * 8) + varint(value)


def decode_fields(data):
    """Bounded protobuf reader for the fixture's expected string/varint fields."""
    cursor = 0

    def integer():
        nonlocal cursor
        value = 0
        for shift in range(0, 70, 7):
            if cursor == len(data):
                raise AssertionError("truncated varint")
            byte = data[cursor]
            cursor += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise AssertionError("oversized varint")

    result = collections.defaultdict(list)
    while cursor < len(data):
        tag = integer()
        field, wire = tag >> 3, tag & 7
        assert field > 0
        if wire == 0:
            value = integer()
        elif wire == 2:
            size = integer()
            assert size <= len(data) - cursor
            value = bytes(data[cursor:cursor + size])
            cursor += size
        else:
            raise AssertionError("unexpected wire type %d" % wire)
        result[field].append(value)
    return result


def frame(data):
    return b"\0" + struct.pack("!I", len(data)) + data


class Peer:
    def __init__(self, mode="normal", tls_context=None):
        self.mode = mode
        self.context = tls_context
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.errors = []
        self.records = []
        self.requests = 0
        self.connection_count = 0
        self.stream_count = 0
        self.clients = []
        self.threads = []
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.listener.settimeout(0.2)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stop.is_set():
            try:
                sock, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            sock.settimeout(3)
            if self.mode == "full_duplex":
                # Keep both kernel directions small relative to in-flight data.
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
            with self.lock:
                self.connection_count += 1
                self.clients.append(sock)
            thread = threading.Thread(target=self.serve, args=(sock,), daemon=True)
            self.threads.append(thread)
            thread.start()

    def serve(self, sock):
        try:
            if self.context:
                sock = self.context.wrap_socket(sock, server_side=True)
                assert sock.selected_alpn_protocol() == "h2"
                with self.lock:
                    self.clients.append(sock)
            h = h2.connection.H2Connection(config=h2.config.H2Configuration(
                client_side=False, header_encoding="utf-8"))
            h.initiate_connection()
            if self.mode == "small_window":
                h.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 1024})
            if self.mode == "stalled_flush":
                # Exactly one 22-byte JSON record plus its 11-byte envelope.
                h.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 33})
            if self.mode == "full_duplex":
                h.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 16 * 1024 * 1024})
                h.increment_flow_control_window(16 * 1024 * 1024 - 65535)
            sock.sendall(h.data_to_send())
            buffers = collections.defaultdict(bytearray)
            created = set()
            previous = {}
            failed = set()

            def reply(sid, message):
                if self.mode == "full_duplex":
                    # A valid unknown protobuf field makes receive pressure
                    # substantial while retaining normal cumulative ack semantics.
                    message += pb_bytes(100, b"x" * 8192)
                wire = frame(message)
                # Fragment headers, varints, and payloads across separate DATA frames.
                for start, end in ((0, 1), (1, 3), (3, 7), (7, len(wire))):
                    if end > start:
                        h.send_data(sid, wire[start:end])

            while not self.stop.is_set():
                try:
                    data = sock.recv(8192)
                except socket.timeout:
                    continue
                if not data:
                    break
                for event in h.receive_data(data):
                    if isinstance(event, h2.events.RequestReceived):
                        headers = dict(event.headers)
                        assert headers[":path"] == "/databricks.zerobus.Zerobus/EphemeralStream"
                        assert headers["content-type"].startswith("application/grpc")
                        assert headers["x-databricks-zerobus-table-name"] == "test.default.events"
                        assert headers["authorization"].startswith("Bearer ")
                        h.send_headers(event.stream_id, [
                            (":status", "200"), ("content-type", "application/grpc")])
                    elif isinstance(event, h2.events.DataReceived):
                        sid = event.stream_id
                        if self.mode != "stalled_flush" and (self.mode != "flow_stall" or sid not in created):
                            h.acknowledge_received_data(event.flow_controlled_length, sid)
                        if sid in failed:
                            continue
                        buf = buffers[sid]
                        buf.extend(event.data)
                        while len(buf) >= 5:
                            assert buf[0] == 0
                            size = struct.unpack("!I", buf[1:5])[0]
                            assert size <= 10 * 1024 * 1024
                            if len(buf) < size + 5:
                                break
                            fields = decode_fields(bytes(buf[5:5 + size]))
                            del buf[:5 + size]  # Fixture only; never on SDK hot path.
                            if 1 in fields:
                                assert sid not in created
                                create = decode_fields(fields[1][0])
                                assert create[1] == [b"test.default.events"]
                                assert create[4] == [2]
                                created.add(sid)
                                if self.mode == "stalled_flush":
                                    # Replenish create bytes explicitly; h2's
                                    # automatic update threshold may be larger.
                                    h.increment_flow_control_window(size + 5, sid)
                                with self.lock:
                                    self.stream_count += 1
                                reply(sid, pb_bytes(1, pb_bytes(1, b"fixture-stream")))
                                continue
                            assert sid in created
                            assert (2 in fields) != (3 in fields)
                            request = decode_fields(fields[2 if 2 in fields else 3][0])
                            offset = request[1][0]
                            assert offset == previous.get(sid, -1) + 1, (offset, previous)
                            previous[sid] = offset
                            records = request[3] if 2 in fields else decode_fields(request[3][0])[1]
                            assert records
                            decoded = [json.loads(record) for record in records]
                            with self.lock:
                                self.requests += 1
                                self.records.extend(decoded)
                            if self.mode == "reset":
                                h.reset_stream(sid, error_code=2)
                                failed.add(sid)
                                break
                            if self.mode == "bad_trailer":
                                h.send_headers(sid, [("grpc-status", "13")], end_stream=True)
                                failed.add(sid)
                                break
                            if self.mode == "truncated":
                                h.send_data(sid, b"\0\0\0\0\x20\x12")
                                h.send_headers(sid, [("grpc-status", "0")], end_stream=True)
                                failed.add(sid)
                                break
                            if self.mode == "late_ack" and offset == 0:
                                continue
                            if self.mode not in ("timeout", "abort", "flow_stall"):
                                ack = offset + 100 if self.mode == "future_ack" else offset
                                reply(sid, pb_bytes(2, pb_int(1, ack)))
                                if self.mode == "duplicate_ack":
                                    reply(sid, pb_bytes(2, pb_int(1, ack)))
                    elif isinstance(event, h2.events.StreamEnded):
                        if event.stream_id not in failed:
                            h.send_headers(event.stream_id, [("grpc-status", "0")], end_stream=True)
                    elif isinstance(event, h2.events.ConnectionTerminated):
                        return
                outbound = h.data_to_send()
                if outbound:
                    sock.sendall(outbound)
        except (ConnectionResetError, BrokenPipeError):
            # The client intentionally aborts in terminal/error tests and teardown.
            pass
        except ssl.SSLError:
            if self.mode != "reject_tls" and not self.stop.is_set():
                with self.lock:
                    self.errors.append(traceback.format_exc())
        except Exception:
            if not self.stop.is_set():
                with self.lock:
                    self.errors.append(traceback.format_exc())
        finally:
            sock.close()

    def close(self):
        self.stop.set()
        self.listener.close()
        with self.lock:
            for sock in self.clients:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        self.thread.join(timeout=5)
        for thread in self.threads:
            thread.join(timeout=5)
            assert not thread.is_alive(), "fixture thread did not stop"


def certificate(directory, stem):
    cert, key = directory / (stem + ".crt"), directory / (stem + ".key")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                    "-keyout", str(key), "-out", str(cert), "-days", "1",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return cert, key


def run_case(client, context, ca, tls, queue, batch, retain, workers, conns,
             mode="normal", idle=0, coalesce=65536):
    peer = Peer(mode, context if tls else None)
    args = [str(client), str(peer.port), str(int(tls)), str(ca), str(queue),
            str(batch), str(retain), str(workers), str(conns), mode,
            str(idle), str(coalesce)]
    try:
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, timeout=45)
        if result.returncode:
            raise AssertionError("client failed (%d): %s\n%s\n%s\npeer: %s" % (
                result.returncode, " ".join(args), result.stdout, result.stderr,
                "\n".join(peer.errors)))
        assert not peer.errors, "\n".join(peer.errors)
        if mode in ("normal", "small_window", "duplicate_ack", "full_duplex"):
            assert len(peer.records) == 2000, len(peer.records)
            seen = {(r["producer"], r["seq"]) for r in peer.records}
            expected = {(producer, seq) for producer in range(8) for seq in range(250)}
            assert seen == expected, "records lost, duplicated, or corrupted"
            assert peer.connection_count == conns
            assert peer.stream_count == 4
        print("PASS", "TLS" if tls else "h2c", "queue", queue, "batch", batch,
              "retain", retain, "workers", workers, "conns", conns, mode,
              "idle", idle, "coalesce", coalesce, flush=True)
    finally:
        peer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, type=Path)
    parser.add_argument("--quick", action="store_true", help="smaller sanitizer-friendly matrix")
    parser.add_argument("--scenario", help="run only one named peer scenario for diagnosis")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="zbperf-fixture-") as temp:
        directory = Path(temp)
        cert, key = certificate(directory, "trusted")
        other_cert, _ = certificate(directory, "untrusted")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.set_alpn_protocols(["h2"])
        context.load_cert_chain(cert, key)
        if args.scenario:
            run_case(args.client, context, other_cert if args.scenario == "reject_tls" else cert,
                     True, 1, 0, 1, 1, 1, args.scenario)
            return
        variants = [(0, 0, 0), (1, 1, 1), (1, 2, 0)] if args.quick else [
            (queue, batch, retain) for queue in (0, 1) for batch in (0, 1, 2) for retain in (0, 1)]
        for queue, batch, retain in variants:
            run_case(args.client, context, cert, False, queue, batch, retain, 1, 1)
            run_case(args.client, context, cert, True, queue, batch, retain, 1, 2)
        for idle in (1, 2):
            run_case(args.client, context, cert, True, 1, 1, 0, 2, 2,
                     idle=idle, coalesce=0)
        for coalesce in (0, 65536):
            run_case(args.client, context, cert, True, 1, 0, 0, 1, 1,
                     "full_duplex", coalesce=coalesce)
        for mode in ("small_window", "duplicate_ack", "flush_delay", "stalled_flush", "late_ack", "timeout", "abort", "reset",
                     "future_ack", "bad_trailer", "truncated", "flow_stall"):
            run_case(args.client, context, cert, True, 1, 0, 1, 1, 1, mode)
        run_case(args.client, context, other_cert, True, 0, 0, 0, 1, 1, "reject_tls")


if __name__ == "__main__":
    main()
