"""Layer-1 conformance harness for the Python Zerobus SDK.

Handshake: binds ``127.0.0.1:0``, prints exactly ``HARNESS_ENDPOINT=127.0.0.1:<port>``
to stdout, then serves the ``TestHarness`` gRPC service. Logs go to stderr only.
Exits on the ``Shutdown`` RPC or SIGINT/SIGTERM.
"""

import concurrent.futures
import logging
import os
import signal
import sys
import threading

import grpc

from ._generated import test_harness_pb2_grpc as pb_grpc
from .harness import HarnessService


def main():
    # stdout is reserved for the endpoint line, so all logging goes to stderr.
    logging.basicConfig(
        stream=sys.stderr,
        level=os.environ.get("HARNESS_LOG", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("zerobus_harness")

    shutdown = threading.Event()
    service = HarnessService(shutdown)

    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_TestHarnessServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")  # ephemeral port on loopback only
    server.start()

    sys.stdout.write(f"HARNESS_ENDPOINT=127.0.0.1:{port}\n")
    sys.stdout.flush()
    log.info("TestHarness (python) listening on 127.0.0.1:%d", port)

    def on_signal(signum, _frame):
        log.info("termination signal received (%s)", signal.Signals(signum).name)
        shutdown.set()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    shutdown.wait()
    # Grace period lets the Shutdown RPC response go out before the server stops.
    server.stop(grace=5).wait()
    service.close_all()
    log.info("harness exited")
    return 0


if __name__ == "__main__":
    sys.exit(main())
