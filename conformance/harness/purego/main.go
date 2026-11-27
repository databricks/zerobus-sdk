// Command zerobus-testing-harness-purego is the Layer-1 conformance harness for the
// pure-Go Zerobus SDK (purego/, built from this checkout).
//
// Handshake: binds 127.0.0.1:0, prints exactly "HARNESS_ENDPOINT=127.0.0.1:<port>"
// to stdout, then serves the TestHarness gRPC service. Logs go to stderr only.
// Exits on the Shutdown RPC or on SIGINT/SIGTERM.
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/databricks/zerobus-sdk/conformance/harness/purego/internal/pb"
)

func main() {
	// stdout is reserved for the endpoint line, so all logging goes to stderr.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if err := run(); err != nil {
		slog.Error("harness failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	// os.Stdout is unbuffered in Go, so this write is the flush.
	if _, err := fmt.Fprintf(os.Stdout, "HARNESS_ENDPOINT=%s\n", lis.Addr()); err != nil {
		return fmt.Errorf("write endpoint: %w", err)
	}
	slog.Info("TestHarness listening", "addr", lis.Addr().String())

	svc := newHarnessService()
	srv := grpc.NewServer()
	pb.RegisterTestHarnessServer(srv, svc)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)

	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()

	select {
	case <-svc.shutdownCh:
		slog.Info("shutting down: Shutdown RPC")
	case sig := <-sigs:
		slog.Info("shutting down: termination signal", "signal", sig.String())
	case err := <-served:
		return fmt.Errorf("serve: %w", err)
	}

	// GracefulStop waits for in-flight RPCs (including the Shutdown response) to
	// finish, then makes Serve return.
	srv.GracefulStop()
	if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serve: %w", err)
	}
	svc.closeAll()
	slog.Info("harness exited")
	return nil
}
