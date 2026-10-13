/* Experimental pure-C Zerobus ingestion SDK. Not a production SDK ABI. */
#ifndef ZBPERF_H
#define ZBPERF_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

typedef struct zbp_runtime zbp_runtime;
typedef struct zbp_connection zbp_connection;
typedef struct zbp_stream zbp_stream;

typedef enum {
  ZBP_OK = 0,
  ZBP_INVALID,
  ZBP_NOMEM,
  ZBP_TIMEOUT,
  ZBP_CLOSED,
  ZBP_IO,
  ZBP_PROTOCOL,
  ZBP_AUTH,
  ZBP_LIMIT
} zbp_status;
typedef enum { ZBP_QUEUE_MUTEX, ZBP_QUEUE_ATOMIC } zbp_queue_mode;
typedef enum { ZBP_BATCH_SINGLE, ZBP_BATCH_AUTO } zbp_batch_mode;
typedef enum { ZBP_RELEASE_SENT, ZBP_RELEASE_ACKED } zbp_retention;
typedef enum { ZBP_IDLE_SLEEP, ZBP_IDLE_SPIN, ZBP_IDLE_BUSY } zbp_idle_mode;

typedef struct {
  size_t io_threads; /* Connections assigned round-robin; default 1. */
  zbp_idle_mode idle;
  unsigned spin_us;
  size_t write_coalesce_bytes; /* 0: direct nghttp2 writes; default 64 KiB. */
} zbp_runtime_config;
typedef struct {
  const char *host;
  uint16_t port;
  const char *token; /* Bare bearer token; copied during open, never logged. */
  const char *ca_file; /* NULL: system trust store. */
  unsigned connect_timeout_ms;
  int test_plaintext; /* Explicit local-test h2c only. TLS verifies host + CA.
                       */
} zbp_connection_config;
typedef struct {
  zbp_queue_mode queue;
  zbp_batch_mode batching;
  zbp_retention retention;
  size_t queue_capacity; /* Power of two, default 4096. */
  size_t buffer_bytes;  /* Exact live payload byte budget, default 64 MiB. */
  size_t max_inflight;  /* Bounds outstanding request metadata, default 4096. */
  size_t batch_bytes;   /* Encoded message cap, default 1 MiB. */
  size_t batch_records; /* Automatic batch cap, default 1024. */
  unsigned batch_delay_us; /* 0 drains available work without waiting. */
  unsigned admission_timeout_ms;
  unsigned sample_every; /* Sample admission-to-ack latency; 0 disables. */
} zbp_stream_config;
typedef struct {
  const void *data;
  size_t size;
} zbp_record;
typedef struct {
  uint64_t admitted_records, admitted_bytes;
  uint64_t acked_records, acked_bytes;
  uint64_t wire_requests, ack_messages;
  /* Payload bytes reserved for admission or retained; excludes allocator overhead. */
  uint64_t buffered_bytes, peak_buffered_bytes, queue_depth, peak_queue_depth;
  uint64_t backpressure_waits, backpressure_ns;
  uint64_t latency_samples, latency_sum_ns, latency_max_ns;
  /* log2(us), bucket 0 includes <2us, last bucket saturates. */
  uint64_t latency_histogram[32];
  zbp_status status;
} zbp_stream_stats;
typedef struct {
  uint64_t write_calls, read_calls, bytes_written, bytes_read;
  uint64_t flow_control_stalls, wakeups;
  zbp_status status;
} zbp_connection_stats;
/* Peer credit limits our sending. These are not our advertised receive windows. */
typedef struct {
  uint64_t peer_initial_stream_window, peer_max_frame_size;
  int64_t remote_connection_window, min_remote_stream_window;
  uint64_t peak_remote_connection_window, peak_remote_stream_window;
  uint64_t flow_control_ns, write_would_block, write_want_read;
  uint64_t send_budget_yields;
  uint64_t stream_window_updates;
  uint64_t min_stream_window_update, max_stream_window_update;
} zbp_connection_diagnostics;

void zbp_runtime_config_init(zbp_runtime_config *config);
void zbp_connection_config_init(zbp_connection_config *config);
void zbp_stream_config_init(zbp_stream_config *config);
const char *zbp_status_string(zbp_status status);
const char *zbp_version(void);

zbp_status zbp_runtime_create(const zbp_runtime_config *config,
                              zbp_runtime **out);
/* Destroy after all API callers stop. Aborts and frees remaining
 * connections/streams. */
void zbp_runtime_destroy(zbp_runtime *runtime);
zbp_status zbp_connection_open(zbp_runtime *runtime,
                               const zbp_connection_config *config,
                               zbp_connection **out);
zbp_status zbp_stream_open(zbp_connection *connection, const char *table,
                           const zbp_stream_config *config, zbp_stream **out);
/* Thread-safe. Success copies all input bytes; caller can immediately reuse
 * them. Queue records in a loop, then flush once. Never wait for every record's
 * ack. */
zbp_status zbp_ingest(zbp_stream *stream, const void *json, size_t size);
/* All-or-nothing admission; one explicit protocol batch, independent of auto
 * mode. */
zbp_status zbp_ingest_batch(zbp_stream *stream, const zbp_record *records,
                            size_t count);
/* Thread-safe barrier. Covers all ingests completed before this call, not later
 * calls. Timeout includes barrier admission and worker scheduling. An empty
 * flush needs no new network traffic but still observes the deadline. */
zbp_status zbp_flush(zbp_stream *stream, unsigned timeout_ms);
/* Stop producers before close. Closes admission and drains. Handle remains
 * readable. */
zbp_status zbp_stream_close(zbp_stream *stream, unsigned timeout_ms);
/* Thread-safe terminal cancellation. Does not free handles; destroy runtime
 * afterward. */
void zbp_stream_abort(zbp_stream *stream);
/* Nonblocking snapshots; individual counters are atomic, the snapshot is not
 * transactional. */
void zbp_stream_get_stats(const zbp_stream *stream, zbp_stream_stats *out);
void zbp_connection_get_stats(const zbp_connection *connection,
                              zbp_connection_stats *out);
/* Read-only transport observations; minimum stream credit is zero if none exist.
 * Counters are cumulative. Flow-control time includes a currently observed stall.
 */
void zbp_connection_get_diagnostics(const zbp_connection *connection,
                                    zbp_connection_diagnostics *out);

#ifdef __cplusplus
}
#endif
#endif
