#define _GNU_SOURCE
#include "build_info.h"
#include <zbperf.h>

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

typedef enum { BATCH_SINGLE, BATCH_AUTO, BATCH_EXPLICIT } batch_mode;
typedef struct {
  const char *host, *table, *input, *token_env, *ca_file;
  unsigned port;
  size_t producers, streams, connections, io_threads;
  size_t buffer_bytes, queue_capacity, max_inflight;
  size_t per_buffer, per_queue, per_inflight;
  size_t batch_records, batch_bytes, write_coalesce_bytes;
  unsigned batch_delay_us, spin_us, sample_every, admission_timeout_ms;
  double warmup, duration, drain_timeout;
  zbp_queue_mode queue;
  batch_mode batch;
  zbp_retention retention;
  zbp_idle_mode idle;
  bool plaintext, dry_run;
} options;
typedef struct {
  char *storage;
  zbp_record *records;
  size_t count, bytes, max_record_bytes;
  uint64_t fingerprint;
} corpus;
typedef struct {
  zbp_stream_stats stream;
  zbp_connection_stats connection;
  zbp_connection_diagnostics diagnostics;
} snapshot;
typedef struct {
  snapshot before, cutoff, after;
  uint64_t start_ns, cutoff_ns, stopped_ns, drained_ns;
  struct rusage before_usage, cutoff_usage, after_usage;
  uint64_t occupancy_samples, sampled_queue_depth, sampled_buffer_bytes;
  uint64_t sampled_max_queue, sampled_max_buffer;
  zbp_status status;
} phase_result;
typedef struct phase phase;
typedef struct {
  phase *phase;
  size_t id;
  zbp_record *batch;
} producer;
struct phase {
  const options *opts;
  const corpus *data;
  zbp_stream **streams;
  zbp_connection **connections;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool start;
  size_t ready;
  atomic_bool stop;
  atomic_int failure;
};
static volatile sig_atomic_t interrupted;

static void signal_handler(int sig) {
  (void)sig;
  interrupted = 1;
}
static uint64_t now_ns(void) {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (uint64_t)t.tv_sec * UINT64_C(1000000000) + (uint64_t)t.tv_nsec;
}
static struct timespec timespec_from_ns(uint64_t n) {
  struct timespec t = {(time_t)(n / UINT64_C(1000000000)),
                       (long)(n % UINT64_C(1000000000))};
  return t;
}
static void json_string(const char *s) {
  putchar('"');
  for (const unsigned char *p = (const unsigned char *)s; *p; ++p) {
    switch (*p) {
    case '"':
      fputs("\\\"", stdout);
      break;
    case '\\':
      fputs("\\\\", stdout);
      break;
    case '\n':
      fputs("\\n", stdout);
      break;
    case '\r':
      fputs("\\r", stdout);
      break;
    case '\t':
      fputs("\\t", stdout);
      break;
    default:
      if (*p < 32)
        printf("\\u%04x", *p);
      else
        putchar(*p);
    }
  }
  putchar('"');
}
static int error_result(const char *message) {
  fprintf(stderr, "zbperf-bench: %s\n", message);
  fputs("{\"schema_version\":2,\"ok\":false,\"error\":", stdout);
  json_string(message);
  puts("}");
  return 1;
}
static bool number(const char *text, size_t *out) {
  char *end;
  if (!*text || *text == '-' || *text == '+')
    return false;
  errno = 0;
  unsigned long long value = strtoull(text, &end, 10);
  if (errno || *end || value > SIZE_MAX)
    return false;
  *out = (size_t)value;
  return true;
}
static bool seconds(const char *text, double *out) {
  char *end;
  errno = 0;
  double n = strtod(text, &end);
  if (errno || !*text || *end || !isfinite(n) || n < 0 || n > 86400)
    return false;
  *out = n;
  return true;
}
static void usage(void) {
  puts("Usage: zbperf-bench --host HOST --port PORT --table "
       "CATALOG.SCHEMA.TABLE\n"
       "       --input RECORDS.jsonl [options]\n"
       "Required inputs are explicit; token is read from ZB_TOKEN by default.\n"
       "  --token-env NAME             --ca-file PATH\n"
       "  --producers N                --streams N --connections N "
       "--io-threads N\n"
       "  --queue mutex|atomic         --batch single|auto|explicit\n"
       "  --batch-records N            --batch-bytes N --batch-delay-us N\n"
       "  --retention sent|acked       --idle sleep|spin|busy --spin-us N\n"
       "  --write-coalesce-bytes N     --buffer-bytes N (aggregate)\n"
       "  --queue-capacity N           --max-inflight N (aggregate)\n"
       "  --sample-every N             --admission-timeout-ms N\n"
       "  --warmup SECONDS             --duration SECONDS --drain-timeout "
       "SECONDS\n"
       "  --test-plaintext             --dry-run\n"
       "Dry run validates inputs and prints configuration; it opens no "
       "connection.");
}
static int parse_options(int argc, char **argv, options *o) {
  *o = (options){.token_env = "ZB_TOKEN",
                 .producers = 1,
                 .streams = 1,
                 .connections = 1,
                 .io_threads = 1,
                 .buffer_bytes = 64 * 1024 * 1024,
                 .queue_capacity = 4096,
                 .max_inflight = 4096,
                 .batch_records = 1024,
                 .batch_bytes = 1024 * 1024,
                 .write_coalesce_bytes = 65536,
                 .spin_us = 50,
                 .sample_every = 1024,
                 .admission_timeout_ms = 1000,
                 .warmup = 10,
                 .duration = 60,
                 .drain_timeout = 60,
                 .queue = ZBP_QUEUE_MUTEX,
                 .batch = BATCH_SINGLE,
                 .retention = ZBP_RELEASE_SENT,
                 .idle = ZBP_IDLE_SLEEP};
  enum {
    OPT_HOST = 1000,
    OPT_PORT,
    OPT_TABLE,
    OPT_INPUT,
    OPT_TOKEN_ENV,
    OPT_CA,
    OPT_PRODUCERS,
    OPT_STREAMS,
    OPT_CONNECTIONS,
    OPT_IO_THREADS,
    OPT_QUEUE,
    OPT_BATCH,
    OPT_BATCH_RECORDS,
    OPT_BATCH_BYTES,
    OPT_BATCH_DELAY,
    OPT_RETENTION,
    OPT_IDLE,
    OPT_SPIN,
    OPT_COALESCE,
    OPT_BUFFER,
    OPT_CAPACITY,
    OPT_INFLIGHT,
    OPT_SAMPLE,
    OPT_ADMISSION,
    OPT_WARMUP,
    OPT_DURATION,
    OPT_DRAIN,
    OPT_PLAINTEXT,
    OPT_DRY
  };
  static const struct option long_options[] = {
      {"host", 1, NULL, OPT_HOST},
      {"port", 1, NULL, OPT_PORT},
      {"table", 1, NULL, OPT_TABLE},
      {"input", 1, NULL, OPT_INPUT},
      {"token-env", 1, NULL, OPT_TOKEN_ENV},
      {"ca-file", 1, NULL, OPT_CA},
      {"producers", 1, NULL, OPT_PRODUCERS},
      {"streams", 1, NULL, OPT_STREAMS},
      {"connections", 1, NULL, OPT_CONNECTIONS},
      {"io-threads", 1, NULL, OPT_IO_THREADS},
      {"queue", 1, NULL, OPT_QUEUE},
      {"batch", 1, NULL, OPT_BATCH},
      {"batch-records", 1, NULL, OPT_BATCH_RECORDS},
      {"batch-bytes", 1, NULL, OPT_BATCH_BYTES},
      {"batch-delay-us", 1, NULL, OPT_BATCH_DELAY},
      {"retention", 1, NULL, OPT_RETENTION},
      {"idle", 1, NULL, OPT_IDLE},
      {"spin-us", 1, NULL, OPT_SPIN},
      {"write-coalesce-bytes", 1, NULL, OPT_COALESCE},
      {"buffer-bytes", 1, NULL, OPT_BUFFER},
      {"queue-capacity", 1, NULL, OPT_CAPACITY},
      {"max-inflight", 1, NULL, OPT_INFLIGHT},
      {"sample-every", 1, NULL, OPT_SAMPLE},
      {"admission-timeout-ms", 1, NULL, OPT_ADMISSION},
      {"warmup", 1, NULL, OPT_WARMUP},
      {"duration", 1, NULL, OPT_DURATION},
      {"drain-timeout", 1, NULL, OPT_DRAIN},
      {"test-plaintext", 0, NULL, OPT_PLAINTEXT},
      {"dry-run", 0, NULL, OPT_DRY},
      {"help", 0, NULL, 'h'},
      {NULL, 0, NULL, 0}};
  int ch;
  opterr = 0;
  while ((ch = getopt_long(argc, argv, "h", long_options, NULL)) != -1) {
    size_t n = 0;
    switch (ch) {
    case 'h':
      usage();
      return 1;
    case OPT_HOST:
      o->host = optarg;
      break;
    case OPT_TABLE:
      o->table = optarg;
      break;
    case OPT_INPUT:
      o->input = optarg;
      break;
    case OPT_TOKEN_ENV:
      o->token_env = optarg;
      break;
    case OPT_CA:
      o->ca_file = optarg;
      break;
    case OPT_PLAINTEXT:
      o->plaintext = true;
      break;
    case OPT_DRY:
      o->dry_run = true;
      break;
    case OPT_QUEUE:
      if (!strcmp(optarg, "mutex"))
        o->queue = ZBP_QUEUE_MUTEX;
      else if (!strcmp(optarg, "atomic"))
        o->queue = ZBP_QUEUE_ATOMIC;
      else
        return -1;
      break;
    case OPT_BATCH:
      if (!strcmp(optarg, "single"))
        o->batch = BATCH_SINGLE;
      else if (!strcmp(optarg, "auto"))
        o->batch = BATCH_AUTO;
      else if (!strcmp(optarg, "explicit"))
        o->batch = BATCH_EXPLICIT;
      else
        return -1;
      break;
    case OPT_RETENTION:
      if (!strcmp(optarg, "sent"))
        o->retention = ZBP_RELEASE_SENT;
      else if (!strcmp(optarg, "acked"))
        o->retention = ZBP_RELEASE_ACKED;
      else
        return -1;
      break;
    case OPT_IDLE:
      if (!strcmp(optarg, "sleep"))
        o->idle = ZBP_IDLE_SLEEP;
      else if (!strcmp(optarg, "spin"))
        o->idle = ZBP_IDLE_SPIN;
      else if (!strcmp(optarg, "busy"))
        o->idle = ZBP_IDLE_BUSY;
      else
        return -1;
      break;
    case OPT_WARMUP:
      if (!seconds(optarg, &o->warmup))
        return -1;
      break;
    case OPT_DURATION:
      if (!seconds(optarg, &o->duration))
        return -1;
      break;
    case OPT_DRAIN:
      if (!seconds(optarg, &o->drain_timeout))
        return -1;
      break;
    default:
      if (!optarg || !number(optarg, &n))
        return -1;
      switch (ch) {
      case OPT_PORT:
        if (n > 65535)
          return -1;
        o->port = (unsigned)n;
        break;
      case OPT_PRODUCERS:
        o->producers = n;
        break;
      case OPT_STREAMS:
        o->streams = n;
        break;
      case OPT_CONNECTIONS:
        o->connections = n;
        break;
      case OPT_IO_THREADS:
        o->io_threads = n;
        break;
      case OPT_BATCH_RECORDS:
        o->batch_records = n;
        break;
      case OPT_BATCH_BYTES:
        o->batch_bytes = n;
        break;
      case OPT_COALESCE:
        o->write_coalesce_bytes = n;
        break;
      case OPT_BUFFER:
        o->buffer_bytes = n;
        break;
      case OPT_CAPACITY:
        o->queue_capacity = n;
        break;
      case OPT_INFLIGHT:
        o->max_inflight = n;
        break;
      default:
        if (n > UINT_MAX)
          return -1;
        switch (ch) {
        case OPT_BATCH_DELAY:
          o->batch_delay_us = (unsigned)n;
          break;
        case OPT_SPIN:
          o->spin_us = (unsigned)n;
          break;
        case OPT_SAMPLE:
          o->sample_every = (unsigned)n;
          break;
        case OPT_ADMISSION:
          o->admission_timeout_ms = (unsigned)n;
          break;
        default:
          return -1;
        }
      }
    }
  }
  if (optind != argc || !o->host || !*o->host || !o->table || !*o->table ||
      !o->input || !*o->input || !o->port || !*o->token_env || !o->producers ||
      !o->streams || !o->connections || !o->io_threads ||
      o->streams > o->producers || o->connections > o->streams ||
      o->io_threads > o->connections || o->producers > 65536 ||
      !o->batch_records || o->batch_records > SIZE_MAX / sizeof(zbp_record) ||
      o->batch_bytes < 64 || o->duration <= 0 || o->drain_timeout <= 0 ||
      o->drain_timeout * 1000 > UINT_MAX || !o->admission_timeout_ms)
    return -1;
  o->per_buffer = o->buffer_bytes / o->streams;
  o->per_inflight = o->max_inflight / o->streams;
  size_t queue_share = o->queue_capacity / o->streams;
  o->per_queue = 1;
  while (o->per_queue <= queue_share / 2)
    o->per_queue *= 2;
  if (queue_share < 2 || !o->per_buffer || !o->per_inflight)
    return -1;
  if (o->plaintext && strcmp(o->host, "127.0.0.1") &&
      strcmp(o->host, "localhost") && strcmp(o->host, "::1"))
    return -1;
  return 0;
}

static bool load_corpus(const char *path, corpus *data) {
  FILE *file = fopen(path, "rb");
  if (!file)
    return false;
  struct stat st;
  if (fstat(fileno(file), &st) || !S_ISREG(st.st_mode) || st.st_size <= 0 ||
      (uintmax_t)st.st_size >= SIZE_MAX) {
    fclose(file);
    return false;
  }
  size_t size = (size_t)st.st_size;
  data->storage = malloc(size + 1);
  if (!data->storage) {
    fclose(file);
    return false;
  }
  bool ok = fread(data->storage, 1, size, file) == size && !ferror(file);
  fclose(file);
  if (!ok || memchr(data->storage, '\0', size))
    return false;
  data->storage[size] = '\0';
  size_t lines = 1;
  data->fingerprint = UINT64_C(14695981039346656037);
  for (size_t i = 0; i < size; ++i) {
    if (data->storage[i] == '\n')
      ++lines;
    data->fingerprint ^= (unsigned char)data->storage[i];
    data->fingerprint *= UINT64_C(1099511628211);
  }
  if (lines > SIZE_MAX / sizeof(zbp_record))
    return false;
  data->records = calloc(lines, sizeof(*data->records));
  if (!data->records)
    return false;
  char *line = data->storage;
  for (size_t i = 0; i <= size; ++i) {
    if (i != size && data->storage[i] != '\n')
      continue;
    char *end = data->storage + i;
    if (end > line && end[-1] == '\r')
      --end;
    size_t len = (size_t)(end - line);
    if (len) {
      data->records[data->count++] = (zbp_record){line, len};
      data->bytes += len;
      if (len > data->max_record_bytes)
        data->max_record_bytes = len;
    }
    line = data->storage + i + 1;
  }
  return data->count != 0;
}
static void free_corpus(corpus *data) {
  free(data->records);
  free(data->storage);
}

static void get_snapshot(const phase *p, snapshot *out) {
  memset(out, 0, sizeof(*out));
  for (size_t i = 0; i < p->opts->streams; ++i) {
    zbp_stream_stats s;
    zbp_stream_get_stats(p->streams[i], &s);
#define ADD_STREAM(field) out->stream.field += s.field
    ADD_STREAM(admitted_records);
    ADD_STREAM(admitted_bytes);
    ADD_STREAM(acked_records);
    ADD_STREAM(acked_bytes);
    ADD_STREAM(wire_requests);
    ADD_STREAM(ack_messages);
    ADD_STREAM(buffered_bytes);
    ADD_STREAM(peak_buffered_bytes);
    ADD_STREAM(queue_depth);
    ADD_STREAM(peak_queue_depth);
    ADD_STREAM(backpressure_waits);
    ADD_STREAM(backpressure_ns);
    ADD_STREAM(latency_samples);
    ADD_STREAM(latency_sum_ns);
#undef ADD_STREAM
    if (s.latency_max_ns > out->stream.latency_max_ns)
      out->stream.latency_max_ns = s.latency_max_ns;
    for (size_t j = 0; j < 32; ++j)
      out->stream.latency_histogram[j] += s.latency_histogram[j];
    if (s.status != ZBP_OK)
      out->stream.status = s.status;
  }
  for (size_t i = 0; i < p->opts->connections; ++i) {
    zbp_connection_stats s;
    zbp_connection_get_stats(p->connections[i], &s);
#define ADD_CONNECTION(field) out->connection.field += s.field
    ADD_CONNECTION(write_calls);
    ADD_CONNECTION(read_calls);
    ADD_CONNECTION(bytes_written);
    ADD_CONNECTION(bytes_read);
    ADD_CONNECTION(flow_control_stalls);
    ADD_CONNECTION(wakeups);
#undef ADD_CONNECTION
    if (s.status != ZBP_OK)
      out->connection.status = s.status;
    zbp_connection_diagnostics d;
    zbp_connection_get_diagnostics(p->connections[i], &d);
#define ADD_DIAGNOSTIC(field) out->diagnostics.field += d.field
    ADD_DIAGNOSTIC(flow_control_ns);
    ADD_DIAGNOSTIC(write_would_block);
    ADD_DIAGNOSTIC(write_want_read);
    ADD_DIAGNOSTIC(send_budget_yields);
    ADD_DIAGNOSTIC(stream_window_updates);
#undef ADD_DIAGNOSTIC
#define MIN_DIAGNOSTIC(field)                                                  \
    if (!i || d.field < out->diagnostics.field)                                 \
      out->diagnostics.field = d.field
    MIN_DIAGNOSTIC(peer_initial_stream_window);
    MIN_DIAGNOSTIC(peer_max_frame_size);
    MIN_DIAGNOSTIC(remote_connection_window);
    MIN_DIAGNOSTIC(min_remote_stream_window);
#undef MIN_DIAGNOSTIC
    if (d.min_stream_window_update &&
        (!out->diagnostics.min_stream_window_update ||
         d.min_stream_window_update < out->diagnostics.min_stream_window_update))
      out->diagnostics.min_stream_window_update = d.min_stream_window_update;
#define MAX_DIAGNOSTIC(field)                                                  \
    if (d.field > out->diagnostics.field)                                       \
      out->diagnostics.field = d.field
    MAX_DIAGNOSTIC(peak_remote_connection_window);
    MAX_DIAGNOSTIC(peak_remote_stream_window);
    MAX_DIAGNOSTIC(max_stream_window_update);
#undef MAX_DIAGNOSTIC
  }
}
static void stop_phase(phase *p, zbp_status status) {
  int expected = ZBP_OK;
  atomic_compare_exchange_strong(&p->failure, &expected, (int)status);
  atomic_store_explicit(&p->stop, true, memory_order_relaxed);
  pthread_mutex_lock(&p->mutex);
  pthread_cond_broadcast(&p->cond);
  pthread_mutex_unlock(&p->mutex);
}
static void *produce(void *opaque) {
  producer *producer = opaque;
  phase *p = producer->phase;
  const options *o = p->opts;
  const corpus *data = p->data;
  zbp_stream *stream = p->streams[producer->id % o->streams];
  size_t index = producer->id % data->count;
  pthread_mutex_lock(&p->mutex);
  ++p->ready;
  pthread_cond_broadcast(&p->cond);
  while (!p->start)
    pthread_cond_wait(&p->cond, &p->mutex);
  pthread_mutex_unlock(&p->mutex);
  while (!atomic_load_explicit(&p->stop, memory_order_relaxed)) {
    zbp_status status;
    if (o->batch == BATCH_EXPLICIT) {
      size_t count = 0, bytes = 64;
      while (count < o->batch_records) {
        zbp_record record = data->records[index];
        if (record.size > o->batch_bytes - bytes ||
            record.size + 10 > o->batch_bytes - bytes)
          break;
        producer->batch[count++] = record;
        bytes += record.size + 10;
        if (++index == data->count)
          index = 0;
      }
      status = zbp_ingest_batch(stream, producer->batch, count);
    } else {
      const zbp_record *record = &data->records[index];
      status = zbp_ingest(stream, record->data, record->size);
      if (++index == data->count)
        index = 0;
    }
    if (status != ZBP_OK) {
      stop_phase(p, status);
      break;
    }
  }
  return NULL;
}
static unsigned remaining_ms(uint64_t deadline) {
  uint64_t now = now_ns();
  if (now >= deadline)
    return 0;
  uint64_t ms = (deadline - now + 999999) / 1000000;
  return ms > UINT_MAX ? UINT_MAX : (unsigned)ms;
}
static zbp_status drain(phase *p) {
  uint64_t deadline = now_ns() + (uint64_t)(p->opts->drain_timeout * 1e9);
  for (size_t i = 0; i < p->opts->streams; ++i) {
    unsigned ms = remaining_ms(deadline);
    if (!ms)
      return ZBP_TIMEOUT;
    zbp_status status = zbp_flush(p->streams[i], ms);
    if (status != ZBP_OK)
      return status;
  }
  return ZBP_OK;
}
static void sample_occupancy(phase_result *result, const snapshot *s) {
  ++result->occupancy_samples;
  result->sampled_queue_depth += s->stream.queue_depth;
  result->sampled_buffer_bytes += s->stream.buffered_bytes;
  if (s->stream.queue_depth > result->sampled_max_queue)
    result->sampled_max_queue = s->stream.queue_depth;
  if (s->stream.buffered_bytes > result->sampled_max_buffer)
    result->sampled_max_buffer = s->stream.buffered_bytes;
}
static zbp_status run_phase(phase *p, double duration, phase_result *result) {
  memset(result, 0, sizeof(*result));
  if (duration == 0)
    return ZBP_OK;
  pthread_t *threads = calloc(p->opts->producers, sizeof(*threads));
  producer *args = calloc(p->opts->producers, sizeof(*args));
  if (!threads || !args) {
    free(threads);
    free(args);
    return ZBP_NOMEM;
  }
  p->start = false;
  p->ready = 0;
  atomic_store(&p->stop, false);
  atomic_store(&p->failure, ZBP_OK);
  size_t created = 0;
  zbp_status status = ZBP_OK;
  for (size_t i = 0; i < p->opts->producers; ++i) {
    args[i] = (producer){.phase = p, .id = i};
    if (p->opts->batch == BATCH_EXPLICIT) {
      args[i].batch = calloc(p->opts->batch_records, sizeof(zbp_record));
      if (!args[i].batch) {
        status = ZBP_NOMEM;
        break;
      }
    }
    if (pthread_create(&threads[i], NULL, produce, &args[i])) {
      status = ZBP_IO;
      break;
    }
    ++created;
  }
  pthread_mutex_lock(&p->mutex);
  while (p->ready < created)
    pthread_cond_wait(&p->cond, &p->mutex);
  get_snapshot(p, &result->before);
  getrusage(RUSAGE_SELF, &result->before_usage);
  result->start_ns = now_ns();
  uint64_t deadline = result->start_ns + (uint64_t)(duration * 1e9);
  if (status != ZBP_OK)
    atomic_store(&p->stop, true);
  p->start = true;
  pthread_cond_broadcast(&p->cond);
  while (status == ZBP_OK && !atomic_load(&p->stop)) {
    uint64_t now = now_ns();
    if (now >= deadline || interrupted)
      break;
    uint64_t tick = now + UINT64_C(100000000);
    struct timespec wake = timespec_from_ns(tick < deadline ? tick : deadline);
    pthread_cond_timedwait(&p->cond, &p->mutex, &wake);
    snapshot current;
    get_snapshot(p, &current);
    sample_occupancy(result, &current);
    if (current.stream.status != ZBP_OK)
      status = current.stream.status;
    else if (current.connection.status != ZBP_OK)
      status = current.connection.status;
  }
  atomic_store_explicit(&p->stop, true, memory_order_relaxed);
  get_snapshot(p, &result->cutoff);
  result->cutoff_ns = now_ns();
  getrusage(RUSAGE_SELF, &result->cutoff_usage);
  pthread_mutex_unlock(&p->mutex);
  for (size_t i = 0; i < created; ++i)
    pthread_join(threads[i], NULL);
  result->stopped_ns = now_ns();
  if (status == ZBP_OK)
    status = (zbp_status)atomic_load(&p->failure);
  if (interrupted && status == ZBP_OK)
    status = ZBP_CLOSED;
  if (status == ZBP_OK)
    status = drain(p);
  if (status != ZBP_OK)
    for (size_t i = 0; i < p->opts->streams; ++i)
      zbp_stream_abort(p->streams[i]);
  get_snapshot(p, &result->after);
  result->drained_ns = now_ns();
  getrusage(RUSAGE_SELF, &result->after_usage);
  if (status == ZBP_OK &&
      (result->after.stream.acked_records !=
           result->after.stream.admitted_records ||
       result->after.stream.acked_bytes != result->after.stream.admitted_bytes))
    status = ZBP_PROTOCOL;
  for (size_t i = 0; i < p->opts->producers; ++i)
    free(args[i].batch);
  free(args);
  free(threads);
  result->status = status;
  return status;
}

static const char *batch_name(batch_mode m) {
  return m == BATCH_AUTO ? "auto" : m == BATCH_EXPLICIT ? "explicit" : "single";
}
static void print_config(const options *o, const corpus *data) {
  fputs("\"config\":{\"host\":", stdout);
  json_string(o->host);
  printf(",\"port\":%u,\"table\":", o->port);
  json_string(o->table);
  fputs(",\"input\":", stdout);
  json_string(o->input);
  fputs(",\"token_env\":", stdout);
  json_string(o->token_env);
  fputs(",\"ca_file\":", stdout);
  if (o->ca_file)
    json_string(o->ca_file);
  else
    fputs("null", stdout);
  printf(",\"test_plaintext\":%s,\"producers\":%zu,\"streams\":%zu,"
         "\"connections\":%zu,\"io_threads\":%zu,\"queue\":\"%s\",\"batch\":\"%"
         "s\","
         "\"retention\":\"%s\",\"idle\":\"%s\",\"spin_us\":%u,"
         "\"batch_records\":%zu,\"batch_bytes\":%zu,\"batch_delay_us\":%u,"
         "\"write_coalesce_bytes\":%zu,\"buffer_bytes_requested\":%zu,"
         "\"buffer_bytes_effective\":%zu,\"buffer_bytes_per_stream\":%zu,"
         "\"queue_capacity_requested\":%zu,\"queue_capacity_effective\":%zu,"
         "\"queue_capacity_per_stream\":%zu,\"max_inflight_requested\":%zu,"
         "\"max_inflight_effective\":%zu,\"max_inflight_per_stream\":%zu,"
         "\"sample_every\":%u,\"admission_timeout_ms\":%u,"
         "\"warmup_seconds\":%.9g,\"duration_seconds\":%.9g,\"drain_timeout_"
         "seconds\":%.9g},",
         o->plaintext ? "true" : "false", o->producers, o->streams,
         o->connections, o->io_threads,
         o->queue == ZBP_QUEUE_ATOMIC ? "atomic" : "mutex",
         batch_name(o->batch),
         o->retention == ZBP_RELEASE_ACKED ? "acked" : "sent",
         o->idle == ZBP_IDLE_BUSY   ? "busy"
         : o->idle == ZBP_IDLE_SPIN ? "spin"
                                    : "sleep",
         o->spin_us, o->batch_records, o->batch_bytes, o->batch_delay_us,
         o->write_coalesce_bytes, o->buffer_bytes, o->per_buffer * o->streams,
         o->per_buffer, o->queue_capacity, o->per_queue * o->streams,
         o->per_queue, o->max_inflight, o->per_inflight * o->streams,
         o->per_inflight, o->sample_every, o->admission_timeout_ms, o->warmup,
         o->duration, o->drain_timeout);
  printf("\"corpus\":{\"records\":%zu,\"payload_bytes\":%zu,\"max_record_"
         "bytes\":%zu,"
         "\"fnv1a64\":\"%016" PRIx64 "\"},",
         data->count, data->bytes, data->max_record_bytes, data->fingerprint);
  fputs("\"build\":{\"benchmark_version\":", stdout);
  json_string(BENCH_VERSION);
  fputs(",\"sdk_version\":", stdout);
  json_string(zbp_version());
  fputs(",\"compiler\":", stdout);
  json_string(__VERSION__);
  fputs(",\"build_type\":", stdout);
  json_string(BENCH_BUILD_TYPE);
  fputs(",\"benchmark_c_flags\":", stdout);
  json_string(BENCH_BUILD_FLAGS);
  fputs(",\"nghttp2_version\":", stdout);
  json_string(BENCH_NGHTTP2_VERSION);
  fputs(",\"openssl_version\":", stdout);
  json_string(BENCH_OPENSSL_VERSION);
  fputs("},\"environment\":{\"online_cpus\":", stdout);
  printf("%ld", sysconf(_SC_NPROCESSORS_ONLN));
  fputs(",\"allowed_cpus\":[", stdout);
  cpu_set_t affinity;
  bool first = true;
  if (!sched_getaffinity(0, sizeof(affinity), &affinity)) {
    for (int i = 0; i < CPU_SETSIZE; ++i)
      if (CPU_ISSET(i, &affinity)) {
        printf("%s%d", first ? "" : ",", i);
        first = false;
      }
  }
  fputs("]}", stdout);
}
static uint64_t delta(uint64_t end, uint64_t start) {
  return end >= start ? end - start : 0;
}
static double cpu_seconds(const struct rusage *u) {
  return (double)u->ru_utime.tv_sec + u->ru_utime.tv_usec / 1e6 +
         (double)u->ru_stime.tv_sec + u->ru_stime.tv_usec / 1e6;
}
static double percentile_us(const uint64_t *histogram, uint64_t total,
                            double quantile) {
  if (!total)
    return 0;
  uint64_t target = (uint64_t)ceil((double)total * quantile), cumulative = 0;
  for (unsigned i = 0; i < 32; ++i) {
    cumulative += histogram[i];
    if (cumulative >= target)
      return i == 31 ? INFINITY : ldexp(1.0, (int)i + 1);
  }
  return 0;
}
static void print_percentile(const uint64_t *histogram, uint64_t total,
                             double q) {
  double value = percentile_us(histogram, total, q);
  if (isfinite(value))
    printf("%.3f", value);
  else
    fputs("null", stdout);
}
static void print_phase(const phase_result *r) {
  const zbp_stream_stats *b = &r->before.stream, *c = &r->cutoff.stream,
                         *a = &r->after.stream;
  double elapsed = (r->cutoff_ns - r->start_ns) / 1e9;
  double complete = (r->drained_ns - r->start_ns) / 1e9;
  double cpu = cpu_seconds(&r->cutoff_usage) - cpu_seconds(&r->before_usage);
  double complete_cpu =
      cpu_seconds(&r->after_usage) - cpu_seconds(&r->before_usage);
  uint64_t acked = delta(c->acked_records, b->acked_records);
  uint64_t final_acked = delta(a->acked_records, b->acked_records);
  uint64_t hist[32], samples = 0;
  for (size_t i = 0; i < 32; ++i) {
    hist[i] = delta(a->latency_histogram[i], b->latency_histogram[i]);
    samples += hist[i];
  }
  printf(
      ",\"measurement\":{\"window_seconds\":%.9f,\"producer_stop_seconds\":%."
      "9f,"
      "\"admission_and_drain_seconds\":%.9f,\"drain_seconds\":%.9f,"
      "\"acked_records_in_window\":%" PRIu64
      ",\"acked_bytes_in_window\":%" PRIu64 ","
      "\"admitted_records_in_window\":%" PRIu64
      ",\"admitted_bytes_in_window\":%" PRIu64 ","
      "\"acked_records_per_second\":%.3f,\"acked_mb_per_second\":%.6f,"
      "\"admitted_records_per_second\":%.3f,\"cohort_admitted_records\":"
      "%" PRIu64 ","
      "\"cohort_acked_records\":%" PRIu64 ",\"cohort_acked_bytes\":%" PRIu64 ","
      "\"cohort_acked_records_per_second\":%.3f,\"cohort_acked_mb_per_"
      "second\":%.6f,"
      "\"cpu_seconds_in_window\":%.6f,\"cpu_cores_in_window\":%.6f,"
      "\"cpu_ns_per_acked_record_in_window\":%.3f,\"cohort_cpu_seconds\":%.6f,"
      "\"cohort_cpu_ns_per_acked_record\":%.3f,\"process_peak_rss_kib_"
      "lifetime\":%ld,"
      "\"voluntary_context_switches\":%ld,\"involuntary_context_switches\":%ld,"
      "\"wire_requests\":%" PRIu64 ",\"ack_messages\":%" PRIu64 ","
      "\"backpressure_waits\":%" PRIu64 ",\"backpressure_ns\":%" PRIu64 ","
      "\"stream_peak_buffer_bytes_sum_lifetime\":%" PRIu64 ","
      "\"stream_peak_queue_depth_sum_lifetime\":%" PRIu64 ","
      "\"sampled_mean_queue_depth\":%.3f,\"sampled_max_queue_depth\":%" PRIu64
      ","
      "\"sampled_mean_buffer_bytes\":%.3f,\"sampled_max_buffer_bytes\":%" PRIu64
      ","
      "\"occupancy_samples\":%" PRIu64 ",",
      elapsed, (r->stopped_ns - r->start_ns) / 1e9, complete,
      (r->drained_ns - r->stopped_ns) / 1e9, acked,
      delta(c->acked_bytes, b->acked_bytes),
      delta(c->admitted_records, b->admitted_records),
      delta(c->admitted_bytes, b->admitted_bytes),
      elapsed > 0 ? acked / elapsed : 0,
      elapsed > 0 ? delta(c->acked_bytes, b->acked_bytes) / elapsed / 1e6 : 0,
      elapsed > 0 ? delta(c->admitted_records, b->admitted_records) / elapsed
                  : 0,
      delta(a->admitted_records, b->admitted_records), final_acked,
      delta(a->acked_bytes, b->acked_bytes),
      complete > 0 ? final_acked / complete : 0,
      complete > 0 ? delta(a->acked_bytes, b->acked_bytes) / complete / 1e6 : 0,
      cpu,
      elapsed > 0 ? cpu / elapsed : 0, acked ? cpu * 1e9 / acked : 0,
      complete_cpu, final_acked ? complete_cpu * 1e9 / final_acked : 0,
      r->after_usage.ru_maxrss,
      r->after_usage.ru_nvcsw - r->before_usage.ru_nvcsw,
      r->after_usage.ru_nivcsw - r->before_usage.ru_nivcsw,
      delta(a->wire_requests, b->wire_requests),
      delta(a->ack_messages, b->ack_messages),
      delta(a->backpressure_waits, b->backpressure_waits),
      delta(a->backpressure_ns, b->backpressure_ns), a->peak_buffered_bytes,
      a->peak_queue_depth,
      r->occupancy_samples
          ? (double)r->sampled_queue_depth / r->occupancy_samples
          : 0,
      r->sampled_max_queue,
      r->occupancy_samples
          ? (double)r->sampled_buffer_bytes / r->occupancy_samples
          : 0,
      r->sampled_max_buffer, r->occupancy_samples);
  const zbp_connection_stats *cb = &r->before.connection,
                             *ca = &r->after.connection;
  printf("\"transport\":{\"write_calls\":%" PRIu64 ",\"read_calls\":%" PRIu64
         ","
         "\"bytes_written\":%" PRIu64 ",\"bytes_read\":%" PRIu64 ","
         "\"flow_control_stalls\":%" PRIu64 ",\"wakeups\":%" PRIu64 "},",
         delta(ca->write_calls, cb->write_calls),
         delta(ca->read_calls, cb->read_calls),
         delta(ca->bytes_written, cb->bytes_written),
         delta(ca->bytes_read, cb->bytes_read),
         delta(ca->flow_control_stalls, cb->flow_control_stalls),
         delta(ca->wakeups, cb->wakeups));
  const zbp_connection_diagnostics *db = &r->before.diagnostics,
                                   *dc = &r->cutoff.diagnostics,
                                   *da = &r->after.diagnostics;
  printf("\"transport_diagnostics\":{"
         "\"peer_initial_stream_window_min\":%" PRIu64 ","
         "\"peer_max_frame_size_min\":%" PRIu64 ","
         "\"peak_remote_connection_window_max_lifetime\":%" PRIu64 ","
         "\"peak_remote_stream_window_max_lifetime\":%" PRIu64 ","
         "\"remote_connection_window_min_at_cutoff\":%" PRId64 ","
         "\"remote_stream_window_min_at_cutoff\":%" PRId64 ","
         "\"flow_control_seconds_in_window_sum\":%.9f,"
         "\"flow_control_seconds_including_drain_sum\":%.9f,"
         "\"write_would_block\":%" PRIu64 ","
         "\"write_want_read\":%" PRIu64 ","
         "\"send_budget_yields\":%" PRIu64 ","
         "\"stream_window_updates\":%" PRIu64 ","
         "\"min_stream_window_update_lifetime\":%" PRIu64 ","
         "\"max_stream_window_update_lifetime\":%" PRIu64 "},",
         da->peer_initial_stream_window, da->peer_max_frame_size,
         da->peak_remote_connection_window, da->peak_remote_stream_window,
         dc->remote_connection_window, dc->min_remote_stream_window,
         delta(dc->flow_control_ns, db->flow_control_ns) / 1e9,
         delta(da->flow_control_ns, db->flow_control_ns) / 1e9,
         delta(da->write_would_block, db->write_would_block),
         delta(da->write_want_read, db->write_want_read),
         delta(da->send_budget_yields, db->send_budget_yields),
         delta(da->stream_window_updates, db->stream_window_updates),
         da->min_stream_window_update, da->max_stream_window_update);
  printf("\"latency\":{\"samples\":%" PRIu64
         ",\"mean_us\":%.3f,\"max_us_lifetime\":%.3f,"
         "\"p50_bucket_upper_us\":",
         samples,
         samples
             ? delta(a->latency_sum_ns, b->latency_sum_ns) / (samples * 1000.0)
             : 0,
         a->latency_max_ns / 1000.0);
  print_percentile(hist, samples, 0.50);
  fputs(",\"p95_bucket_upper_us\":", stdout);
  print_percentile(hist, samples, 0.95);
  fputs(",\"p99_bucket_upper_us\":", stdout);
  print_percentile(hist, samples, 0.99);
  fputs(",\"histogram_log2_us\":[", stdout);
  for (size_t i = 0; i < 32; ++i)
    printf("%s%" PRIu64, i ? "," : "", hist[i]);
  fputs("]}}", stdout);
}

int main(int argc, char **argv) {
  options opts;
  int parsed = parse_options(argc, argv, &opts);
  if (parsed > 0)
    return 0;
  if (parsed < 0)
    return error_result("invalid arguments; see --help");
  corpus data = {0};
  if (!load_corpus(opts.input, &data)) {
    free_corpus(&data);
    return error_result("cannot load a nonempty JSONL regular file");
  }
  if (data.max_record_bytes > opts.per_buffer ||
      data.max_record_bytes > opts.batch_bytes - 64 ||
      data.max_record_bytes + 10 > opts.batch_bytes - 64) {
    free_corpus(&data);
    return error_result(
        "a record exceeds the configured per-stream buffer or batch budget");
  }
  if (opts.dry_run) {
    fputs("{\"schema_version\":2,\"ok\":true,\"dry_run\":true,", stdout);
    print_config(&opts, &data);
    puts("}");
    free_corpus(&data);
    return 0;
  }
  const char *token = getenv(opts.token_env);
  if (!token || !*token) {
    free_corpus(&data);
    return error_result("token environment variable is unset or empty");
  }
  zbp_runtime_config rc;
  zbp_runtime_config_init(&rc);
  rc.io_threads = opts.io_threads;
  rc.idle = opts.idle;
  rc.spin_us = opts.spin_us;
  rc.write_coalesce_bytes = opts.write_coalesce_bytes;
  zbp_runtime *runtime = NULL;
  zbp_status status = zbp_runtime_create(&rc, &runtime);
  zbp_connection **connections = calloc(opts.connections, sizeof(*connections));
  zbp_stream **streams = calloc(opts.streams, sizeof(*streams));
  if (!connections || !streams)
    status = ZBP_NOMEM;
  zbp_connection_config cc;
  zbp_connection_config_init(&cc);
  cc.host = opts.host;
  cc.port = (uint16_t)opts.port;
  cc.token = token;
  cc.ca_file = opts.ca_file;
  cc.test_plaintext = opts.plaintext;
  for (size_t i = 0; i < opts.connections && status == ZBP_OK; ++i)
    status = zbp_connection_open(runtime, &cc, &connections[i]);
  zbp_stream_config sc;
  zbp_stream_config_init(&sc);
  sc.queue = opts.queue;
  sc.batching = opts.batch == BATCH_AUTO ? ZBP_BATCH_AUTO : ZBP_BATCH_SINGLE;
  sc.retention = opts.retention;
  sc.queue_capacity = opts.per_queue;
  sc.buffer_bytes = opts.per_buffer;
  sc.max_inflight = opts.per_inflight;
  sc.batch_bytes = opts.batch_bytes;
  sc.batch_records = opts.batch_records;
  sc.batch_delay_us = opts.batch_delay_us;
  sc.admission_timeout_ms = opts.admission_timeout_ms;
  sc.sample_every = opts.sample_every;
  for (size_t i = 0; i < opts.streams && status == ZBP_OK; ++i)
    status = zbp_stream_open(connections[i % opts.connections], opts.table, &sc,
                             &streams[i]);
  if (status != ZBP_OK) {
    zbp_runtime_destroy(runtime);
    free(connections);
    free(streams);
    free_corpus(&data);
    return error_result(zbp_status_string(status));
  }
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  action.sa_handler = signal_handler;
  sigemptyset(&action.sa_mask);
  sigaction(SIGINT, &action, NULL);
  sigaction(SIGTERM, &action, NULL);
  phase p = {.opts = &opts,
             .data = &data,
             .streams = streams,
             .connections = connections};
  atomic_init(&p.stop, false);
  atomic_init(&p.failure, ZBP_OK);
  pthread_mutex_init(&p.mutex, NULL);
  pthread_condattr_t attr;
  pthread_condattr_init(&attr);
  pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
  pthread_cond_init(&p.cond, &attr);
  pthread_condattr_destroy(&attr);
  phase_result warmup, measured = {0};
  status = run_phase(&p, opts.warmup, &warmup);
  bool measured_started = status == ZBP_OK;
  if (measured_started)
    status = run_phase(&p, opts.duration, &measured);
  fputs("{\"schema_version\":2,\"dry_run\":false,\"ok\":", stdout);
  fputs(status == ZBP_OK ? "true," : "false,", stdout);
  print_config(&opts, &data);
  fputs(",\"status\":", stdout);
  json_string(zbp_status_string(status));
  printf(",\"warmup_acked_records\":%" PRIu64,
         delta(warmup.after.stream.acked_records,
               warmup.before.stream.acked_records));
  if (measured_started && measured.start_ns)
    print_phase(&measured);
  puts("}");
  if (status != ZBP_OK)
    fprintf(stderr, "benchmark failed: %s\n", zbp_status_string(status));
  for (size_t i = 0; i < opts.streams; ++i) {
    if (status == ZBP_OK)
      (void)zbp_stream_close(streams[i], 1000);
    else
      zbp_stream_abort(streams[i]);
  }
  zbp_runtime_destroy(runtime);
  pthread_cond_destroy(&p.cond);
  pthread_mutex_destroy(&p.mutex);
  free(connections);
  free(streams);
  free_corpus(&data);
  return status == ZBP_OK ? 0 : 1;
}
