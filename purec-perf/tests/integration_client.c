#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zbperf.h>

#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "check failed: %s:%d: %s\n", __FILE__, __LINE__, #x);    \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
#define OK(x)                                                                  \
  do {                                                                         \
    zbp_status result_ = (x);                                                  \
    if (result_ != ZBP_OK) {                                                   \
      fprintf(stderr, "%s: %s at %d\n", #x, zbp_status_string(result_),        \
              __LINE__);                                                       \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

typedef struct {
  zbp_stream *stream;
  pthread_barrier_t *start;
  int id, count, batch;
  int large_records;
  int tolerate_failure;
  zbp_status status;
  atomic_int done;
} producer;

static void *produce(void *arg) {
  producer *p = arg;
  char records[8][20000];
  zbp_record views[8];
  if (p->start)
    pthread_barrier_wait(p->start);
  for (int seq = 0; seq < p->count;) {
    int n = p->batch ? 8 : 1;
    if (n > p->count - seq)
      n = p->count - seq;
    for (int j = 0; j < n; ++j) {
      int size =
          snprintf(records[j], sizeof(records[j]),
                   "{\"producer\":%d,\"seq\":%d,\"pad\":\"", p->id, seq + j);
      int padding = p->large_records || (seq + j) % 101 == 0 ? 18000 : 17;
      memset(records[j] + size, 'x', (size_t)padding);
      size += padding;
      records[j][size++] = '"';
      records[j][size++] = '}';
      views[j] = (zbp_record){records[j], (size_t)size};
    }
    p->status = p->batch ? zbp_ingest_batch(p->stream, views, (size_t)n)
                         : zbp_ingest(p->stream, views[0].data, views[0].size);
    /* Successful admission owns a copy; overwriting catches borrowed inputs. */
    memset(records, 0xa5, sizeof(records));
    if (p->status != ZBP_OK) {
      CHECK(p->tolerate_failure);
      break;
    }
    seq += n;
  }
  atomic_store(&p->done, 1);
  return NULL;
}

static double now_seconds(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

int main(int argc, char **argv) {
  /* PORT TLS CA QUEUE BATCH RETENTION WORKERS CONNECTIONS SCENARIO [IDLE]
   * [COALESCE] */
  CHECK(argc >= 10 && argc <= 12);
  unsigned port = (unsigned)strtoul(argv[1], NULL, 10);
  int tls = atoi(argv[2]);
  int atomic_queue = atoi(argv[4]);
  int batch = atoi(argv[5]);
  int retain = atoi(argv[6]);
  int workers = atoi(argv[7]);
  int connections = atoi(argv[8]);
  const char *scenario = argv[9];
  CHECK(connections >= 1 && connections <= 4);
  zbp_runtime_config rc;
  zbp_runtime_config_init(&rc);
  rc.io_threads = (size_t)workers;
  if (argc >= 11)
    rc.idle = (zbp_idle_mode)atoi(argv[10]);
  if (argc >= 12)
    rc.write_coalesce_bytes = (size_t)strtoul(argv[11], NULL, 10);
  zbp_runtime *runtime;
  OK(zbp_runtime_create(&rc, &runtime));
  zbp_connection *conns[4];
  zbp_stream *streams[4];
  for (int c = 0; c < connections; ++c) {
    zbp_connection_config cc;
    zbp_connection_config_init(&cc);
    cc.host = "localhost";
    cc.port = (uint16_t)port;
    cc.ca_file = tls ? argv[3] : NULL;
    cc.test_plaintext = !tls;
    cc.token = "local-fixture-token";
    cc.connect_timeout_ms = 3000;
    zbp_status status = zbp_connection_open(runtime, &cc, &conns[c]);
    if (strcmp(scenario, "reject_tls") == 0) {
      CHECK(status != ZBP_OK);
      zbp_runtime_destroy(runtime);
      return 0;
    }
    OK(status);
  }
  for (int s = 0; s < 4; ++s) {
    zbp_stream_config sc;
    zbp_stream_config_init(&sc);
    sc.queue = atomic_queue ? ZBP_QUEUE_ATOMIC : ZBP_QUEUE_MUTEX;
    sc.batching = batch == 1 ? ZBP_BATCH_AUTO : ZBP_BATCH_SINGLE;
    sc.retention = retain ? ZBP_RELEASE_ACKED : ZBP_RELEASE_SENT;
    sc.queue_capacity = 32;
    sc.max_inflight = 32;
    sc.buffer_bytes = 1024 * 1024;
    sc.batch_bytes = 65536;
    sc.batch_records = 32;
    sc.batch_delay_us = batch == 1 ? 250 : 0;
    if (!strcmp(scenario, "flush_delay")) {
      sc.batching = ZBP_BATCH_AUTO;
      sc.batch_delay_us = 5000000;
    }
    sc.admission_timeout_ms = 5000;
    sc.sample_every = 1;
    if (!strcmp(scenario, "full_duplex")) {
      sc.queue_capacity = 128;
      sc.max_inflight = 128;
      sc.buffer_bytes = 8 * 1024 * 1024;
    }
    OK(zbp_stream_open(conns[s % connections], "test.default.events", &sc,
                       &streams[s]));
  }
  if (!strcmp(scenario, "flush_delay") || !strcmp(scenario, "stalled_flush")) {
    OK(zbp_ingest(streams[0], "{\"producer\":0,\"seq\":0}", 22));
    OK(zbp_flush(streams[0], 1500));
    /* An empty barrier must also complete with no remaining send window. */
    OK(zbp_flush(streams[0], 1500));
    zbp_runtime_destroy(runtime);
    return 0;
  }
  if (!strcmp(scenario, "late_ack")) {
    OK(zbp_ingest(streams[0], "{\"producer\":0,\"seq\":0}", 22));
    CHECK(zbp_flush(streams[0], 100) == ZBP_TIMEOUT);
    OK(zbp_ingest(streams[0], "{\"producer\":0,\"seq\":1}", 22));
    OK(zbp_flush(streams[0], 10000));
    zbp_stream_stats st;
    zbp_stream_get_stats(streams[0], &st);
    CHECK(st.acked_records == 2);
    zbp_runtime_destroy(runtime);
    return 0;
  }
  if (!strcmp(scenario, "timeout") || !strcmp(scenario, "flow_stall")) {
    OK(zbp_ingest(streams[0], "{\"producer\":0,\"seq\":0}", 22));
    CHECK(zbp_flush(streams[0], 100) == ZBP_TIMEOUT);
    zbp_stream_abort(streams[0]);
    CHECK(zbp_flush(streams[0], 100) != ZBP_OK);
    zbp_runtime_destroy(runtime);
    return 0;
  }
  if (!strcmp(scenario, "abort")) {
    producer p = {
        .stream = streams[0], .id = 0, .count = 100000, .tolerate_failure = 1};
    atomic_init(&p.done, 0);
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, produce, &p) == 0);
    double deadline = now_seconds() + 4;
    for (;;) {
      zbp_stream_stats st;
      zbp_stream_get_stats(streams[0], &st);
      if (st.backpressure_waits)
        break;
      CHECK(!atomic_load(&p.done));
      CHECK(now_seconds() < deadline);
      sched_yield();
    }
    zbp_stream_abort(streams[0]);
    CHECK(pthread_join(thread, NULL) == 0);
    CHECK(p.status != ZBP_OK);
    CHECK(zbp_flush(streams[0], 100) != ZBP_OK);
    zbp_runtime_destroy(runtime);
    return 0;
  }
  const int fail_expected =
      !strcmp(scenario, "reset") || !strcmp(scenario, "future_ack") ||
      !strcmp(scenario, "bad_trailer") || !strcmp(scenario, "truncated");
  pthread_barrier_t start;
  CHECK(pthread_barrier_init(&start, NULL, 9) == 0);
  pthread_t threads[8];
  producer producers[8];
  for (int i = 0; i < 8; ++i) {
    producers[i] = (producer){.stream = streams[i % 4],
                              .start = &start,
                              .id = i,
                              .count = 250,
                              .batch = batch == 2,
                              .large_records = !strcmp(scenario, "full_duplex"),
                              .tolerate_failure = fail_expected};
    atomic_init(&producers[i].done, 0);
    CHECK(pthread_create(&threads[i], NULL, produce, &producers[i]) == 0);
  }
  pthread_barrier_wait(&start);
  if (!fail_expected) {
    /* A flush overlapping producers exercises queue barrier ordering. */
    for (int s = 0; s < 4; ++s)
      OK(zbp_flush(streams[s], 10000));
  }
  for (int i = 0; i < 8; ++i)
    CHECK(pthread_join(threads[i], NULL) == 0);
  pthread_barrier_destroy(&start);
  for (int s = 0; s < 4; ++s) {
    zbp_status status = zbp_flush(streams[s], 10000);
    if (fail_expected) {
      CHECK(status != ZBP_OK);
      zbp_stream_abort(streams[s]);
      continue;
    }
    OK(status);
    OK(zbp_stream_close(streams[s], 10000));
    zbp_stream_stats st;
    zbp_stream_get_stats(streams[s], &st);
    CHECK(st.admitted_records == 500 && st.acked_records == 500);
    CHECK(st.admitted_bytes == st.acked_bytes);
    CHECK(st.latency_samples > 0);
    CHECK(zbp_ingest(streams[s], "{}", 2) != ZBP_OK);
  }
  zbp_runtime_destroy(runtime);
  puts("integration client passed");
  return 0;
}
