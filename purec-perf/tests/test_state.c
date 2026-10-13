#define _GNU_SOURCE
#include "queue.h"
#include <assert.h>
#include <openssl/ssl.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Interpose external boundaries only; exercise the actual SDK state machine. */
static void *scripted_queue_peek(zbp_queue *queue);
static int scripted_ssl_write(SSL *ssl, const void *data, int size);
static int scripted_ssl_read(SSL *ssl, void *data, int size);
static int scripted_ssl_error(const SSL *ssl, int result);
static void *scripted_malloc(size_t size);
static void scripted_free(void *pointer);
static int scripted_cond_broadcast(pthread_cond_t *cond);
#define zbp_queue_peek scripted_queue_peek
#define SSL_write scripted_ssl_write
#define SSL_read scripted_ssl_read
#define SSL_get_error scripted_ssl_error
#define malloc scripted_malloc
#define free scripted_free
#define pthread_cond_broadcast scripted_cond_broadcast
#include "../src/sdk.c"
#undef zbp_queue_peek
#undef SSL_write
#undef SSL_read
#undef SSL_get_error
#undef malloc
#undef free
#undef pthread_cond_broadcast

static int track_allocations;
static _Atomic int fail_allocation;
static _Atomic unsigned allocations, frees, capacity_broadcasts;
static pthread_cond_t *observed_cond;

static void *scripted_malloc(size_t size) {
  if (atomic_exchange(&fail_allocation, 0)) {
    return NULL;
  }
  void *pointer = malloc(size);
  if (track_allocations && pointer)
    ++allocations;
  return pointer;
}

static void scripted_free(void *pointer) {
  if (track_allocations && pointer)
    ++frees;
  free(pointer);
}

static int scripted_cond_broadcast(pthread_cond_t *cond) {
  if (cond == observed_cond)
    atomic_fetch_add_explicit(&capacity_broadcasts, 1, memory_order_relaxed);
  return pthread_cond_broadcast(cond);
}

typedef struct {
  zbp_stream stream;
  zbp_connection connection;
  worker owner;
  request descriptors[8];
  outstanding in_flight[2];
} admission_fixture;

static void admission_fixture_init(admission_fixture *fixture,
                                    zbp_queue_mode mode) {
  memset(fixture, 0, sizeof(*fixture));
  zbp_stream *s = &fixture->stream;
  zbp_stream_config_init(&s->config);
  s->config.buffer_bytes = 1025;
  s->config.queue_capacity = 4;
  s->config.max_inflight = 2;
  s->config.admission_timeout_ms = 0;
  s->descriptors = fixture->descriptors;
  s->descriptor_count = 8;
  s->outstanding = fixture->in_flight;
  s->connection = &fixture->connection;
  fixture->connection.worker = &fixture->owner;
  atomic_init(&fixture->connection.streams, s);
  atomic_init(&fixture->owner.notified, 1);
  atomic_init(&s->accepting, 1);
  atomic_init(&s->ready, 1);
  atomic_init(&s->free_descriptors, 1);
  for (unsigned i = 0; i < 8; ++i) {
    fixture->descriptors[i].index = i;
    atomic_init(&fixture->descriptors[i].free_next, i == 7 ? 0 : i + 2);
  }
  assert(!pthread_mutex_init(&s->wait_mutex, NULL));
  assert(init_cond(&s->wait_cond));
  assert(zbp_queue_init(&s->queue, 4, mode));
  allocations = frees = 0;
  track_allocations = 1;
}

static void admission_fixture_destroy(admission_fixture *fixture) {
  zbp_stream *s = &fixture->stream;
  stream_fail(s, ZBP_CLOSED);
  stream_cleanup(s);
  assert(!atomic_load(&s->buffered_bytes));
  assert(allocations == frees);
  unsigned descriptors = 0;
  while (descriptor_get(s))
    ++descriptors;
  assert(descriptors == s->descriptor_count);
  track_allocations = 0;
  zbp_queue_destroy(&s->queue);
  pthread_cond_destroy(&s->wait_cond);
  pthread_mutex_destroy(&s->wait_mutex);
}

static void payload_budget_and_failure(zbp_queue_mode mode) {
  admission_fixture fixture;
  admission_fixture_init(&fixture, mode);
  zbp_stream *s = &fixture.stream;
  unsigned char bytes[1026];
  memset(bytes, 'x', sizeof(bytes));
  fail_allocation = 1;
  assert(zbp_ingest(s, bytes, 513) == ZBP_NOMEM);
  assert(!atomic_load(&s->buffered_bytes));
  assert(!atomic_load(&s->admitted_records));
  assert(!atomic_load(&s->admissions));
  assert(!atomic_load(&s->queue.count));
  assert(zbp_ingest(s, bytes, 513) == ZBP_OK);
  assert(zbp_ingest(s, bytes, 512) == ZBP_OK);
  assert(atomic_load(&s->buffered_bytes) == 1025);
  assert(zbp_ingest(s, bytes, 1) == ZBP_TIMEOUT);
  assert(allocations == 2);
  request *first = zbp_queue_pop(&s->queue);
  assert(first && first->size == 513);
  assert(!memcmp(first->payload, bytes, first->size));
  request_put(s, first);
  assert(zbp_ingest(s, bytes, 513) == ZBP_OK);
  assert(atomic_load(&s->buffered_bytes) == 1025);
  assert(atomic_load(&s->peak_buffered_bytes) == 1025);
  assert(zbp_ingest(s, bytes, 1026) == ZBP_LIMIT);
  admission_fixture_destroy(&fixture);
}

static void payload_retention_and_teardown(zbp_queue_mode mode,
                                            zbp_retention retention,
                                            int phase) {
  admission_fixture fixture;
  admission_fixture_init(&fixture, mode);
  zbp_stream *s = &fixture.stream;
  s->config.retention = retention;
  unsigned char bytes[513], encoded[1024];
  memset(bytes, 'x', sizeof(bytes));
  const zbp_record records[] = {{bytes, 257}, {bytes, 256}};
  assert(zbp_ingest_batch(s, records, 2) == ZBP_OK);
  size_t payload_size = 513 + 2 * (1 + zbp_varint_size(256));
  assert(atomic_load(&s->buffered_bytes) == payload_size);
  nghttp2_data_source source = {.ptr = s};
  uint32_t flags = 0;
  assert(data_read(NULL, 1, encoded, phase == 1 ? 10 : sizeof(encoded), &flags,
                   &source, NULL) > 0);
  assert(atomic_load(&s->buffered_bytes) ==
         (phase == 1 || retention == ZBP_RELEASE_ACKED ? payload_size : 0));
  if (!phase) {
    unsigned char ack[] = {18, 2, 8, 0};
    s->rx = ack;
    s->rx_size = sizeof(ack);
    assert(receive_message(s));
    s->rx = NULL;
    assert(!atomic_load(&s->buffered_bytes));
    assert(atomic_load(&s->acked_records) == 2);
  }
  admission_fixture_destroy(&fixture);
}

static void capacity_notifications(zbp_queue_mode mode,
                                     zbp_retention retention, unsigned reason) {
  admission_fixture fixture;
  admission_fixture_init(&fixture, mode);
  zbp_stream *s = &fixture.stream;
  s->config.retention = retention;
  unsigned char bytes[512], encoded[2048], header[64];
  memset(bytes, 'x', sizeof(bytes));
  assert(zbp_ingest(s, bytes, sizeof(bytes)) == ZBP_OK);
  assert(zbp_ingest(s, bytes, sizeof(bytes)) == ZBP_OK);
  atomic_store(reason == CAPACITY_ALLOCATION ? &s->allocation_waiters
                                             : &s->queue_waiters, 1);
  atomic_store(&s->waiters, 1);
  atomic_store(&capacity_broadcasts, 0);
  observed_cond = &s->wait_cond;

  nghttp2_data_source source = {.ptr = s};
  uint32_t flags = 0;
  for (uint64_t offset = 0; offset < 2; ++offset) {
    size_t wire_size = zbp_wire_header(header, offset, sizeof(bytes), 0) +
                       sizeof(bytes);
    assert(data_read(NULL, 1, encoded, wire_size, &flags, &source, NULL) ==
           (ssize_t)wire_size);
  }
  assert(s->sent_plus_one == 2);
  assert(!atomic_load(&capacity_broadcasts));
  flush_capacity_notifications(s->connection);
  unsigned expected = reason == CAPACITY_QUEUE || retention == ZBP_RELEASE_SENT;
  assert(atomic_load(&capacity_broadcasts) == expected);
  assert(!s->pending_capacity);
  unsigned char ack[] = {18, 2, 8, 0};
  s->rx = ack;
  s->rx_size = sizeof(ack);
  assert(receive_message(s));
  ack[3] = 1;
  assert(receive_message(s));
  assert(!s->out_count && !atomic_load(&s->buffered_bytes));
  assert(atomic_load(&capacity_broadcasts) == expected);
  flush_capacity_notifications(s->connection);
  expected += reason == CAPACITY_ALLOCATION && retention == ZBP_RELEASE_ACKED;
  assert(atomic_load(&capacity_broadcasts) == expected);
  assert(!s->pending_capacity);
  assert(receive_message(s));
  flush_capacity_notifications(s->connection);
  assert(atomic_load(&capacity_broadcasts) == expected);
  s->rx = NULL;
  observed_cond = NULL;
  atomic_store(&s->allocation_waiters, 0);
  atomic_store(&s->queue_waiters, 0);
  atomic_store(&s->waiters, 0);
  admission_fixture_destroy(&fixture);
}

typedef struct {
  zbp_stream *stream;
  const void *bytes;
  size_t size;
  zbp_status result;
} blocked_admission;

static void *admit_when_capacity_returns(void *argument) {
  blocked_admission *admission = argument;
  admission->result = zbp_ingest(admission->stream, admission->bytes,
                                  admission->size);
  return NULL;
}

static void await_capacity_wait(zbp_stream *s, unsigned reason) {
  _Atomic unsigned *waiters = reason == CAPACITY_ALLOCATION
                                  ? &s->allocation_waiters : &s->queue_waiters;
  uint64_t deadline = deadline_ms(2000);
  while (!atomic_load_explicit(waiters, memory_order_acquire)) {
    assert(now_ns() < deadline);
    sched_yield();
  }
  /* Acquiring this mutex proves the producer reached its condition wait. */
  pthread_mutex_lock(&s->wait_mutex);
  assert(atomic_load(waiters) == 1);
  pthread_mutex_unlock(&s->wait_mutex);
}

static void assert_no_waiters(zbp_stream *s) {
  assert(!atomic_load(&s->waiters));
  assert(!atomic_load(&s->allocation_waiters));
  assert(!atomic_load(&s->queue_waiters));
}

static void blocked_producer_resumes(zbp_queue_mode mode,
                                       zbp_retention retention) {
  admission_fixture fixture;
  admission_fixture_init(&fixture, mode);
  zbp_stream *s = &fixture.stream;
  s->config.retention = retention;
  s->config.buffer_bytes = 512;
  s->config.admission_timeout_ms = 2000;
  unsigned char bytes[512], encoded[1024], header[64];
  memset(bytes, 'x', sizeof(bytes));
  assert(zbp_ingest(s, bytes, sizeof(bytes)) == ZBP_OK);
  blocked_admission admission = {s, bytes, 256, ZBP_TIMEOUT};
  pthread_t thread;
  observed_cond = &s->wait_cond;
  atomic_store(&capacity_broadcasts, 0);
  assert(!pthread_create(&thread, NULL, admit_when_capacity_returns, &admission));
  await_capacity_wait(s, CAPACITY_ALLOCATION);
  assert(atomic_load(&s->buffered_bytes) == 512);

  nghttp2_data_source source = {.ptr = s};
  uint32_t flags = 0;
  size_t wire_size = zbp_wire_header(header, 0, sizeof(bytes), 0) + sizeof(bytes);
  assert(data_read(NULL, 1, encoded, wire_size, &flags, &source, NULL) ==
         (ssize_t)wire_size);
  assert(!atomic_load(&capacity_broadcasts));
  flush_capacity_notifications(s->connection);
  assert(atomic_load(&capacity_broadcasts) == (retention == ZBP_RELEASE_SENT));
  unsigned char ack[] = {18, 2, 8, 0};
  s->rx = ack;
  s->rx_size = sizeof(ack);
  assert(receive_message(s));
  s->rx = NULL;
  flush_capacity_notifications(s->connection);
  assert(!pthread_join(thread, NULL));
  assert(admission.result == ZBP_OK);
  assert(atomic_load(&s->admitted_records) == 2);
  assert(atomic_load(&s->buffered_bytes) == 256);
  assert(atomic_load(&capacity_broadcasts) == 1);
  observed_cond = NULL;
  assert_no_waiters(s);
  admission_fixture_destroy(&fixture);
}

static void queue_waiter_resumes_before_ack(zbp_queue_mode mode,
                                             zbp_retention retention) {
  admission_fixture fixture;
  admission_fixture_init(&fixture, mode);
  zbp_stream *s = &fixture.stream;
  s->config.retention = retention;
  s->config.buffer_bytes = 2048;
  s->config.admission_timeout_ms = 2000;
  unsigned char bytes[128], encoded[256], header[64];
  memset(bytes, 'x', sizeof(bytes));
  for (unsigned i = 0; i < 4; ++i)
    assert(zbp_ingest(s, bytes, sizeof(bytes)) == ZBP_OK);
  blocked_admission admission = {s, bytes, sizeof(bytes), ZBP_TIMEOUT};
  pthread_t thread;
  observed_cond = &s->wait_cond;
  atomic_store(&capacity_broadcasts, 0);
  assert(!pthread_create(&thread, NULL, admit_when_capacity_returns, &admission));
  await_capacity_wait(s, CAPACITY_QUEUE);
  assert(!atomic_load(&s->allocation_waiters));
  nghttp2_data_source source = {.ptr = s};
  uint32_t flags = 0;
  size_t wire_size = zbp_wire_header(header, 0, sizeof(bytes), 0) + sizeof(bytes);
  assert(data_read(NULL, 1, encoded, wire_size, &flags, &source, NULL) ==
         (ssize_t)wire_size);
  assert(!atomic_load(&capacity_broadcasts));
  flush_capacity_notifications(s->connection);
  assert(!pthread_join(thread, NULL));
  assert(admission.result == ZBP_OK);
  assert(atomic_load(&s->admitted_records) == 5);
  assert(!atomic_load(&s->acked_records));
  assert(atomic_load(&s->queue.count) == 4);
  assert(atomic_load(&capacity_broadcasts) == 1);
  observed_cond = NULL;
  assert_no_waiters(s);
  admission_fixture_destroy(&fixture);
}

static void barrier_descriptor_wakes_allocator(zbp_queue_mode mode) {
  admission_fixture fixture;
  admission_fixture_init(&fixture, mode);
  zbp_stream *s = &fixture.stream;
  s->config.admission_timeout_ms = 2000;
  request *held[8];
  for (unsigned i = 0; i < 8; ++i) {
    held[i] = descriptor_get(s);
    assert(held[i]);
  }
  atomic_store(&held[0]->references, 2);
  const unsigned char bytes[] = "{}";
  blocked_admission admission = {s, bytes, 2, ZBP_TIMEOUT};
  pthread_t thread;
  observed_cond = &s->wait_cond;
  atomic_store(&capacity_broadcasts, 0);
  assert(!pthread_create(&thread, NULL, admit_when_capacity_returns, &admission));
  await_capacity_wait(s, CAPACITY_ALLOCATION);
  barrier_release(s, held[0]);
  assert(!atomic_load(&capacity_broadcasts));
  barrier_release(s, held[0]);
  assert(!pthread_join(thread, NULL));
  assert(admission.result == ZBP_OK);
  assert(atomic_load(&capacity_broadcasts) == 1);
  observed_cond = NULL;
  for (unsigned i = 1; i < 8; ++i)
    descriptor_put(s, held[i]);
  assert_no_waiters(s);
  admission_fixture_destroy(&fixture);
}

static void capacity_wait_ends(zbp_queue_mode mode, unsigned reason, int abort) {
  admission_fixture fixture;
  admission_fixture_init(&fixture, mode);
  zbp_stream *s = &fixture.stream;
  unsigned char bytes[128];
  memset(bytes, 'x', sizeof(bytes));
  if (reason == CAPACITY_ALLOCATION)
    s->config.buffer_bytes = sizeof(bytes);
  for (unsigned i = 0; i < (reason == CAPACITY_ALLOCATION ? 1u : 4u); ++i)
    assert(zbp_ingest(s, bytes, sizeof(bytes)) == ZBP_OK);
  s->config.admission_timeout_ms = abort ? 2000 : 10;
  blocked_admission admission = {s, bytes, sizeof(bytes), ZBP_OK};
  if (abort) {
    pthread_t thread;
    assert(!pthread_create(&thread, NULL, admit_when_capacity_returns, &admission));
    await_capacity_wait(s, reason);
    zbp_stream_abort(s);
    assert(!pthread_join(thread, NULL));
    assert(admission.result == ZBP_CLOSED);
  } else {
    assert(zbp_ingest(s, bytes, sizeof(bytes)) == ZBP_TIMEOUT);
  }
  assert_no_waiters(s);
  admission_fixture_destroy(&fixture);
}

static zbp_queue *injection_queue;
static request *injection_request;
static unsigned injected;

static void *scripted_queue_peek(zbp_queue *queue) {
  void *head = zbp_queue_peek(queue);
  if (queue == injection_queue && !head && injection_request) {
    assert(zbp_queue_push(queue, injection_request));
    injection_request = NULL;
    ++injected;
  }
  return head;
}

static void barrier_between_drain_and_peek(zbp_queue_mode mode,
                                           int prior_unacknowledged) {
  zbp_stream stream = {0};
  request barrier = {0};
  outstanding in_flight[2] = {{0}};
  zbp_stream_config_init(&stream.config);
  stream.config.max_inflight = 2;
  stream.outstanding = in_flight;
  stream.next_offset = (uint64_t)prior_unacknowledged;
  stream.sent_plus_one = stream.next_offset;
  stream.out_count = (size_t)prior_unacknowledged;
  assert(zbp_queue_init(&stream.queue, 4, mode));
  barrier.barrier = 1;
  atomic_init(&barrier.references, 2);
  atomic_init(&barrier.done, 0);
  atomic_init(&stream.barriers_pending, 1);
  injection_queue = &stream.queue;
  injection_request = &barrier;
  injected = 0;

  assert(start_request(&stream) == 0);
  assert(injected == 1 && !injection_request);
  assert(!stream.sending && !stream.sending_cursor);
  assert(!atomic_load(&stream.wire_requests));
  assert(stream.next_offset == (uint64_t)prior_unacknowledged);
  assert(stream.out_count == (size_t)prior_unacknowledged);
  assert(!atomic_load(&stream.queue.count));
  if (prior_unacknowledged) {
    assert(!atomic_load(&barrier.done));
    assert(stream.barrier_head == &barrier);
    assert(barrier.target == 1);
    stream.ack_plus_one = 1;
    complete_barriers(&stream);
  }
  assert(atomic_load(&barrier.done));
  assert(barrier.result == ZBP_OK);
  assert(atomic_load(&barrier.references) == 1);
  assert(!atomic_load(&stream.barriers_pending));
  assert(!stream.barrier_head && !stream.barrier_tail);
  injection_queue = NULL;
  zbp_queue_destroy(&stream.queue);
}

typedef struct {
  int write_error, read_error, last_error;
  unsigned writes, reads;
  int write_retry, read_retry;
  const void *write_buffer;
  void *read_buffer;
  int write_size, read_size;
  unsigned char write_bytes[65536];
  unsigned char incoming[128];
  size_t incoming_size;
} tls_script;

static tls_script script;

static int scripted_ssl_write(SSL *ssl, const void *data, int size) {
  assert(ssl && data && size > 0);
  assert(!script.read_retry);
  assert((size_t)size <= sizeof(script.write_bytes));
  ++script.writes;
  if (script.write_retry) {
    assert(data == script.write_buffer);
    assert(size == script.write_size);
    assert(!memcmp(data, script.write_bytes, (size_t)size));
  }
  if (script.write_error) {
    if (!script.write_retry) {
      script.write_buffer = data;
      script.write_size = size;
      memcpy(script.write_bytes, data, (size_t)size);
    }
    script.write_retry = 1;
    script.last_error = script.write_error;
    return -1;
  }
  script.write_retry = 0;
  return size;
}

static int scripted_ssl_read(SSL *ssl, void *data, int size) {
  assert(ssl && data && size > 0);
  assert(!(script.write_retry && script.write_error == SSL_ERROR_WANT_READ));
  ++script.reads;
  if (script.read_retry) {
    assert(data == script.read_buffer);
    assert(size == script.read_size);
  }
  if (script.incoming_size) {
    assert((size_t)size >= script.incoming_size);
    memcpy(data, script.incoming, script.incoming_size);
    int read = (int)script.incoming_size;
    script.incoming_size = 0;
    script.read_retry = 0;
    return read;
  }
  script.last_error = script.read_error;
  script.read_retry = script.read_error == SSL_ERROR_WANT_WRITE;
  if (script.read_retry) {
    script.read_buffer = data;
    script.read_size = size;
  }
  return -1;
}

static int scripted_ssl_error(const SSL *ssl, int result) {
  assert(ssl && result == -1);
  return script.last_error;
}

typedef struct {
  zbp_runtime runtime;
  worker owner;
  zbp_connection connection;
} connection_fixture;

static void fixture_init(connection_fixture *fixture, size_t coalesce) {
  memset(fixture, 0, sizeof(*fixture));
  memset(&script, 0, sizeof(script));
  script.read_error = SSL_ERROR_WANT_READ;
  zbp_runtime_config_init(&fixture->runtime.config);
  fixture->runtime.config.write_coalesce_bytes = coalesce;
  fixture->owner.runtime = &fixture->runtime;
  zbp_connection *c = &fixture->connection;
  c->worker = &fixture->owner;
  c->fd = -1;
  c->input = malloc(65536);
  assert(c->input);
  if (coalesce) {
    c->output_capacity = coalesce;
    c->output = malloc(coalesce);
    assert(c->output);
  }
  assert(!pthread_mutex_init(&c->wait_mutex, NULL));
  assert(init_cond(&c->wait_cond));
  /* All SSL entry points used in established state are scripted above. */
  c->ssl = malloc(1);
  assert(c->ssl);
  assert(initialize_h2(c));
  /* A real peer's first frame must be SETTINGS. */
  static const unsigned char settings[] = {0, 0, 0, 4, 0, 0, 0, 0, 0};
  memcpy(script.incoming, settings, sizeof(settings));
  script.incoming_size = sizeof(settings);
}

static void fixture_destroy(connection_fixture *fixture) {
  zbp_connection *c = &fixture->connection;
  nghttp2_session_del(c->h2);
  free(c->ssl);
  pthread_cond_destroy(&c->wait_cond);
  pthread_mutex_destroy(&c->wait_mutex);
  free(c->input);
  free(c->output);
}

static void feed_window_update(void) {
  /* Stream zero increases the connection's send window by 4096 bytes. */
  static const unsigned char frame[] = {0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 0, 16, 0};
  assert(!script.incoming_size);
  memcpy(script.incoming, frame, sizeof(frame));
  script.incoming_size = sizeof(frame);
}

static void write_retry_allows_reads(size_t coalesce) {
  connection_fixture fixture;
  fixture_init(&fixture, coalesce);
  zbp_connection *c = &fixture.connection;
  script.write_error = SSL_ERROR_WANT_WRITE;
  service_connection(c);
  assert(atomic_load(&c->status) == ZBP_OK);
  assert(c->write_pending && !c->write_want_read);
  assert(script.write_retry && !script.incoming_size);
  int32_t window_before = nghttp2_session_get_remote_window_size(c->h2);
  unsigned reads_before = script.reads, writes_before = script.writes;
  feed_window_update();

  service_connection(c);
  assert(atomic_load(&c->status) == ZBP_OK);
  assert(script.writes > writes_before);
  assert(script.reads > reads_before);
  assert(!script.incoming_size);
  assert(nghttp2_session_get_remote_window_size(c->h2) == window_before + 4096);
  assert(c->write_pending && script.write_retry);
  if (coalesce) {
    assert(c->retry_size == (size_t)script.write_size);
    assert(c->output + c->output_start == script.write_buffer);
  }

  /* A read needing handshake output must be retried before the blocked write.
   */
  script.read_error = SSL_ERROR_WANT_WRITE;
  service_connection(c);
  assert(c->tls_read_want_write && script.read_retry && c->write_pending);
  writes_before = script.writes;
  reads_before = script.reads;
  service_connection(c);
  assert(script.reads > reads_before && script.writes == writes_before);
  script.read_error = SSL_ERROR_WANT_READ;
  script.write_error = 0;
  service_connection(c);
  assert(atomic_load(&c->status) == ZBP_OK);
  assert(!c->write_pending && !c->tls_read_want_write);
  assert(!script.write_retry && !script.read_retry);
  assert(c->output_start == c->output_end);
  fixture_destroy(&fixture);
}

static void write_want_read_retries_write(size_t coalesce) {
  connection_fixture fixture;
  fixture_init(&fixture, coalesce);
  zbp_connection *c = &fixture.connection;
  script.write_error = SSL_ERROR_WANT_READ;
  service_connection(c);
  assert(c->write_pending && c->write_want_read);
  unsigned reads_before = script.reads, writes_before = script.writes;
  feed_window_update();
  service_connection(c);
  assert(atomic_load(&c->status) == ZBP_OK);
  assert(script.reads == reads_before && script.writes > writes_before);
  assert(script.incoming_size && script.write_retry);
  script.write_error = 0;
  service_connection(c);
  assert(atomic_load(&c->status) == ZBP_OK);
  assert(!c->write_pending && !c->write_want_read);
  assert(!script.incoming_size && script.reads > reads_before);
  fixture_destroy(&fixture);
}

static void capacity_flush_on_early_return(unsigned retry_kind) {
  connection_fixture connection;
  fixture_init(&connection, 4096);
  zbp_connection *c = &connection.connection;
  script.write_error = retry_kind == 0 ? SSL_ERROR_WANT_READ
                                       : SSL_ERROR_WANT_WRITE;
  service_connection(c);
  assert(c->write_pending);
  if (retry_kind == 1) {
    script.read_error = SSL_ERROR_WANT_WRITE;
    service_connection(c);
    assert(c->tls_read_want_write);
  }

  admission_fixture admission;
  admission_fixture_init(&admission, ZBP_QUEUE_ATOMIC);
  zbp_stream *s = &admission.stream;
  s->connection = c;
  atomic_store(&c->streams, s);
  atomic_store(&s->allocation_waiters, 1);
  atomic_store(&s->waiters, 1);
  s->pending_capacity = CAPACITY_ALLOCATION;
  observed_cond = &s->wait_cond;
  atomic_store(&capacity_broadcasts, 0);
  if (retry_kind == 2)
    atomic_store(&c->status, ZBP_IO);
  service_connection(c);
  assert(!s->pending_capacity);
  assert(atomic_load(&capacity_broadcasts) >= 1);
  if (retry_kind != 2)
    assert(atomic_load(&capacity_broadcasts) == 1);
  observed_cond = NULL;
  atomic_store(&s->allocation_waiters, 0);
  atomic_store(&s->waiters, 0);
  atomic_store(&c->streams, NULL);
  admission_fixture_destroy(&admission);
  fixture_destroy(&connection);
}

static void transport_diagnostics(void) {
  connection_fixture fixture;
  fixture_init(&fixture, 4096);
  zbp_connection *c = &fixture.connection;
  service_connection(c);
  zbp_connection_diagnostics before, after;
  zbp_connection_get_diagnostics(c, &before);
  assert(before.peer_initial_stream_window == 65535);
  assert(before.peer_max_frame_size == 16384);
  assert(before.remote_connection_window == 65535);
  assert(before.peak_remote_connection_window == 65535);
  assert(before.min_remote_stream_window == 0);
  feed_window_update();
  service_connection(c);
  zbp_connection_get_diagnostics(c, &after);
  assert(after.remote_connection_window ==
         before.remote_connection_window + 4096);
  assert(after.peak_remote_connection_window ==
         before.peak_remote_connection_window + 4096);
  nghttp2_frame frame = {0};
  frame.hd.type = NGHTTP2_WINDOW_UPDATE;
  frame.hd.stream_id = 1;
  frame.window_update.window_size_increment = 1024 * 1024;
  assert(!on_frame(c->h2, &frame, c));
  frame.window_update.window_size_increment = 512 * 1024;
  assert(!on_frame(c->h2, &frame, c));
  zbp_connection_get_diagnostics(c, &after);
  assert(after.stream_window_updates == 2);
  assert(after.min_stream_window_update == 512 * 1024);
  assert(after.max_stream_window_update == 1024 * 1024);

  record_flow_stall(c, 1);
  zbp_connection_get_diagnostics(c, &before);
  record_flow_stall(c, 0);
  zbp_connection_get_diagnostics(c, &after);
  assert(after.flow_control_ns >= before.flow_control_ns);
  assert(atomic_load(&c->flow_control_stalls) == 1);
  assert(!atomic_load(&c->flow_stall_since_ns));

  c->turn_bytes = 1024 * 1024;
  const unsigned char payload[] = "x";
  assert(send_callback(c->h2, payload, 1, 0, c) == NGHTTP2_ERR_WOULDBLOCK);
  c->turn_bytes = 0;
  script.write_error = SSL_ERROR_WANT_WRITE;
  assert(transport_write(c, payload, 1) == -2);
  script.write_error = SSL_ERROR_WANT_READ;
  assert(transport_write(c, payload, 1) == -2);
  zbp_connection_get_diagnostics(c, &after);
  assert(after.send_budget_yields == 1);
  assert(after.write_would_block == 1 && after.write_want_read == 1);
  zbp_connection_get_diagnostics(NULL, &after);
  assert(after.peer_initial_stream_window == 0 && after.flow_control_ns == 0);
  zbp_connection_get_diagnostics(c, NULL);
  fixture_destroy(&fixture);
}

static _Atomic int diagnostics_finished;

static void set_peer_stream_window(zbp_connection *c, uint32_t size) {
  unsigned char settings[] = {0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 4,
                              (unsigned char)(size >> 24),
                              (unsigned char)(size >> 16),
                              (unsigned char)(size >> 8),
                              (unsigned char)size};
  assert(nghttp2_session_mem_recv(c->h2, settings, sizeof(settings)) ==
         (ssize_t)sizeof(settings));
}

static void flow_stall_predicate(void) {
  connection_fixture fixture;
  fixture_init(&fixture, 4096);
  zbp_connection *c = &fixture.connection;
  c->host = "localhost";
  c->token = "fixture-token";
  c->port = 443;
  zbp_stream stream = {0};
  request pending = {0};
  stream.connection = c;
  stream.table = "test.default.events";
  zbp_stream_config_init(&stream.config);
  assert(zbp_queue_init(&stream.queue, 4, ZBP_QUEUE_MUTEX));
  atomic_init(&stream.ready, 1);
  atomic_store(&c->streams, &stream);
  service_connection(c);
  assert(stream.submitted && stream.h2_id > 0);
  assert(nghttp2_session_get_stream_remote_window_size(c->h2, stream.h2_id) ==
         65535);

  /* Real peer SETTINGS updates nghttp2's credit before invoking the callback. */
  set_peer_stream_window(c, 0);
  assert(nghttp2_session_get_stream_remote_window_size(c->h2, stream.h2_id) == 0);
  assert(!atomic_load(&c->flow_stall_since_ns));
  assert(!atomic_load(&c->flow_control_stalls));
  assert(zbp_queue_push(&stream.queue, &pending));
  update_flow_stall(c);
  assert(atomic_load(&c->flow_stall_since_ns));
  assert(atomic_load(&c->flow_control_stalls) == 1);

  atomic_store(&stream.ready, 0);
  update_flow_stall(c);
  assert(!atomic_load(&c->flow_stall_since_ns));
  atomic_store(&stream.ready, 1);
  atomic_store(&stream.status, ZBP_PROTOCOL);
  update_flow_stall(c);
  assert(!atomic_load(&c->flow_stall_since_ns));
  assert(atomic_load(&c->flow_control_stalls) == 1);

  atomic_store(&stream.status, ZBP_OK);
  update_flow_stall(c);
  assert(atomic_load(&c->flow_stall_since_ns));
  assert(atomic_load(&c->flow_control_stalls) == 2);
  set_peer_stream_window(c, 4096);
  assert(!atomic_load(&c->flow_stall_since_ns));
  assert(nghttp2_session_get_stream_remote_window_size(c->h2, stream.h2_id) ==
         4096);

  set_peer_stream_window(c, 0);
  assert(atomic_load(&c->flow_stall_since_ns));
  assert(zbp_queue_pop(&stream.queue) == &pending);
  update_flow_stall(c);
  assert(!atomic_load(&c->flow_stall_since_ns));
  stream.sending = &pending;
  update_flow_stall(c);
  assert(atomic_load(&c->flow_stall_since_ns));
  stream.sending = NULL;
  update_flow_stall(c);
  assert(!atomic_load(&c->flow_stall_since_ns));
  assert(atomic_load(&c->flow_control_stalls) == 4);

  atomic_store(&c->streams, NULL);
  zbp_queue_destroy(&stream.queue);
  fixture_destroy(&fixture);
}

static void *diagnostics_writer(void *argument) {
  zbp_connection *c = argument;
  for (unsigned i = 0; i < 20000; i++) {
    record_flow_stall(c, 1);
    record_flow_stall(c, 0);
  }
  atomic_store_explicit(&diagnostics_finished, 1, memory_order_release);
  return NULL;
}

static void concurrent_diagnostics(void) {
  connection_fixture fixture;
  fixture_init(&fixture, 0);
  zbp_connection *c = &fixture.connection;
  atomic_store(&diagnostics_finished, 0);
  pthread_t thread;
  assert(!pthread_create(&thread, NULL, diagnostics_writer, c));
  uint64_t previous = 0;
  do {
    zbp_connection_diagnostics observed;
    zbp_connection_get_diagnostics(c, &observed);
    assert(observed.flow_control_ns >= previous);
    previous = observed.flow_control_ns;
  } while (!atomic_load_explicit(&diagnostics_finished, memory_order_acquire));
  assert(!pthread_join(thread, NULL));
  assert(atomic_load(&c->flow_control_ns) >= previous);
  fixture_destroy(&fixture);
}

static void barriers_progress_during_tls_retry(zbp_queue_mode mode,
                                               size_t coalesce,
                                               unsigned retry_kind) {
  connection_fixture fixture;
  fixture_init(&fixture, coalesce);
  zbp_connection *c = &fixture.connection;
  script.write_error =
      retry_kind == 1 ? SSL_ERROR_WANT_READ : SSL_ERROR_WANT_WRITE;
  service_connection(c);
  assert(c->write_pending && script.write_retry);
  if (retry_kind == 2) {
    script.read_error = SSL_ERROR_WANT_WRITE;
    service_connection(c);
    assert(c->tls_read_want_write && script.read_retry);
  }

  /* Another stream's completed work needs no progress on the blocked socket. */
  zbp_stream ready_stream = {0};
  request barrier = {0};
  ready_stream.connection = c;
  ready_stream.submitted = 1;
  ready_stream.next_offset = 1;
  ready_stream.sent_plus_one = 1;
  ready_stream.ack_plus_one = 1;
  zbp_stream_config_init(&ready_stream.config);
  assert(zbp_queue_init(&ready_stream.queue, 4, mode));
  atomic_init(&ready_stream.ready, 1);
  atomic_init(&ready_stream.barriers_pending, 1);
  barrier.barrier = 1;
  barrier.result = ZBP_TIMEOUT;
  atomic_init(&barrier.references, 2);
  atomic_init(&barrier.done, 0);
  assert(zbp_queue_push(&ready_stream.queue, &barrier));
  atomic_store(&c->streams, &ready_stream);

  service_connection(c);
  assert(atomic_load(&c->status) == ZBP_OK);
  assert(c->write_pending && script.write_retry);
  assert(atomic_load(&barrier.done));
  assert(barrier.result == ZBP_OK && barrier.target == 1);
  assert(atomic_load(&barrier.references) == 1);
  assert(!atomic_load(&ready_stream.barriers_pending));
  assert(!atomic_load(&ready_stream.queue.count));
  assert(!ready_stream.barrier_head && !ready_stream.barrier_tail);
  assert(!ready_stream.sending && !atomic_load(&ready_stream.wire_requests));

  atomic_store(&c->streams, NULL);
  zbp_queue_destroy(&ready_stream.queue);
  fixture_destroy(&fixture);
}

int main(void) {
  for (unsigned retry = 0; retry < 3; ++retry)
    capacity_flush_on_early_return(retry);
  for (int queue = ZBP_QUEUE_MUTEX; queue <= ZBP_QUEUE_ATOMIC; ++queue) {
    payload_budget_and_failure((zbp_queue_mode)queue);
    barrier_descriptor_wakes_allocator((zbp_queue_mode)queue);
    for (unsigned reason = CAPACITY_ALLOCATION; reason <= CAPACITY_QUEUE;
         ++reason)
      for (int abort = 0; abort <= 1; ++abort)
        capacity_wait_ends((zbp_queue_mode)queue, reason, abort);
    for (int retention = ZBP_RELEASE_SENT; retention <= ZBP_RELEASE_ACKED;
         ++retention) {
      for (unsigned reason = CAPACITY_ALLOCATION; reason <= CAPACITY_QUEUE;
           ++reason)
        capacity_notifications((zbp_queue_mode)queue, (zbp_retention)retention,
                                 reason);
      blocked_producer_resumes((zbp_queue_mode)queue, (zbp_retention)retention);
      queue_waiter_resumes_before_ack((zbp_queue_mode)queue,
                                        (zbp_retention)retention);
      for (int phase = 0; phase <= 2; ++phase)
        payload_retention_and_teardown((zbp_queue_mode)queue,
                                        (zbp_retention)retention, phase);
    }
  }
  transport_diagnostics();
  flow_stall_predicate();
  concurrent_diagnostics();
  for (int queue = ZBP_QUEUE_MUTEX; queue <= ZBP_QUEUE_ATOMIC; ++queue)
    for (int prior = 0; prior <= 1; ++prior)
      barrier_between_drain_and_peek((zbp_queue_mode)queue, prior);
  for (size_t coalesce = 0; coalesce <= 4096; coalesce += 4096) {
    write_retry_allows_reads(coalesce);
    write_want_read_retries_write(coalesce);
    for (int queue = ZBP_QUEUE_MUTEX; queue <= ZBP_QUEUE_ATOMIC; ++queue)
      for (unsigned retry = 0; retry < 3; ++retry)
        barriers_progress_during_tls_retry((zbp_queue_mode)queue, coalesce,
                                           retry);
  }
  puts("state: payload admission and lifetime, barrier publication and progress, "
       "TLS duplex and stable retries passed");
  return 0;
}
