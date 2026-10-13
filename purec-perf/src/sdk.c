#define _GNU_SOURCE
#include "pool.h"
#include "proto.h"
#include "queue.h"
#include "zbperf.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <nghttp2/nghttp2.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

// clang-format off
#define STREAM_COUNTERS(X) \
  X(admitted_records) X(admitted_bytes) X(acked_records) X(acked_bytes) \
  X(wire_requests) X(ack_messages) X(buffered_bytes) X(peak_buffered_bytes) \
  X(backpressure_waits) X(backpressure_ns) X(latency_samples) \
  X(latency_sum_ns) X(latency_max_ns)
#define CONNECTION_COUNTERS(X) \
  X(write_calls) X(read_calls) X(bytes_written) X(bytes_read) \
  X(flow_control_stalls) X(wakeups)
#define CONNECTION_DIAGNOSTICS(X) \
  X(peer_initial_stream_window) X(peer_max_frame_size) \
  X(peak_remote_connection_window) X(peak_remote_stream_window) \
  X(flow_control_ns) X(write_would_block) X(write_want_read_count) \
  X(send_budget_yields) X(stream_window_updates) \
  X(min_stream_window_update) X(max_stream_window_update)
// clang-format on
#define DECLARE_COUNTER(name) _Atomic uint64_t name;

typedef struct request {
  struct request *next;
  _Atomic uint32_t free_next;
  uint32_t index;
  unsigned char *payload;
  size_t size, records, bytes;
  uint64_t sample_ns;
  int explicit_batch, barrier;
  _Atomic unsigned references;
  _Atomic int done;
  zbp_status result;
  uint64_t target;
} request;

typedef struct {
  uint64_t offset, records, bytes, sample_ns;
  request *retained;
} outstanding;

typedef struct worker worker;
struct zbp_stream {
  zbp_connection *connection;
  struct zbp_stream *next;
  zbp_stream_config config;
  char *table;
  zbp_queue queue;
  request *descriptors;
  size_t descriptor_count;
  _Atomic uint64_t free_descriptors;
  pthread_mutex_t wait_mutex;
  pthread_cond_t wait_cond;
  _Atomic unsigned waiters, allocation_waiters, queue_waiters, admissions,
      barriers_pending;
  _Atomic int status, accepting, ready, close_requested;
  STREAM_COUNTERS(DECLARE_COUNTER)
  _Atomic uint64_t latency_histogram[32];
  _Atomic uint64_t sample_counter;
  /* Everything below is owned by the connection's I/O worker. */
  int32_t h2_id;
  unsigned pending_capacity;
  int submitted, cleaned, grpc_status_seen, http_status_seen;
  unsigned char *create;
  size_t create_size, create_cursor;
  outstanding *outstanding;
  size_t out_head, out_count;
  uint64_t next_offset, sent_plus_one, ack_plus_one;
  request *barrier_head, *barrier_tail;
  request *sending, *sending_cursor;
  size_t payload_cursor;
  unsigned char header[64], record_header[16];
  size_t header_size, header_cursor, record_header_size, record_header_cursor;
  int sending_batch;
  uint64_t batch_start_ns;
  unsigned char rx_header[5];
  size_t rx_header_size, rx_size, rx_cursor, rx_capacity;
  unsigned char *rx;
};

struct zbp_connection {
  struct zbp_connection *next;
  worker *worker;
  _Atomic(zbp_stream *) streams;
  char *host, *token, *ca_file;
  uint16_t port;
  int plaintext;
  uint64_t deadline;
  struct addrinfo *addresses, *address;
  pthread_mutex_t wait_mutex;
  pthread_cond_t wait_cond;
  _Atomic int status, ready;
  CONNECTION_COUNTERS(DECLARE_COUNTER)
  CONNECTION_DIAGNOSTICS(DECLARE_COUNTER)
  _Atomic int64_t remote_connection_window, min_remote_stream_window;
  _Atomic uint64_t flow_stall_since_ns;
  _Atomic unsigned flow_clock_version;
  /* Everything below is owned by the worker. */
  int fd, state, want_write, tls_read_want_write, write_pending,
      write_want_read;
  int read_again;
  SSL_CTX *ssl_ctx;
  SSL *ssl;
  nghttp2_session *h2;
  unsigned char *output, *input;
  size_t output_capacity, output_start, output_end, retry_size;
  size_t turn_bytes;
};

struct worker {
  zbp_runtime *runtime;
  pthread_t thread;
  int event_fd;
  _Atomic int notified;
  _Atomic(zbp_connection *) connections;
  struct pollfd *pollfds;
  size_t poll_capacity;
};

struct zbp_runtime {
  zbp_runtime_config config;
  worker *workers;
  _Atomic size_t next_worker;
  _Atomic int stopping;
};

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec;
}

static uint64_t deadline_ms(unsigned ms) {
  return now_ns() + (uint64_t)ms * 1000000;
}
static struct timespec to_timespec(uint64_t ns) {
  struct timespec ts = {(time_t)(ns / 1000000000), (long)(ns % 1000000000)};
  return ts;
}

static int init_cond(pthread_cond_t *cond) {
  pthread_condattr_t attr;
  if (pthread_condattr_init(&attr))
    return 0;
  int result = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
  if (!result)
    result = pthread_cond_init(cond, &attr);
  pthread_condattr_destroy(&attr);
  return result == 0;
}

static void peak_update(_Atomic uint64_t *peak, uint64_t value) {
  uint64_t old = atomic_load_explicit(peak, memory_order_relaxed);
  while (old < value &&
         !atomic_compare_exchange_weak_explicit(
             peak, &old, value, memory_order_relaxed, memory_order_relaxed)) {
  }
}

static void wake_worker(worker *w) {
  if (!atomic_exchange_explicit(&w->notified, 1, memory_order_acq_rel)) {
    uint64_t one = 1;
    ssize_t n;
    do {
      n = write(w->event_fd, &one, sizeof(one));
    } while (n < 0 && errno == EINTR);
  }
}

enum { CAPACITY_ALLOCATION = 1, CAPACITY_QUEUE = 2 };

static void broadcast_waiters(zbp_stream *s) {
  pthread_mutex_lock(&s->wait_mutex);
  pthread_cond_broadcast(&s->wait_cond);
  pthread_mutex_unlock(&s->wait_mutex);
}

static void notify_waiters(zbp_stream *s) {
  /* Pair with registration fences before callers recheck their predicates. */
  atomic_thread_fence(memory_order_seq_cst);
  if (atomic_load_explicit(&s->waiters, memory_order_acquire))
    broadcast_waiters(s);
}

static void notify_capacity(zbp_stream *s, unsigned reasons) {
  /* Capacity release and waiter registration cannot both go unobserved. */
  atomic_thread_fence(memory_order_seq_cst);
  if (((reasons & CAPACITY_ALLOCATION) &&
       atomic_load_explicit(&s->allocation_waiters, memory_order_acquire)) ||
      ((reasons & CAPACITY_QUEUE) &&
       atomic_load_explicit(&s->queue_waiters, memory_order_acquire)))
    broadcast_waiters(s);
}

static void stream_fail(zbp_stream *s, zbp_status status) {
  int expected = ZBP_OK;
  atomic_compare_exchange_strong_explicit(&s->status, &expected, status,
                                          memory_order_acq_rel,
                                          memory_order_relaxed);
  atomic_store_explicit(&s->accepting, 0, memory_order_release);
  notify_waiters(s);
}

static void connection_fail(zbp_connection *c, zbp_status status) {
  int expected = ZBP_OK;
  atomic_compare_exchange_strong_explicit(&c->status, &expected, status,
                                          memory_order_acq_rel,
                                          memory_order_relaxed);
  pthread_mutex_lock(&c->wait_mutex);
  pthread_cond_broadcast(&c->wait_cond);
  pthread_mutex_unlock(&c->wait_mutex);
  for (zbp_stream *s = atomic_load_explicit(&c->streams, memory_order_acquire);
       s; s = s->next)
    stream_fail(s, status);
}

static request *descriptor_get(zbp_stream *s) {
  uint64_t old =
      atomic_load_explicit(&s->free_descriptors, memory_order_acquire);
  for (;;) {
    uint32_t id = (uint32_t)old;
    if (!id)
      return NULL;
    request *r = &s->descriptors[id - 1];
    uint32_t next = atomic_load_explicit(&r->free_next, memory_order_relaxed);
    uint64_t desired =
        ((old + ((uint64_t)1 << 32)) & UINT64_C(0xffffffff00000000)) | next;
    if (atomic_compare_exchange_weak_explicit(&s->free_descriptors, &old,
                                              desired, memory_order_acquire,
                                              memory_order_acquire)) {
      r->next = NULL;
      r->payload = NULL;
      r->size = r->records = r->bytes = 0;
      r->sample_ns = 0;
      r->explicit_batch = r->barrier = 0;
      return r;
    }
  }
}

static void descriptor_put(zbp_stream *s, request *r) {
  uint64_t old =
      atomic_load_explicit(&s->free_descriptors, memory_order_relaxed);
  for (;;) {
    atomic_store_explicit(&r->free_next, (uint32_t)old, memory_order_relaxed);
    uint64_t desired =
        ((old + ((uint64_t)1 << 32)) & UINT64_C(0xffffffff00000000)) |
        (r->index + 1);
    if (atomic_compare_exchange_weak_explicit(&s->free_descriptors, &old,
                                              desired, memory_order_release,
                                              memory_order_relaxed))
      return;
  }
}

static void request_put(zbp_stream *s, request *r) {
  if (r->payload) {
    free(r->payload);
    zbp_payload_release(&s->buffered_bytes, r->size);
  }
  descriptor_put(s, r);
}

static void release_chain(zbp_stream *s, request *r) {
  while (r) {
    request *next = r->next;
    request_put(s, r);
    r = next;
  }
}

static void barrier_release(zbp_stream *s, request *r) {
  if (atomic_fetch_sub_explicit(&r->references, 1, memory_order_acq_rel) == 1) {
    descriptor_put(s, r);
    notify_capacity(s, CAPACITY_ALLOCATION);
  }
}

static void barrier_done(zbp_stream *s, request *r, zbp_status result) {
  r->result = result;
  atomic_store_explicit(&r->done, 1, memory_order_release);
  atomic_fetch_sub_explicit(&s->barriers_pending, 1, memory_order_relaxed);
  notify_waiters(s);
  barrier_release(s, r);
}

static void complete_barriers(zbp_stream *s) {
  while (s->barrier_head && s->barrier_head->target <= s->ack_plus_one) {
    request *r = s->barrier_head;
    s->barrier_head = r->next;
    if (!s->barrier_head)
      s->barrier_tail = NULL;
    barrier_done(s, r, ZBP_OK);
  }
}

/* Called only by the owner, after it has stopped supplying payload to nghttp2.
 */
static void stream_cleanup(zbp_stream *s) {
  if (s->cleaned || atomic_load_explicit(&s->admissions, memory_order_acquire))
    return;
  s->cleaned = 1;
  release_chain(s, s->sending);
  s->sending = s->sending_cursor = NULL;
  for (size_t i = 0; i < s->out_count; i++) {
    outstanding *o =
        &s->outstanding[(s->out_head + i) % s->config.max_inflight];
    release_chain(s, o->retained);
    o->retained = NULL;
  }
  s->out_count = 0;
  request *r;
  while ((r = zbp_queue_pop(&s->queue))) {
    if (r->barrier)
      barrier_done(s, r, (zbp_status)atomic_load(&s->status));
    else
      request_put(s, r);
  }
  while ((r = s->barrier_head)) {
    s->barrier_head = r->next;
    barrier_done(s, r, (zbp_status)atomic_load(&s->status));
  }
  s->barrier_tail = NULL;
  notify_waiters(s);
}

static void note_latency(zbp_stream *s, uint64_t start) {
  if (!start)
    return;
  uint64_t elapsed = now_ns() - start;
  atomic_fetch_add_explicit(&s->latency_samples, 1, memory_order_relaxed);
  atomic_fetch_add_explicit(&s->latency_sum_ns, elapsed, memory_order_relaxed);
  peak_update(&s->latency_max_ns, elapsed);
  uint64_t us = elapsed / 1000;
  unsigned bucket = 0;
  while (us > 1 && bucket < 31) {
    us >>= 1;
    bucket++;
  }
  atomic_fetch_add_explicit(&s->latency_histogram[bucket], 1,
                            memory_order_relaxed);
}

static int receive_message(zbp_stream *s) {
  uint64_t ack = 0;
  int kind = zbp_parse_response(s->rx, s->rx_size, &ack);
  if (kind < 0)
    return 0;
  if (kind == 1) {
    if (atomic_exchange_explicit(&s->ready, 1, memory_order_acq_rel))
      return 0;
    notify_waiters(s);
  } else if (kind == 2) {
    if (!atomic_load_explicit(&s->ready, memory_order_acquire) ||
        ack >= s->sent_plus_one)
      return 0;
    atomic_fetch_add_explicit(&s->ack_messages, 1, memory_order_relaxed);
    if (ack + 1 <= s->ack_plus_one)
      return 1;
    s->ack_plus_one = ack + 1;
    int capacity_changed = 0;
    while (s->out_count) {
      outstanding *o = &s->outstanding[s->out_head];
      if (o->offset > ack)
        break;
      atomic_fetch_add_explicit(&s->acked_records, o->records,
                                memory_order_relaxed);
      atomic_fetch_add_explicit(&s->acked_bytes, o->bytes,
                                memory_order_relaxed);
      note_latency(s, o->sample_ns);
      if (o->retained)
        capacity_changed = 1;
      release_chain(s, o->retained);
      o->retained = NULL;
      s->out_head = (s->out_head + 1) % s->config.max_inflight;
      s->out_count--;
    }
    if (capacity_changed)
      s->pending_capacity |= CAPACITY_ALLOCATION;
    complete_barriers(s);
  }
  /* Close signals are advisory; this POC never reconnects. */
  return 1;
}

static int on_data(nghttp2_session *h2, uint8_t flags, int32_t id,
                   const uint8_t *data, size_t size, void *user) {
  (void)flags;
  (void)user;
  zbp_stream *s = nghttp2_session_get_stream_user_data(h2, id);
  if (!s || atomic_load_explicit(&s->status, memory_order_acquire) != ZBP_OK)
    return 0;
  while (size) {
    if (s->rx_header_size < 5) {
      size_t n = 5 - s->rx_header_size;
      if (n > size)
        n = size;
      memcpy(s->rx_header + s->rx_header_size, data, n);
      s->rx_header_size += n;
      data += n;
      size -= n;
      if (s->rx_header_size < 5)
        continue;
      s->rx_size = ((size_t)s->rx_header[1] << 24) |
                   ((size_t)s->rx_header[2] << 16) |
                   ((size_t)s->rx_header[3] << 8) | s->rx_header[4];
      if (s->rx_header[0] || !s->rx_size || s->rx_size > 1024 * 1024) {
        stream_fail(s, ZBP_PROTOCOL);
        return 0;
      }
      if (s->rx_size > s->rx_capacity) {
        unsigned char *buffer = realloc(s->rx, s->rx_size);
        if (!buffer) {
          stream_fail(s, ZBP_NOMEM);
          return 0;
        }
        s->rx = buffer;
        s->rx_capacity = s->rx_size;
      }
      s->rx_cursor = 0;
    }
    size_t n = s->rx_size - s->rx_cursor;
    if (n > size)
      n = size;
    memcpy(s->rx + s->rx_cursor, data, n);
    s->rx_cursor += n;
    data += n;
    size -= n;
    if (s->rx_cursor == s->rx_size) {
      if (!receive_message(s)) {
        stream_fail(s, ZBP_PROTOCOL);
        return 0;
      }
      s->rx_header_size = 0;
    }
  }
  return 0;
}

static int on_header(nghttp2_session *h2, const nghttp2_frame *frame,
                     const uint8_t *name, size_t name_size,
                     const uint8_t *value, size_t value_size, uint8_t flags,
                     void *user) {
  (void)flags;
  (void)user;
  zbp_stream *s = nghttp2_session_get_stream_user_data(h2, frame->hd.stream_id);
  if (!s)
    return 0;
  if (name_size == 7 && !memcmp(name, ":status", 7)) {
    s->http_status_seen = 1;
    if (value_size != 3 || memcmp(value, "200", 3))
      stream_fail(s, value_size == 3 && (!memcmp(value, "401", 3) ||
                                         !memcmp(value, "403", 3))
                         ? ZBP_AUTH
                         : ZBP_PROTOCOL);
  } else if (name_size == 11 && !memcmp(name, "grpc-status", 11)) {
    unsigned code = 0;
    if (!value_size || value_size > 2) {
      stream_fail(s, ZBP_PROTOCOL);
      return 0;
    }
    for (size_t i = 0; i < value_size; i++) {
      if (value[i] < '0' || value[i] > '9') {
        stream_fail(s, ZBP_PROTOCOL);
        return 0;
      }
      code = code * 10 + (unsigned)(value[i] - '0');
    }
    s->grpc_status_seen = 1;
    if (code)
      stream_fail(s, code == 7 || code == 16 ? ZBP_AUTH : ZBP_PROTOCOL);
  }
  return 0;
}

static int on_stream_close(nghttp2_session *h2, int32_t id, uint32_t error,
                           void *user) {
  (void)user;
  zbp_stream *s = nghttp2_session_get_stream_user_data(h2, id);
  if (s && (!atomic_load_explicit(&s->close_requested, memory_order_acquire) ||
            error || !s->grpc_status_seen || s->out_count || s->rx_header_size))
    stream_fail(s, ZBP_PROTOCOL);
  return 0;
}

static void capture_send_windows(zbp_connection *c) {
  int32_t connection_window = nghttp2_session_get_remote_window_size(c->h2);
  atomic_store_explicit(&c->remote_connection_window, connection_window,
                        memory_order_relaxed);
  if (connection_window > 0)
    peak_update(&c->peak_remote_connection_window, (uint64_t)connection_window);
  int64_t minimum = INT64_MAX;
  for (zbp_stream *s = atomic_load_explicit(&c->streams, memory_order_acquire);
       s; s = s->next) {
    if (!s->submitted ||
        atomic_load_explicit(&s->status, memory_order_relaxed) != ZBP_OK)
      continue;
    int32_t window =
        nghttp2_session_get_stream_remote_window_size(c->h2, s->h2_id);
    if (window < minimum)
      minimum = window;
    if (window > 0)
      peak_update(&c->peak_remote_stream_window, (uint64_t)window);
  }
  atomic_store_explicit(&c->min_remote_stream_window,
                        minimum == INT64_MAX ? 0 : minimum, memory_order_relaxed);
}

static void record_flow_stall(zbp_connection *c, int stalled) {
  uint64_t since =
      atomic_load_explicit(&c->flow_stall_since_ns, memory_order_relaxed);
  if (stalled && !since) {
    atomic_fetch_add_explicit(&c->flow_clock_version, 1, memory_order_seq_cst);
    /* Publish the odd version through payload stores to the reader's fence. */
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&c->flow_stall_since_ns, now_ns(), memory_order_relaxed);
    atomic_fetch_add_explicit(&c->flow_control_stalls, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&c->flow_clock_version, 1, memory_order_seq_cst);
  } else if (!stalled && since) {
    atomic_fetch_add_explicit(&c->flow_clock_version, 1, memory_order_seq_cst);
    atomic_thread_fence(memory_order_release);
    atomic_fetch_add_explicit(&c->flow_control_ns, now_ns() - since,
                              memory_order_relaxed);
    atomic_store_explicit(&c->flow_stall_since_ns, 0, memory_order_relaxed);
    atomic_fetch_add_explicit(&c->flow_clock_version, 1, memory_order_seq_cst);
  }
}

static void update_flow_stall(zbp_connection *c) {
  int stalled = 0;
  int32_t connection_window = nghttp2_session_get_remote_window_size(c->h2);
  for (zbp_stream *s = atomic_load_explicit(&c->streams, memory_order_acquire);
       s; s = s->next) {
    if (s->submitted && atomic_load_explicit(&s->ready, memory_order_relaxed) &&
        atomic_load_explicit(&s->status, memory_order_relaxed) == ZBP_OK &&
        (s->sending || atomic_load_explicit(&s->queue.count, memory_order_relaxed)) &&
        (connection_window <= 0 ||
         nghttp2_session_get_stream_remote_window_size(c->h2, s->h2_id) <= 0)) {
      stalled = 1;
      break;
    }
  }
  record_flow_stall(c, stalled);
}

static int on_frame(nghttp2_session *h2, const nghttp2_frame *frame,
                    void *user) {
  zbp_connection *c = user;
  if (frame->hd.type == NGHTTP2_SETTINGS &&
      !(frame->hd.flags & NGHTTP2_FLAG_ACK)) {
    atomic_store_explicit(
        &c->peer_initial_stream_window,
        nghttp2_session_get_remote_settings(
            h2, NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE), memory_order_relaxed);
    atomic_store_explicit(
        &c->peer_max_frame_size,
        nghttp2_session_get_remote_settings(h2, NGHTTP2_SETTINGS_MAX_FRAME_SIZE),
        memory_order_relaxed);
  }
  if (frame->hd.type == NGHTTP2_SETTINGS ||
      frame->hd.type == NGHTTP2_WINDOW_UPDATE) {
    capture_send_windows(c);
    update_flow_stall(c);
  }
  if (frame->hd.type == NGHTTP2_WINDOW_UPDATE && frame->hd.stream_id) {
    uint64_t increment = (uint64_t)frame->window_update.window_size_increment;
    uint64_t minimum = atomic_load_explicit(&c->min_stream_window_update,
                                            memory_order_relaxed);
    if (!minimum || increment < minimum)
      atomic_store_explicit(&c->min_stream_window_update, increment,
                            memory_order_relaxed);
    peak_update(&c->max_stream_window_update, increment);
    atomic_fetch_add_explicit(&c->stream_window_updates, 1, memory_order_relaxed);
  }
  if (frame->hd.type == NGHTTP2_GOAWAY)
    connection_fail(user, ZBP_PROTOCOL);
  if ((frame->hd.type == NGHTTP2_HEADERS || frame->hd.type == NGHTTP2_DATA) &&
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
    zbp_stream *s =
        nghttp2_session_get_stream_user_data(h2, frame->hd.stream_id);
    if (s &&
        (!atomic_load_explicit(&s->close_requested, memory_order_acquire) ||
         s->rx_header_size || !s->grpc_status_seen || s->out_count))
      stream_fail(s, ZBP_PROTOCOL);
  }
  return 0;
}

static size_t encoded_size(size_t body, uint64_t offset) {
  size_t inner = 1 + zbp_varint_size(offset) + 1 + zbp_varint_size(body) + body;
  return 1 + zbp_varint_size(inner) + inner;
}

static void drain_barriers(zbp_stream *s) {
  request *r;
  while ((r = zbp_queue_peek(&s->queue)) && r->barrier) {
    (void)zbp_queue_pop(&s->queue);
    r->target = s->next_offset;
    r->next = NULL;
    if (s->barrier_tail)
      s->barrier_tail->next = r;
    else
      s->barrier_head = r;
    s->barrier_tail = r;
    complete_barriers(s);
    notify_capacity(s, CAPACITY_QUEUE);
  }
}

static int start_request(zbp_stream *s) {
  request *first;
  do {
    drain_barriers(s);
    first = zbp_queue_peek(&s->queue);
    /* An empty queue can receive another barrier between drain and peek. */
  } while (first && first->barrier);
  if (s->out_count == s->config.max_inflight)
    return 0;
  if (!first) {
    s->batch_start_ns = 0;
    return 0;
  }
  int automatic =
      s->config.batching == ZBP_BATCH_AUTO && !first->explicit_batch;
  if (automatic && s->config.batch_delay_us &&
      atomic_load_explicit(&s->queue.count, memory_order_relaxed) <
          s->config.batch_records &&
      !atomic_load_explicit(&s->barriers_pending, memory_order_relaxed) &&
      !atomic_load_explicit(&s->close_requested, memory_order_acquire)) {
    uint64_t now = now_ns();
    if (!s->batch_start_ns)
      s->batch_start_ns = now;
    if (now < s->batch_start_ns + (uint64_t)s->config.batch_delay_us * 1000)
      return 0;
  }
  s->batch_start_ns = 0;
  first = zbp_queue_pop(&s->queue);
  request *last = first;
  size_t records = first->records, bytes = first->bytes;
  size_t body = first->size;
  int batch = first->explicit_batch || automatic;
  if (automatic)
    body += 1 + zbp_varint_size(first->size);
  uint64_t sample = first->sample_ns;
  if (automatic) {
    request *next;
    while (records < s->config.batch_records &&
           (next = zbp_queue_peek(&s->queue))) {
      if (next->barrier || next->explicit_batch)
        break;
      size_t next_body = body + 1 + zbp_varint_size(next->size) + next->size;
      if (encoded_size(next_body, s->next_offset) > s->config.batch_bytes)
        break;
      next = zbp_queue_pop(&s->queue);
      last->next = next;
      last = next;
      body = next_body;
      records += next->records;
      bytes += next->bytes;
      if (next->sample_ns && (!sample || next->sample_ns < sample))
        sample = next->sample_ns;
    }
  }
  if (s->next_offset > INT64_MAX ||
      encoded_size(body, s->next_offset) > s->config.batch_bytes) {
    release_chain(s, first);
    stream_fail(s, ZBP_LIMIT);
    return 0;
  }
  outstanding *out =
      &s->outstanding[(s->out_head + s->out_count) % s->config.max_inflight];
  out->offset = s->next_offset;
  out->records = records;
  out->bytes = bytes;
  out->sample_ns = sample;
  out->retained = NULL;
  s->out_count++;
  s->header_size = zbp_wire_header(s->header, s->next_offset++, body, batch);
  s->header_cursor = 0;
  s->payload_cursor = 0;
  s->record_header_cursor = s->record_header_size = 0;
  s->sending_batch = automatic;
  s->sending = s->sending_cursor = first;
  atomic_fetch_add_explicit(&s->wire_requests, 1, memory_order_relaxed);
  return 1;
}

static size_t copy_segment(unsigned char **out, size_t *available,
                           const unsigned char *data, size_t size,
                           size_t *cursor) {
  size_t n = size - *cursor;
  if (n > *available)
    n = *available;
  memcpy(*out, data + *cursor, n);
  *cursor += n;
  *out += n;
  *available -= n;
  return n;
}

static ssize_t data_read(nghttp2_session *h2, int32_t id, uint8_t *buffer,
                         size_t length, uint32_t *flags,
                         nghttp2_data_source *source, void *user) {
  (void)h2;
  (void)id;
  (void)user;
  zbp_stream *s = source->ptr;
  int status = atomic_load_explicit(&s->status, memory_order_acquire);
  if (status != ZBP_OK &&
      !(status == ZBP_CLOSED && atomic_load(&s->close_requested)))
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
  unsigned char *out = buffer;
  size_t available = length;
  if (s->create_cursor < s->create_size) {
    copy_segment(&out, &available, s->create, s->create_size,
                 &s->create_cursor);
    return (ssize_t)(length - available);
  }
  if (!atomic_load_explicit(&s->ready, memory_order_acquire))
    return NGHTTP2_ERR_DEFERRED;
  unsigned capacity_changed = 0;
  while (available) {
    if (!s->sending) {
      if (!start_request(s))
        break;
      capacity_changed |= CAPACITY_QUEUE;
    }
    copy_segment(&out, &available, s->header, s->header_size,
                 &s->header_cursor);
    if (!available)
      break;
    request *r = s->sending_cursor;
    if (s->sending_batch) {
      if (!s->record_header_size)
        s->record_header_size = zbp_record_prefix(s->record_header, r->size);
      copy_segment(&out, &available, s->record_header, s->record_header_size,
                   &s->record_header_cursor);
      if (!available)
        break;
    }
    copy_segment(&out, &available, r->payload, r->size, &s->payload_cursor);
    if (s->payload_cursor == r->size) {
      s->sending_cursor = r->next;
      s->payload_cursor = 0;
      s->record_header_size = s->record_header_cursor = 0;
      if (!s->sending_cursor) {
        if (s->config.retention == ZBP_RELEASE_ACKED) {
          outstanding *o = &s->outstanding[(s->out_head + s->out_count - 1) %
                                           s->config.max_inflight];
          o->retained = s->sending;
        } else {
          release_chain(s, s->sending);
          capacity_changed |= CAPACITY_ALLOCATION;
        }
        s->sending = NULL;
        s->sent_plus_one = s->next_offset;
      }
    }
  }
  s->pending_capacity |= capacity_changed;
  if (length != available)
    return (ssize_t)(length - available);
  if (atomic_load_explicit(&s->close_requested, memory_order_acquire) &&
      !s->sending &&
      !atomic_load_explicit(&s->queue.count, memory_order_relaxed)) {
    *flags |= NGHTTP2_DATA_FLAG_EOF;
    return 0;
  }
  return NGHTTP2_ERR_DEFERRED;
}

/* -2 denotes a nonblocking retry; -1 is terminal. */
static ssize_t transport_write(zbp_connection *c, const unsigned char *data,
                               size_t size) {
  if (size > INT_MAX)
    size = INT_MAX;
  atomic_fetch_add_explicit(&c->write_calls, 1, memory_order_relaxed);
  if (c->plaintext) {
    ssize_t n = send(c->fd, data, size, MSG_NOSIGNAL);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      atomic_fetch_add_explicit(&c->write_would_block, 1, memory_order_relaxed);
      c->want_write = 1;
      return -2;
    }
    if (n <= 0)
      return -1;
    c->want_write = 0;
    atomic_fetch_add_explicit(&c->bytes_written, (uint64_t)n,
                              memory_order_relaxed);
    c->turn_bytes += (size_t)n;
    return n;
  }
  int n = SSL_write(c->ssl, data, (int)size);
  if (n > 0) {
    c->want_write = c->write_pending = c->write_want_read = 0;
    atomic_fetch_add_explicit(&c->bytes_written, (uint64_t)n,
                              memory_order_relaxed);
    c->turn_bytes += (size_t)n;
    return n;
  }
  int error = SSL_get_error(c->ssl, n);
  if (error == SSL_ERROR_WANT_WRITE || error == SSL_ERROR_WANT_READ) {
    atomic_fetch_add_explicit(
        error == SSL_ERROR_WANT_READ ? &c->write_want_read_count
                                    : &c->write_would_block,
        1, memory_order_relaxed);
    c->want_write = error == SSL_ERROR_WANT_WRITE;
    c->write_pending = 1;
    c->write_want_read = error == SSL_ERROR_WANT_READ;
    return -2;
  }
  return -1;
}

static int flush_output(zbp_connection *c) {
  while (c->output_start != c->output_end && c->turn_bytes < 1024 * 1024) {
    size_t n = c->retry_size ? c->retry_size : c->output_end - c->output_start;
    ssize_t written = transport_write(c, c->output + c->output_start, n);
    if (written == -2) {
      c->retry_size = n;
      return 1;
    }
    if (written < 0)
      return 0;
    c->retry_size = 0;
    c->output_start += (size_t)written;
  }
  if (c->output_start == c->output_end)
    c->output_start = c->output_end = 0;
  return 1;
}

static ssize_t send_callback(nghttp2_session *h2, const uint8_t *data,
                             size_t length, int flags, void *user) {
  (void)h2;
  (void)flags;
  zbp_connection *c = user;
  if (c->turn_bytes >= 1024 * 1024) {
    atomic_fetch_add_explicit(&c->send_budget_yields, 1, memory_order_relaxed);
    c->want_write = 1;
    return NGHTTP2_ERR_WOULDBLOCK;
  }
  if (!c->output_capacity) {
    ssize_t n = transport_write(c, data, length);
    if (n == -2)
      return NGHTTP2_ERR_WOULDBLOCK;
    return n < 0 ? NGHTTP2_ERR_CALLBACK_FAILURE : n;
  }
  if (c->write_pending)
    return NGHTTP2_ERR_WOULDBLOCK;
  if (length > c->output_capacity - c->output_end) {
    if (!flush_output(c))
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    if (c->write_pending || c->output_end == c->output_capacity)
      return NGHTTP2_ERR_WOULDBLOCK;
  }
  /* The peer can negotiate frames larger than our bounded output buffer. */
  if (length > c->output_capacity - c->output_end)
    length = c->output_capacity - c->output_end;
  memcpy(c->output + c->output_end, data, length);
  c->output_end += length;
  if (c->output_end >= c->worker->runtime->config.write_coalesce_bytes &&
      !flush_output(c))
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  return (ssize_t)length;
}

static int send_connection(zbp_connection *c) {
  if (c->output_capacity) {
    if (!flush_output(c))
      return 0;
    if (c->write_pending)
      return 1;
  }
  /* In direct mode nghttp2 retains the exact buffer for an SSL_write retry. */
  if (nghttp2_session_send(c->h2))
    return 0;
  return c->write_pending || flush_output(c);
}

static int initialize_h2(zbp_connection *c) {
  nghttp2_session_callbacks *callbacks = NULL;
  if (nghttp2_session_callbacks_new(&callbacks))
    return 0;
  nghttp2_session_callbacks_set_send_callback(callbacks, send_callback);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, on_data);
  nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header);
  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks,
                                                         on_stream_close);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, on_frame);
  int result = nghttp2_session_client_new(&c->h2, callbacks, c);
  nghttp2_session_callbacks_del(callbacks);
  if (result)
    return 0;
  nghttp2_settings_entry settings[] = {
      {NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 16 * 1024 * 1024}};
  if (nghttp2_submit_settings(c->h2, NGHTTP2_FLAG_NONE, settings, 2) ||
      nghttp2_session_set_local_window_size(c->h2, NGHTTP2_FLAG_NONE, 0,
                                            16 * 1024 * 1024))
    return 0;
  c->state = 3;
  atomic_store_explicit(&c->ready, 1, memory_order_release);
  pthread_mutex_lock(&c->wait_mutex);
  pthread_cond_broadcast(&c->wait_cond);
  pthread_mutex_unlock(&c->wait_mutex);
  return 1;
}

static int start_socket(zbp_connection *c) {
  while (c->address) {
    struct addrinfo *address = c->address;
    c->address = c->address->ai_next;
    int fd =
        socket(address->ai_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
               address->ai_protocol);
    if (fd < 0)
      continue;
    int one = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    int result = connect(fd, address->ai_addr, address->ai_addrlen);
    if (!result || errno == EINPROGRESS) {
      c->fd = fd;
      c->state = 1;
      c->want_write = 1;
      return 1;
    }
    close(fd);
  }
  return 0;
}

static int connected(zbp_connection *c) {
  if (c->plaintext)
    return initialize_h2(c);
  c->ssl_ctx = SSL_CTX_new(TLS_client_method());
  if (!c->ssl_ctx)
    return 0;
  SSL_CTX_set_min_proto_version(c->ssl_ctx, TLS1_2_VERSION);
  SSL_CTX_set_verify(c->ssl_ctx, SSL_VERIFY_PEER, NULL);
  if (c->ca_file
          ? SSL_CTX_load_verify_locations(c->ssl_ctx, c->ca_file, NULL) != 1
          : SSL_CTX_set_default_verify_paths(c->ssl_ctx) != 1)
    return 0;
  c->ssl = SSL_new(c->ssl_ctx);
  if (!c->ssl || SSL_set_fd(c->ssl, c->fd) != 1)
    return 0;
  SSL_set_mode(c->ssl, SSL_MODE_ENABLE_PARTIAL_WRITE |
                           SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  unsigned char address[16];
  if (inet_pton(AF_INET, c->host, address) == 1 ||
      inet_pton(AF_INET6, c->host, address) == 1) {
    if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(c->ssl), c->host) != 1)
      return 0;
  } else if (SSL_set_tlsext_host_name(c->ssl, c->host) != 1 ||
             SSL_set1_host(c->ssl, c->host) != 1)
    return 0;
  const unsigned char alpn[] = {2, 'h', '2'};
  if (SSL_set_alpn_protos(c->ssl, alpn, sizeof(alpn)))
    return 0;
  c->state = 2;
  return 1;
}

static void submit_stream(zbp_stream *s) {
  zbp_connection *c = s->connection;
  char authority[1024];
  int n =
      strchr(c->host, ':')
          ? snprintf(authority, sizeof(authority), "[%s]:%u", c->host, c->port)
          : snprintf(authority, sizeof(authority), "%s:%u", c->host, c->port);
  if (n < 0 || (size_t)n >= sizeof(authority)) {
    stream_fail(s, ZBP_INVALID);
    return;
  }
  size_t token_length = strlen(c->token);
  char *authorization = malloc(token_length + 8);
  if (!authorization) {
    stream_fail(s, ZBP_NOMEM);
    return;
  }
  memcpy(authorization, "Bearer ", 7);
  memcpy(authorization + 7, c->token, token_length + 1);
#define HEADER(name, value)                                                    \
  {(uint8_t *)(name), (uint8_t *)(value), sizeof(name) - 1, strlen(value),     \
   NGHTTP2_NV_FLAG_NONE}
  nghttp2_nv headers[] = {
      HEADER(":method", "POST"),
      HEADER(":scheme", c->plaintext ? "http" : "https"),
      HEADER(":authority", authority),
      HEADER(":path", "/databricks.zerobus.Zerobus/EphemeralStream"),
      HEADER("content-type", "application/grpc"),
      HEADER("te", "trailers"),
      HEADER("authorization", authorization),
      HEADER("user-agent", "zbperf/0.1.0"),
      HEADER("x-databricks-zerobus-table-name", s->table)};
#undef HEADER
  /* Authorization is never entered into the peer's HPACK dynamic table. */
  headers[6].flags = NGHTTP2_NV_FLAG_NO_INDEX;
  nghttp2_data_provider provider = {.source.ptr = s,
                                    .read_callback = data_read};
  s->h2_id = nghttp2_submit_request(
      c->h2, NULL, headers, sizeof(headers) / sizeof(headers[0]), &provider, s);
  OPENSSL_cleanse(authorization, token_length + 7);
  free(authorization);
  if (s->h2_id < 0) {
    stream_fail(s, ZBP_PROTOCOL);
    return;
  }
  s->submitted = 1;
}

static int read_connection(zbp_connection *c) {
  c->read_again = 0;
  size_t total = 0;
  while (total < 1024 * 1024) {
    atomic_fetch_add_explicit(&c->read_calls, 1, memory_order_relaxed);
    ssize_t n;
    if (c->plaintext) {
      n = recv(c->fd, c->input, 65536, 0);
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
        return 1;
      if (n <= 0)
        return 0;
    } else {
      n = SSL_read(c->ssl, c->input, 65536);
      if (n <= 0) {
        int error = SSL_get_error(c->ssl, (int)n);
        if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
          c->tls_read_want_write = error == SSL_ERROR_WANT_WRITE;
          return 1;
        }
        return 0;
      }
      c->tls_read_want_write = 0;
    }
    atomic_fetch_add_explicit(&c->bytes_read, (uint64_t)n,
                              memory_order_relaxed);
    total += (size_t)n;
    ssize_t consumed = nghttp2_session_mem_recv(c->h2, c->input, (size_t)n);
    if (consumed != n)
      return 0;
    if (atomic_load_explicit(&c->status, memory_order_acquire) != ZBP_OK)
      return 0;
  }
  c->read_again = 1;
  return 1;
}

static void drain_connection_barriers(zbp_connection *c) {
  for (zbp_stream *s = atomic_load_explicit(&c->streams, memory_order_acquire);
       s; s = s->next) {
    if (!s->sending &&
        atomic_load_explicit(&s->barriers_pending, memory_order_relaxed) &&
        atomic_load_explicit(&s->ready, memory_order_acquire) &&
        atomic_load_explicit(&s->status, memory_order_acquire) == ZBP_OK)
      drain_barriers(s);
  }
}

static void service_connection_inner(zbp_connection *c) {
  if (atomic_load_explicit(&c->status, memory_order_acquire) != ZBP_OK) {
    record_flow_stall(c, 0);
    for (zbp_stream *s =
             atomic_load_explicit(&c->streams, memory_order_acquire);
         s; s = s->next) {
      stream_fail(s, (zbp_status)atomic_load(&c->status));
      stream_cleanup(s);
    }
    if (c->fd >= 0) {
      close(c->fd);
      c->fd = -1;
    }
    return;
  }
  c->turn_bytes = 0;
  if (c->state < 3 && now_ns() >= c->deadline) {
    connection_fail(c, ZBP_TIMEOUT);
    return;
  }
  if (!c->state && !start_socket(c)) {
    connection_fail(c, ZBP_IO);
    return;
  }
  if (c->state == 1) {
    struct pollfd fd = {c->fd, POLLOUT, 0};
    if (poll(&fd, 1, 0) <= 0)
      return;
    int error = 0;
    socklen_t size = sizeof(error);
    if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &error, &size) || error) {
      close(c->fd);
      c->fd = -1;
      if (!start_socket(c))
        connection_fail(c, ZBP_IO);
      return;
    }
    if (!connected(c)) {
      connection_fail(c, ZBP_IO);
      return;
    }
  }
  if (c->state == 2) {
    int result = SSL_connect(c->ssl);
    if (result != 1) {
      int error = SSL_get_error(c->ssl, result);
      if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
        c->want_write = error == SSL_ERROR_WANT_WRITE;
        return;
      }
      connection_fail(c, SSL_get_verify_result(c->ssl) == X509_V_OK ? ZBP_IO
                                                                    : ZBP_AUTH);
      return;
    }
    const unsigned char *alpn = NULL;
    unsigned length = 0;
    SSL_get0_alpn_selected(c->ssl, &alpn, &length);
    if (length != 2 || memcmp(alpn, "h2", 2) || !initialize_h2(c)) {
      connection_fail(c, ZBP_PROTOCOL);
      return;
    }
  }
  int received = 0;
  if (c->tls_read_want_write) {
    /* SSL_read keeps the same input buffer when a handshake needs writing. */
    if (!read_connection(c)) {
      connection_fail(c, ZBP_IO);
      return;
    }
    received = 1;
    if (c->tls_read_want_write) {
      drain_connection_barriers(c);
      return;
    }
  }
  if (c->write_pending) {
    if (!send_connection(c)) {
      connection_fail(c, ZBP_IO);
      return;
    }
    /* WANT_READ is handshake progress owned by the SSL_write retry. WANT_WRITE
     * still permits receiving acknowledgments when both sockets are full. */
    if (c->write_pending && c->write_want_read) {
      drain_connection_barriers(c);
      return;
    }
  }
  if (!received && !read_connection(c)) {
    connection_fail(c, ZBP_IO);
    return;
  }
  if (c->tls_read_want_write || c->write_pending) {
    /* Local flush completion must not wait for another stream's socket I/O. */
    drain_connection_barriers(c);
    return;
  }
  for (zbp_stream *s = atomic_load_explicit(&c->streams, memory_order_acquire);
       s; s = s->next) {
    int status = atomic_load_explicit(&s->status, memory_order_acquire);
    if (status != ZBP_OK &&
        !(status == ZBP_CLOSED && atomic_load(&s->close_requested))) {
      if (s->submitted && !s->cleaned)
        nghttp2_submit_rst_stream(c->h2, NGHTTP2_FLAG_NONE, s->h2_id,
                                  NGHTTP2_CANCEL);
      stream_cleanup(s);
      continue;
    }
    if (!s->submitted)
      submit_stream(s);
    if (s->submitted) {
      if (!s->sending && atomic_load_explicit(&s->ready, memory_order_acquire))
        drain_barriers(s);
      nghttp2_session_resume_data(c->h2, s->h2_id);
    }
  }
  if (!send_connection(c)) {
    connection_fail(c, ZBP_IO);
    return;
  }
  update_flow_stall(c);
  capture_send_windows(c);
}

static void flush_capacity_notifications(zbp_connection *c) {
  for (zbp_stream *s = atomic_load_explicit(&c->streams, memory_order_acquire);
       s; s = s->next) {
    unsigned reasons = s->pending_capacity;
    s->pending_capacity = 0;
    if (reasons)
      notify_capacity(s, reasons);
  }
}

static void service_connection(zbp_connection *c) {
  service_connection_inner(c);
  /* Flush on every return path before the owner can wait for socket readiness. */
  flush_capacity_notifications(c);
}

static void *worker_main(void *argument) {
  worker *w = argument;
  /* OpenSSL's socket BIO does not use MSG_NOSIGNAL. Scope SIGPIPE suppression
   * to SDK workers instead of changing the application's process-wide handler.
   */
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &signals, NULL);
  uint64_t idle_since = now_ns();
  while (!atomic_load_explicit(&w->runtime->stopping, memory_order_acquire)) {
    uint64_t events;
    while (read(w->event_fd, &events, sizeof(events)) > 0) {
    }
    atomic_store_explicit(&w->notified, 0, memory_order_release);
    size_t count = 1;
    uint64_t before = 0, after = 0;
    for (zbp_connection *c =
             atomic_load_explicit(&w->connections, memory_order_acquire);
         c; c = c->next) {
      before += atomic_load_explicit(&c->bytes_written, memory_order_relaxed) +
                atomic_load_explicit(&c->bytes_read, memory_order_relaxed);
      service_connection(c);
      after += atomic_load_explicit(&c->bytes_written, memory_order_relaxed) +
               atomic_load_explicit(&c->bytes_read, memory_order_relaxed);
      if (c->fd >= 0 &&
          atomic_load_explicit(&c->status, memory_order_acquire) == ZBP_OK)
        count++;
    }
    if (before != after)
      idle_since = now_ns();
    if (w->runtime->config.idle == ZBP_IDLE_BUSY ||
        (w->runtime->config.idle == ZBP_IDLE_SPIN &&
         now_ns() - idle_since < (uint64_t)w->runtime->config.spin_us * 1000))
      continue;
    if (count > w->poll_capacity) {
      struct pollfd *fds = realloc(w->pollfds, count * sizeof(*fds));
      if (!fds) {
        for (zbp_connection *c = atomic_load(&w->connections); c; c = c->next)
          connection_fail(c, ZBP_NOMEM);
        continue;
      }
      w->pollfds = fds;
      w->poll_capacity = count;
    }
    w->pollfds[0] = (struct pollfd){w->event_fd, POLLIN, 0};
    size_t i = 1;
    uint64_t timeout = 1000000000;
    uint64_t now = now_ns();
    for (zbp_connection *c =
             atomic_load_explicit(&w->connections, memory_order_acquire);
         c; c = c->next) {
      if (c->fd < 0 ||
          atomic_load_explicit(&c->status, memory_order_acquire) != ZBP_OK)
        continue;
      short interest = POLLIN;
      if (c->want_write || c->tls_read_want_write ||
          (c->state == 3 && !c->write_want_read &&
           (c->output_start != c->output_end ||
            nghttp2_session_want_write(c->h2))))
        interest |= POLLOUT;
      w->pollfds[i++] = (struct pollfd){c->fd, interest, 0};
      if (c->state == 3 && !c->write_want_read && !c->tls_read_want_write &&
          (c->read_again || (c->ssl && SSL_pending(c->ssl) > 0)))
        timeout = 0;
      if (c->state < 3) {
        uint64_t left = c->deadline > now ? c->deadline - now : 0;
        if (left < timeout)
          timeout = left;
      }
      for (zbp_stream *s =
               atomic_load_explicit(&c->streams, memory_order_acquire);
           s; s = s->next) {
        if (!s->batch_start_ns)
          continue;
        uint64_t due =
            s->batch_start_ns + (uint64_t)s->config.batch_delay_us * 1000;
        uint64_t left = due > now ? due - now : 0;
        if (left < timeout)
          timeout = left;
      }
    }
    struct timespec timeout_ts = to_timespec(timeout);
    int result = ppoll(w->pollfds, i, &timeout_ts, NULL);
    if (result > 0) {
      for (zbp_connection *c =
               atomic_load_explicit(&w->connections, memory_order_acquire);
           c; c = c->next)
        atomic_fetch_add_explicit(&c->wakeups, 1, memory_order_relaxed);
    }
  }
  return NULL;
}

void zbp_runtime_config_init(zbp_runtime_config *config) {
  if (!config)
    return;
  *config = (zbp_runtime_config){.io_threads = 1,
                                 .idle = ZBP_IDLE_SLEEP,
                                 .spin_us = 50,
                                 .write_coalesce_bytes = 64 * 1024};
}

void zbp_connection_config_init(zbp_connection_config *config) {
  if (!config)
    return;
  *config = (zbp_connection_config){.port = 443, .connect_timeout_ms = 10000};
}

void zbp_stream_config_init(zbp_stream_config *config) {
  if (!config)
    return;
  *config = (zbp_stream_config){.queue = ZBP_QUEUE_MUTEX,
                                .batching = ZBP_BATCH_SINGLE,
                                .retention = ZBP_RELEASE_SENT,
                                .queue_capacity = 4096,
                                .buffer_bytes = 64 * 1024 * 1024,
                                .max_inflight = 4096,
                                .batch_bytes = 1024 * 1024,
                                .batch_records = 1024,
                                .admission_timeout_ms = 10000};
}

const char *zbp_version(void) { return "0.1.0"; }
const char *zbp_status_string(zbp_status status) {
  static const char *names[] = {"ok",
                                "invalid argument",
                                "out of memory",
                                "timeout",
                                "closed",
                                "I/O error",
                                "protocol error",
                                "authentication error",
                                "limit exceeded"};
  return (unsigned)status < sizeof(names) / sizeof(names[0]) ? names[status]
                                                             : "unknown";
}

zbp_status zbp_runtime_create(const zbp_runtime_config *config,
                              zbp_runtime **out) {
  if (!out)
    return ZBP_INVALID;
  *out = NULL;
  zbp_runtime_config defaults;
  if (!config) {
    zbp_runtime_config_init(&defaults);
    config = &defaults;
  }
  if (!config->io_threads || config->io_threads > 1024 ||
      config->idle > ZBP_IDLE_BUSY || config->idle < ZBP_IDLE_SLEEP ||
      config->write_coalesce_bytes > 16 * 1024 * 1024)
    return ZBP_INVALID;
  zbp_runtime *runtime = calloc(1, sizeof(*runtime));
  if (!runtime)
    return ZBP_NOMEM;
  runtime->config = *config;
  atomic_init(&runtime->stopping, 0);
  atomic_init(&runtime->next_worker, 0);
  runtime->workers = calloc(config->io_threads, sizeof(*runtime->workers));
  if (!runtime->workers) {
    free(runtime);
    return ZBP_NOMEM;
  }
  size_t created = 0;
  for (size_t i = 0; i < config->io_threads; i++) {
    worker *w = &runtime->workers[i];
    w->runtime = runtime;
    w->event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    atomic_init(&w->notified, 0);
    atomic_init(&w->connections, NULL);
    if (w->event_fd < 0 || pthread_create(&w->thread, NULL, worker_main, w)) {
      if (w->event_fd >= 0)
        close(w->event_fd);
      atomic_store_explicit(&runtime->stopping, 1, memory_order_release);
      for (size_t j = 0; j < created; j++)
        wake_worker(&runtime->workers[j]);
      for (size_t j = 0; j < created; j++) {
        pthread_join(runtime->workers[j].thread, NULL);
        close(runtime->workers[j].event_fd);
        free(runtime->workers[j].pollfds);
      }
      free(runtime->workers);
      free(runtime);
      return ZBP_IO;
    }
    created++;
  }
  *out = runtime;
  return ZBP_OK;
}

static void free_stream(zbp_stream *s) {
  stream_fail(s, ZBP_CLOSED);
  stream_cleanup(s);
  zbp_queue_destroy(&s->queue);
  pthread_cond_destroy(&s->wait_cond);
  pthread_mutex_destroy(&s->wait_mutex);
  free(s->descriptors);
  free(s->outstanding);
  free(s->create);
  free(s->rx);
  free(s->table);
  free(s);
}

static void free_connection(zbp_connection *c) {
  if (c->h2)
    nghttp2_session_del(c->h2);
  if (c->ssl)
    SSL_free(c->ssl);
  if (c->ssl_ctx)
    SSL_CTX_free(c->ssl_ctx);
  if (c->fd >= 0)
    close(c->fd);
  zbp_stream *s = atomic_load_explicit(&c->streams, memory_order_relaxed);
  while (s) {
    zbp_stream *next = s->next;
    free_stream(s);
    s = next;
  }
  if (c->addresses)
    freeaddrinfo(c->addresses);
  pthread_cond_destroy(&c->wait_cond);
  pthread_mutex_destroy(&c->wait_mutex);
  if (c->token) {
    OPENSSL_cleanse(c->token, strlen(c->token));
    free(c->token);
  }
  free(c->host);
  free(c->ca_file);
  free(c->output);
  free(c->input);
  free(c);
}

void zbp_runtime_destroy(zbp_runtime *runtime) {
  if (!runtime)
    return;
  atomic_store_explicit(&runtime->stopping, 1, memory_order_release);
  for (size_t i = 0; i < runtime->config.io_threads; i++)
    wake_worker(&runtime->workers[i]);
  for (size_t i = 0; i < runtime->config.io_threads; i++) {
    worker *w = &runtime->workers[i];
    pthread_join(w->thread, NULL);
    zbp_connection *c =
        atomic_load_explicit(&w->connections, memory_order_relaxed);
    while (c) {
      zbp_connection *next = c->next;
      free_connection(c);
      c = next;
    }
    close(w->event_fd);
    free(w->pollfds);
  }
  free(runtime->workers);
  free(runtime);
}

static int loopback_address(const struct addrinfo *address) {
  if (address->ai_family == AF_INET) {
    const struct sockaddr_in *v4 = (const struct sockaddr_in *)address->ai_addr;
    return (ntohl(v4->sin_addr.s_addr) >> 24) == 127;
  }
  if (address->ai_family == AF_INET6) {
    const struct sockaddr_in6 *v6 =
        (const struct sockaddr_in6 *)address->ai_addr;
    return IN6_IS_ADDR_LOOPBACK(&v6->sin6_addr);
  }
  return 0;
}

zbp_status zbp_connection_open(zbp_runtime *runtime,
                               const zbp_connection_config *config,
                               zbp_connection **out) {
  if (!out)
    return ZBP_INVALID;
  *out = NULL;
  if (!runtime || !config || !config->host || !*config->host ||
      strlen(config->host) > 900 || !config->token || !config->port ||
      !config->connect_timeout_ms || strchr(config->token, '\r') ||
      strchr(config->token, '\n'))
    return ZBP_INVALID;
  zbp_connection *c = calloc(1, sizeof(*c));
  if (!c)
    return ZBP_NOMEM;
  c->fd = -1;
  if (pthread_mutex_init(&c->wait_mutex, NULL)) {
    free(c);
    return ZBP_IO;
  }
  if (!init_cond(&c->wait_cond)) {
    pthread_mutex_destroy(&c->wait_mutex);
    free(c);
    return ZBP_IO;
  }
  atomic_init(&c->streams, NULL);
  atomic_init(&c->status, ZBP_OK);
  atomic_init(&c->ready, 0);
#define INITIALIZE_COUNTER(name) atomic_init(&c->name, 0);
  CONNECTION_COUNTERS(INITIALIZE_COUNTER)
  CONNECTION_DIAGNOSTICS(INITIALIZE_COUNTER)
#undef INITIALIZE_COUNTER
  atomic_init(&c->remote_connection_window, 0);
  atomic_init(&c->min_remote_stream_window, 0);
  atomic_init(&c->flow_stall_since_ns, 0);
  atomic_init(&c->flow_clock_version, 0);
  c->host = strdup(config->host);
  c->token = strdup(config->token);
  c->ca_file = config->ca_file ? strdup(config->ca_file) : NULL;
  c->port = config->port;
  c->plaintext = config->test_plaintext != 0;
  c->deadline = deadline_ms(config->connect_timeout_ms);
  c->output_capacity = runtime->config.write_coalesce_bytes;
  if (c->output_capacity && c->output_capacity < 32768)
    c->output_capacity = 32768;
  c->output = c->output_capacity ? malloc(c->output_capacity) : NULL;
  c->input = malloc(65536);
  if (!c->host || !c->token || !c->input || (config->ca_file && !c->ca_file) ||
      (c->output_capacity && !c->output)) {
    free_connection(c);
    return ZBP_NOMEM;
  }
  char port[6];
  snprintf(port, sizeof(port), "%u", c->port);
  struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
  if (getaddrinfo(c->host, port, &hints, &c->addresses)) {
    free_connection(c);
    return ZBP_IO;
  }
  c->address = c->addresses;
  if (c->plaintext) {
    for (struct addrinfo *address = c->addresses; address;
         address = address->ai_next)
      if (!loopback_address(address)) {
        free_connection(c);
        return ZBP_INVALID;
      }
  }
  size_t index = atomic_fetch_add_explicit(&runtime->next_worker, 1,
                                           memory_order_relaxed) %
                 runtime->config.io_threads;
  worker *w = c->worker = &runtime->workers[index];
  c->next = atomic_load_explicit(&w->connections, memory_order_relaxed);
  while (!atomic_compare_exchange_weak_explicit(&w->connections, &c->next, c,
                                                memory_order_release,
                                                memory_order_relaxed)) {
  }
  wake_worker(w);
  pthread_mutex_lock(&c->wait_mutex);
  struct timespec deadline = to_timespec(c->deadline);
  while (!atomic_load_explicit(&c->ready, memory_order_acquire) &&
         atomic_load(&c->status) == ZBP_OK) {
    if (pthread_cond_timedwait(&c->wait_cond, &c->wait_mutex, &deadline) ==
        ETIMEDOUT)
      break;
  }
  int ready = atomic_load_explicit(&c->ready, memory_order_acquire);
  zbp_status status =
      (zbp_status)atomic_load_explicit(&c->status, memory_order_acquire);
  pthread_mutex_unlock(&c->wait_mutex);
  if (!ready && status == ZBP_OK) {
    connection_fail(c, ZBP_TIMEOUT);
    wake_worker(w);
    return ZBP_TIMEOUT;
  }
  if (status != ZBP_OK)
    return status;
  *out = c;
  return ZBP_OK;
}

static int valid_stream_config(const zbp_stream_config *c) {
  return c->queue >= ZBP_QUEUE_MUTEX && c->queue <= ZBP_QUEUE_ATOMIC &&
         c->batching >= ZBP_BATCH_SINGLE && c->batching <= ZBP_BATCH_AUTO &&
         c->retention >= ZBP_RELEASE_SENT &&
         c->retention <= ZBP_RELEASE_ACKED && c->queue_capacity >= 2 &&
         !(c->queue_capacity & (c->queue_capacity - 1)) &&
         c->queue_capacity <= UINT32_MAX / 2 && c->max_inflight &&
         c->max_inflight <= UINT32_MAX / 2 - 2 && c->buffer_bytes >= 256 &&
         c->batch_bytes >= 64 && c->batch_bytes <= UINT32_MAX - 64 &&
         c->batch_records && c->batch_records <= UINT32_MAX;
}

zbp_status zbp_stream_open(zbp_connection *connection, const char *table,
                           const zbp_stream_config *config, zbp_stream **out) {
  if (!out)
    return ZBP_INVALID;
  *out = NULL;
  zbp_stream_config defaults;
  if (!config) {
    zbp_stream_config_init(&defaults);
    config = &defaults;
  }
  if (!connection || !table || !*table || strlen(table) > 65536 ||
      strchr(table, '\r') || strchr(table, '\n') ||
      !valid_stream_config(config))
    return ZBP_INVALID;
  int status = atomic_load_explicit(&connection->status, memory_order_acquire);
  if (status != ZBP_OK)
    return (zbp_status)status;
  zbp_stream *s = NULL;
  if (posix_memalign((void **)&s, _Alignof(zbp_stream), sizeof(*s)))
    return ZBP_NOMEM;
  memset(s, 0, sizeof(*s));
  s->connection = connection;
  s->config = *config;
  atomic_init(&s->status, ZBP_OK);
  atomic_init(&s->accepting, 1);
  atomic_init(&s->ready, 0);
  atomic_init(&s->close_requested, 0);
  atomic_init(&s->waiters, 0);
  atomic_init(&s->admissions, 0);
  atomic_init(&s->allocation_waiters, 0);
  atomic_init(&s->queue_waiters, 0);
  atomic_init(&s->barriers_pending, 0);
  atomic_init(&s->sample_counter, 0);
#define INITIALIZE_COUNTER(name) atomic_init(&s->name, 0);
  STREAM_COUNTERS(INITIALIZE_COUNTER)
#undef INITIALIZE_COUNTER
  for (unsigned i = 0; i < 32; i++)
    atomic_init(&s->latency_histogram[i], 0);
  if (pthread_mutex_init(&s->wait_mutex, NULL)) {
    free(s);
    return ZBP_IO;
  }
  if (!init_cond(&s->wait_cond)) {
    pthread_mutex_destroy(&s->wait_mutex);
    free(s);
    return ZBP_IO;
  }
  if (!zbp_queue_init(&s->queue, config->queue_capacity, config->queue)) {
    pthread_cond_destroy(&s->wait_cond);
    pthread_mutex_destroy(&s->wait_mutex);
    free(s);
    return ZBP_NOMEM;
  }
  s->descriptor_count = config->queue_capacity + config->max_inflight + 2;
  s->descriptors = calloc(s->descriptor_count, sizeof(*s->descriptors));
  s->outstanding = calloc(config->max_inflight, sizeof(*s->outstanding));
  s->table = strdup(table);
  s->create = zbp_create_message(table, &s->create_size);
  if (!s->descriptors || !s->outstanding || !s->table || !s->create) {
    s->cleaned = 1;
    free_stream(s);
    return ZBP_NOMEM;
  }
  for (size_t i = 0; i < s->descriptor_count; i++) {
    s->descriptors[i].index = (uint32_t)i;
    atomic_init(&s->descriptors[i].free_next,
                i + 1 == s->descriptor_count ? 0 : (uint32_t)i + 2);
    atomic_init(&s->descriptors[i].references, 0);
    atomic_init(&s->descriptors[i].done, 0);
  }
  atomic_init(&s->free_descriptors, 1);
  s->next = atomic_load_explicit(&connection->streams, memory_order_relaxed);
  while (!atomic_compare_exchange_weak_explicit(&connection->streams, &s->next,
                                                s, memory_order_release,
                                                memory_order_relaxed)) {
  }
  wake_worker(connection->worker);
  uint64_t expires = deadline_ms(
      config->admission_timeout_ms ? config->admission_timeout_ms : 10000);
  struct timespec deadline = to_timespec(expires);
  pthread_mutex_lock(&s->wait_mutex);
  atomic_fetch_add_explicit(&s->waiters, 1, memory_order_acq_rel);
  atomic_thread_fence(memory_order_seq_cst);
  while (!atomic_load_explicit(&s->ready, memory_order_acquire) &&
         atomic_load(&s->status) == ZBP_OK) {
    if (pthread_cond_timedwait(&s->wait_cond, &s->wait_mutex, &deadline) ==
        ETIMEDOUT)
      break;
  }
  int ready = atomic_load_explicit(&s->ready, memory_order_acquire);
  status = atomic_load_explicit(&s->status, memory_order_acquire);
  atomic_fetch_sub_explicit(&s->waiters, 1, memory_order_acq_rel);
  pthread_mutex_unlock(&s->wait_mutex);
  if (!ready && status == ZBP_OK) {
    stream_fail(s, ZBP_TIMEOUT);
    wake_worker(connection->worker);
    return ZBP_TIMEOUT;
  }
  if (status != ZBP_OK)
    return (zbp_status)status;
  *out = s;
  return ZBP_OK;
}

static zbp_status admission_status(zbp_stream *s, int barrier) {
  int status = atomic_load_explicit(&s->status, memory_order_acquire);
  if (status != ZBP_OK)
    return (zbp_status)status;
  if (!barrier && !atomic_load_explicit(&s->accepting, memory_order_acquire))
    return ZBP_CLOSED;
  return ZBP_OK;
}

typedef struct {
  uint64_t deadline, start;
  int waiting;
  unsigned timeout, reason;
} wait_state;

static void end_wait(zbp_stream *s, wait_state *wait) {
  if (!wait->waiting)
    return;
  atomic_fetch_add_explicit(&s->backpressure_ns, now_ns() - wait->start,
                            memory_order_relaxed);
  atomic_fetch_sub_explicit(wait->reason == CAPACITY_ALLOCATION
                                ? &s->allocation_waiters : &s->queue_waiters,
                            1, memory_order_acq_rel);
  atomic_fetch_sub_explicit(&s->waiters, 1, memory_order_acq_rel);
  pthread_mutex_unlock(&s->wait_mutex);
  wait->waiting = 0;
}

/* The first call locks and requests a recheck; later calls sleep atomically. */
static int wait_capacity(zbp_stream *s, wait_state *wait, unsigned reason) {
  if (!wait->timeout || now_ns() >= wait->deadline)
    return 0;
  if (!wait->waiting) {
    wait->start = now_ns();
    pthread_mutex_lock(&s->wait_mutex);
    atomic_fetch_add_explicit(&s->waiters, 1, memory_order_acq_rel);
    atomic_fetch_add_explicit(reason == CAPACITY_ALLOCATION
                                  ? &s->allocation_waiters : &s->queue_waiters,
                              1, memory_order_acq_rel);
    atomic_thread_fence(memory_order_seq_cst);
    atomic_fetch_add_explicit(&s->backpressure_waits, 1, memory_order_relaxed);
    wait->waiting = 1;
    wait->reason = reason;
    return 1;
  }
  struct timespec deadline = to_timespec(wait->deadline);
  return pthread_cond_timedwait(&s->wait_cond, &s->wait_mutex, &deadline) !=
         ETIMEDOUT;
}

static zbp_status allocate_request(zbp_stream *s, size_t size, int barrier,
                                   wait_state *wait, request **out) {
  request *r = NULL;
  for (;;) {
    zbp_status status = admission_status(s, barrier);
    if (status != ZBP_OK) {
      if (r)
        descriptor_put(s, r);
      end_wait(s, wait);
      notify_capacity(s, CAPACITY_ALLOCATION);
      return status;
    }
    if (!r)
      r = descriptor_get(s);
    uint64_t used = 0;
    if (r && (barrier || zbp_payload_reserve(&s->buffered_bytes,
                                            s->config.buffer_bytes, size,
                                            &used))) {
      if (!barrier) {
        r->size = size;
        r->payload = malloc(size);
        if (!r->payload) {
          zbp_payload_release(&s->buffered_bytes, size);
          descriptor_put(s, r);
          end_wait(s, wait);
          notify_capacity(s, CAPACITY_ALLOCATION);
          return ZBP_NOMEM;
        }
        peak_update(&s->peak_buffered_bytes, used);
      }
      end_wait(s, wait);
      *out = r;
      return ZBP_OK;
    }
    if (!wait_capacity(s, wait, CAPACITY_ALLOCATION)) {
      if (r)
        descriptor_put(s, r);
      end_wait(s, wait);
      notify_capacity(s, CAPACITY_ALLOCATION);
      return ZBP_TIMEOUT;
    }
  }
}

static zbp_status enqueue_request(zbp_stream *s, request *r, int barrier,
                                  wait_state *wait) {
  for (;;) {
    zbp_status status = admission_status(s, barrier);
    if (status != ZBP_OK) {
      end_wait(s, wait);
      return status;
    }
    if (wait->timeout && now_ns() >= wait->deadline) {
      end_wait(s, wait);
      return ZBP_TIMEOUT;
    }
    if (zbp_queue_push(&s->queue, r)) {
      end_wait(s, wait);
      wake_worker(s->connection->worker);
      return ZBP_OK;
    }
    if (!wait_capacity(s, wait, CAPACITY_QUEUE)) {
      end_wait(s, wait);
      return ZBP_TIMEOUT;
    }
  }
}

static void end_admission(zbp_stream *s) {
  if (atomic_fetch_sub_explicit(&s->admissions, 1, memory_order_acq_rel) == 1 &&
      atomic_load_explicit(&s->status, memory_order_acquire) != ZBP_OK)
    wake_worker(s->connection->worker);
}

static zbp_status ingest(zbp_stream *s, const zbp_record *records, size_t count,
                         int batch) {
  if (!s || !records || !count)
    return ZBP_INVALID;
  wait_state wait = {.deadline = deadline_ms(s->config.admission_timeout_ms),
                     .timeout = s->config.admission_timeout_ms};
  size_t size = 0, bytes = 0;
  for (size_t i = 0; i < count; i++) {
    if (!records[i].data || !records[i].size)
      return ZBP_INVALID;
    if (records[i].size > UINT32_MAX || records[i].size > SIZE_MAX - bytes)
      return ZBP_LIMIT;
    bytes += records[i].size;
    size_t addition =
        records[i].size + (batch ? 1 + zbp_varint_size(records[i].size) : 0);
    if (addition > SIZE_MAX - size)
      return ZBP_LIMIT;
    size += addition;
  }
  size_t body = size;
  if (!batch && s->config.batching == ZBP_BATCH_AUTO)
    body += 1 + zbp_varint_size(size);
  if (encoded_size(body, INT64_MAX) > s->config.batch_bytes ||
      size > s->config.buffer_bytes)
    return ZBP_LIMIT;
  atomic_fetch_add_explicit(&s->admissions, 1, memory_order_acq_rel);
  request *r = NULL;
  zbp_status result = allocate_request(s, size, 0, &wait, &r);
  if (result != ZBP_OK) {
    end_admission(s);
    return result;
  }
  size_t cursor = 0;
  for (size_t i = 0; i < count; i++) {
    if (batch)
      cursor += zbp_record_prefix(r->payload + cursor, records[i].size);
    memcpy(r->payload + cursor, records[i].data, records[i].size);
    cursor += records[i].size;
  }
  r->size = size;
  r->bytes = bytes;
  r->records = count;
  r->explicit_batch = batch;
  if (s->config.sample_every &&
      atomic_fetch_add_explicit(&s->sample_counter, 1, memory_order_relaxed) %
              s->config.sample_every ==
          0)
    r->sample_ns = wait.deadline - (uint64_t)wait.timeout * 1000000;
  result = enqueue_request(s, r, 0, &wait);
  if (result != ZBP_OK) {
    request_put(s, r);
    notify_capacity(s, CAPACITY_ALLOCATION);
  } else {
    atomic_fetch_add_explicit(&s->admitted_records, count,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&s->admitted_bytes, bytes, memory_order_relaxed);
  }
  end_admission(s);
  return result;
}

zbp_status zbp_ingest(zbp_stream *stream, const void *json, size_t size) {
  zbp_record record = {json, size};
  return ingest(stream, &record, 1, 0);
}

zbp_status zbp_ingest_batch(zbp_stream *stream, const zbp_record *records,
                            size_t count) {
  return ingest(stream, records, count, 1);
}

zbp_status zbp_flush(zbp_stream *s, unsigned timeout_ms) {
  if (!s)
    return ZBP_INVALID;
  zbp_status status = admission_status(s, 1);
  if (status != ZBP_OK)
    return status;
  wait_state wait = {.deadline = deadline_ms(timeout_ms),
                     .timeout = timeout_ms};
  atomic_fetch_add_explicit(&s->admissions, 1, memory_order_acq_rel);
  request *r = NULL;
  status = allocate_request(s, 0, 1, &wait, &r);
  if (status != ZBP_OK) {
    end_admission(s);
    return status;
  }
  r->barrier = 1;
  atomic_store_explicit(&r->references, 2, memory_order_relaxed);
  atomic_store_explicit(&r->done, 0, memory_order_relaxed);
  atomic_fetch_add_explicit(&s->barriers_pending, 1, memory_order_relaxed);
  status = enqueue_request(s, r, 1, &wait);
  if (status != ZBP_OK) {
    atomic_fetch_sub_explicit(&s->barriers_pending, 1, memory_order_relaxed);
    descriptor_put(s, r);
    notify_capacity(s, CAPACITY_ALLOCATION);
    end_admission(s);
    return status;
  }
  end_admission(s);
  pthread_mutex_lock(&s->wait_mutex);
  atomic_fetch_add_explicit(&s->waiters, 1, memory_order_acq_rel);
  atomic_thread_fence(memory_order_seq_cst);
  struct timespec deadline = to_timespec(wait.deadline);
  while (!atomic_load_explicit(&r->done, memory_order_acquire) &&
         atomic_load(&s->status) == ZBP_OK) {
    if (pthread_cond_timedwait(&s->wait_cond, &s->wait_mutex, &deadline) ==
        ETIMEDOUT)
      break;
  }
  status = (zbp_status)atomic_load_explicit(&s->status, memory_order_acquire);
  if (status == ZBP_OK)
    status = atomic_load_explicit(&r->done, memory_order_acquire) ? r->result
                                                                  : ZBP_TIMEOUT;
  atomic_fetch_sub_explicit(&s->waiters, 1, memory_order_acq_rel);
  pthread_mutex_unlock(&s->wait_mutex);
  barrier_release(s, r);
  return status;
}

zbp_status zbp_stream_close(zbp_stream *s, unsigned timeout_ms) {
  if (!s)
    return ZBP_INVALID;
  atomic_store_explicit(&s->accepting, 0, memory_order_release);
  notify_waiters(s);
  zbp_status status = zbp_flush(s, timeout_ms);
  if (status != ZBP_OK)
    return status;
  atomic_store_explicit(&s->close_requested, 1, memory_order_release);
  int expected = ZBP_OK;
  if (!atomic_compare_exchange_strong_explicit(&s->status, &expected,
                                               ZBP_CLOSED, memory_order_acq_rel,
                                               memory_order_relaxed))
    return (zbp_status)expected;
  wake_worker(s->connection->worker);
  return ZBP_OK;
}

void zbp_stream_abort(zbp_stream *s) {
  if (!s)
    return;
  stream_fail(s, ZBP_CLOSED);
  wake_worker(s->connection->worker);
}

void zbp_stream_get_stats(const zbp_stream *s, zbp_stream_stats *out) {
  if (!out)
    return;
  memset(out, 0, sizeof(*out));
  if (!s) {
    out->status = ZBP_INVALID;
    return;
  }
#define COPY_COUNTER(name)                                                     \
  out->name = atomic_load_explicit(&s->name, memory_order_relaxed);
  STREAM_COUNTERS(COPY_COUNTER)
#undef COPY_COUNTER
  out->queue_depth =
      atomic_load_explicit(&s->queue.count, memory_order_relaxed);
  out->peak_queue_depth =
      atomic_load_explicit(&s->queue.peak, memory_order_relaxed);
  for (unsigned i = 0; i < 32; i++)
    out->latency_histogram[i] =
        atomic_load_explicit(&s->latency_histogram[i], memory_order_relaxed);
  out->status =
      (zbp_status)atomic_load_explicit(&s->status, memory_order_acquire);
}

void zbp_connection_get_stats(const zbp_connection *c,
                              zbp_connection_stats *out) {
  if (!out)
    return;
  memset(out, 0, sizeof(*out));
  if (!c) {
    out->status = ZBP_INVALID;
    return;
  }
#define COPY_COUNTER(name)                                                     \
  out->name = atomic_load_explicit(&c->name, memory_order_relaxed);
  CONNECTION_COUNTERS(COPY_COUNTER)
#undef COPY_COUNTER
  out->status =
      (zbp_status)atomic_load_explicit(&c->status, memory_order_acquire);
}

void zbp_connection_get_diagnostics(const zbp_connection *c,
                                    zbp_connection_diagnostics *out) {
  if (!out)
    return;
  memset(out, 0, sizeof(*out));
  if (!c)
    return;
#define COPY_DIAGNOSTIC(name) \
  out->name = atomic_load_explicit(&c->name, memory_order_relaxed)
  COPY_DIAGNOSTIC(peer_initial_stream_window);
  COPY_DIAGNOSTIC(peer_max_frame_size);
  COPY_DIAGNOSTIC(remote_connection_window);
  COPY_DIAGNOSTIC(min_remote_stream_window);
  COPY_DIAGNOSTIC(peak_remote_connection_window);
  COPY_DIAGNOSTIC(peak_remote_stream_window);
  COPY_DIAGNOSTIC(write_would_block);
  COPY_DIAGNOSTIC(send_budget_yields);
  COPY_DIAGNOSTIC(stream_window_updates);
  COPY_DIAGNOSTIC(min_stream_window_update);
  COPY_DIAGNOSTIC(max_stream_window_update);
#undef COPY_DIAGNOSTIC
  out->write_want_read = atomic_load_explicit(&c->write_want_read_count,
                                              memory_order_relaxed);
  /* One observed clock value must not double-count a stall being completed. */
  for (;;) {
    unsigned version = atomic_load_explicit(&c->flow_clock_version,
                                             memory_order_acquire);
    if (version & 1)
      continue;
    uint64_t total =
        atomic_load_explicit(&c->flow_control_ns, memory_order_relaxed);
    uint64_t since =
        atomic_load_explicit(&c->flow_stall_since_ns, memory_order_relaxed);
    uint64_t observed = total + (since ? now_ns() - since : 0);
    atomic_thread_fence(memory_order_acquire);
    if (version == atomic_load_explicit(&c->flow_clock_version,
                                        memory_order_acquire)) {
      out->flow_control_ns = observed;
      break;
    }
  }
}
