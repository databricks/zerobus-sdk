/* Unit tests for the message channel (network/channel.c). */
#define _GNU_SOURCE

#include <stdlib.h>
#include <unistd.h>

#include "internal/network/channel.h"
#include "test_common.h"

enum {
    TIMEOUT_MS = 5000,
    SHORT_TIMEOUT_MS = 20,
    MESSAGES = 10000,
    LARGE_CAPACITY = 9,
};

/* ---- helpers ----------------------------------------------------------- */

/* A one-byte message holding value. */
static zb_message_t *message_new(uint8_t value)
{
    zb_message_t *message = malloc(sizeof(*message));
    uint8_t *data = malloc(1);
    if (message == NULL || data == NULL) {
        free(message);
        free(data);
        return NULL;
    }
    data[0] = value;
    *message = (zb_message_t){data, 1};
    return message;
}

static void message_free(zb_message_t *message)
{
    if (message != NULL) {
        free(message->data);
        free(message);
    }
}

/* Pops the next one-byte message and returns its byte, -1 at EOF, or -2 if
 * none arrives in time. */
static int pop_value(zb_channel_t *channel)
{
    zb_message_t *message = NULL;
    if (zb_channel_pop(channel, zb_deadline_after_ms(TIMEOUT_MS), &message) !=
        ZEROBUS_STATUS_OK) {
        return -2;
    }
    int value = message != NULL ? message->data[0] : -1;
    message_free(message);
    return value;
}

/* Whether wakeup is signalled, waiting up to timeout_ms for it. Clears the
 * signal. */
static bool take_signal(zb_wakeup_t *wakeup, uint64_t timeout_ms)
{
    zb_deadline_t deadline = zb_deadline_after_ms(timeout_ms);
    zb_net_interest_t interest = {ZB_SOCKET_INVALID, 0, deadline};
    unsigned events = 0;
    bool signaled =
        zb_net_wait(&interest, wakeup, &events) == ZEROBUS_STATUS_OK &&
        !zb_deadline_expired(deadline);
    (void)zb_wakeup_drain(wakeup);
    return signaled;
}

/* Closes the channel a moment later, once the test thread waits on it. */
static void *close_soon(void *channel)
{
    usleep(SHORT_TIMEOUT_MS * 1000);
    (void)zb_channel_close(channel);
    return NULL;
}

/* Pushes MESSAGES messages holding their index, then closes the channel. */
static void *produce(void *channel)
{
    for (int i = 0; i < MESSAGES; ++i) {
        zb_message_t *message = message_new((uint8_t)i);
        if (zb_channel_push(channel, message,
                            zb_deadline_after_ms(TIMEOUT_MS)) !=
            ZEROBUS_STATUS_OK) {
            message_free(message);
            break;
        }
    }
    (void)zb_channel_close(channel);
    return NULL;
}

/* ---- tests ------------------------------------------------------------- */

static void test_arguments(void)
{
    zb_channel_t *channel = NULL;
    zb_message_t *out = NULL;
    zb_message_t no_data = {NULL, 1};
    CHECK_EQ_INT(zb_channel_new(1, NULL, NULL, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_channel_new(0, NULL, NULL, &channel),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_channel_new(ZB_SEM_VALUE_MAX, NULL, NULL, &channel),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK(channel == NULL);
    REQUIRE_OK(zb_channel_new(1, NULL, NULL, &channel));

    CHECK_EQ_INT(zb_channel_try_push(NULL, &no_data),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_channel_try_push(channel, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    /* A length needs data. */
    CHECK_EQ_INT(zb_channel_push(channel, &no_data, ZB_DEADLINE_INFINITE),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_channel_try_pop(NULL, &out),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_channel_pop(channel, ZB_DEADLINE_INFINITE, NULL),
                 ZEROBUS_STATUS_INVALID_ARGUMENT);
    CHECK_EQ_INT(zb_channel_close(NULL), ZEROBUS_STATUS_INVALID_ARGUMENT);
    zb_channel_free(NULL);

zb_cleanup:
    zb_channel_free(channel);
}

/* Messages come out in order, and capacity counts messages, empty ones too.
 * Failures leave the message with the caller and the output unchanged. */
static void test_order_and_capacity(void)
{
    zb_channel_t *channel = NULL;
    zb_message_t *extra = message_new(3);
    zb_message_t *empty = calloc(1, sizeof(*empty));
    zb_message_t *queued_empty = empty;
    zb_message_t *out = NULL;
    REQUIRE(extra != NULL && empty != NULL);
    REQUIRE_OK(zb_channel_new(3, NULL, NULL, &channel));

    CHECK_OK(zb_channel_try_push(channel, message_new(1)));
    REQUIRE_OK(zb_channel_try_push(channel, empty));
    empty = NULL;
    CHECK_OK(zb_channel_try_push(channel, message_new(2)));
    CHECK_EQ_INT(zb_channel_try_push(channel, extra),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(
        zb_channel_push(channel, extra, zb_deadline_after_ms(SHORT_TIMEOUT_MS)),
        ZEROBUS_STATUS_DEADLINE_EXCEEDED);

    CHECK_EQ_INT(pop_value(channel), 1);
    CHECK_OK(zb_channel_try_pop(channel, &out));
    CHECK(out == queued_empty);
    message_free(out);
    CHECK_EQ_INT(pop_value(channel), 2);

    out = extra;
    CHECK_EQ_INT(zb_channel_try_pop(channel, &out),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK_EQ_INT(
        zb_channel_pop(channel, zb_deadline_after_ms(SHORT_TIMEOUT_MS), &out),
        ZEROBUS_STATUS_DEADLINE_EXCEEDED);
    CHECK(out == extra);

    /* There is room again. Free takes the message left queued. */
    CHECK_OK(zb_channel_try_push(channel, extra));
    extra = NULL;

zb_cleanup:
    message_free(extra);
    free(empty);
    zb_channel_free(channel);
}

/* A larger channel holds exactly max_messages, not the room its queue grows
 * to, and keeps the order over rounds that wrap around the queue. */
static void test_large_capacity(void)
{
    zb_channel_t *channel = NULL;
    zb_message_t *extra = message_new(0);
    REQUIRE(extra != NULL);
    REQUIRE_OK(zb_channel_new(LARGE_CAPACITY, NULL, NULL, &channel));

    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < LARGE_CAPACITY; ++i) {
            REQUIRE_OK(zb_channel_try_push(channel, message_new((uint8_t)i)));
        }
        CHECK_EQ_INT(zb_channel_try_push(channel, extra),
                     ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
        for (int i = 0; i < LARGE_CAPACITY; ++i) {
            REQUIRE_EQ_INT(pop_value(channel), i);
        }
    }

zb_cleanup:
    message_free(extra);
    zb_channel_free(channel);
}

/* After close, pushes are refused, queued messages still come out in order,
 * and then every pop reports EOF. */
static void test_close(void)
{
    zb_channel_t *channel = NULL;
    zb_message_t *refused = message_new(3);
    zb_message_t *out = refused;
    REQUIRE(refused != NULL);
    REQUIRE_OK(zb_channel_new(4, NULL, NULL, &channel));
    CHECK_OK(zb_channel_try_push(channel, message_new(1)));
    CHECK_OK(zb_channel_try_push(channel, message_new(2)));
    CHECK_OK(zb_channel_close(channel));
    CHECK_OK(zb_channel_close(channel));

    CHECK_EQ_INT(zb_channel_try_push(channel, refused),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK_EQ_INT(zb_channel_push(channel, refused, ZB_DEADLINE_INFINITE),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    CHECK_EQ_INT(pop_value(channel), 1);
    CHECK_EQ_INT(pop_value(channel), 2);
    CHECK_EQ_INT(pop_value(channel), -1);
    CHECK_EQ_INT(pop_value(channel), -1);
    CHECK_OK(zb_channel_try_pop(channel, &out));
    CHECK(out == NULL);

zb_cleanup:
    message_free(refused);
    zb_channel_free(channel);
}

/* Close wakes a consumer waiting on an empty channel, and a producer waiting
 * on a full one. */
static void test_close_wakes_waiters(void)
{
    zb_channel_t *empty = NULL;
    zb_channel_t *full = NULL;
    zb_message_t *blocked = message_new(2);
    zb_message_t *out = blocked;
    zb_thread_t closer;
    REQUIRE(blocked != NULL);
    REQUIRE_OK(zb_channel_new(1, NULL, NULL, &empty));
    REQUIRE_OK(zb_channel_new(1, NULL, NULL, &full));
    REQUIRE_OK(zb_channel_try_push(full, message_new(1)));

    REQUIRE_OK(zb_thread_create(&closer, close_soon, empty));
    CHECK_OK(zb_channel_pop(empty, ZB_DEADLINE_INFINITE, &out));
    CHECK(out == NULL);
    (void)zb_thread_join(&closer, NULL);

    REQUIRE_OK(zb_thread_create(&closer, close_soon, full));
    CHECK_EQ_INT(zb_channel_push(full, blocked, ZB_DEADLINE_INFINITE),
                 ZEROBUS_STATUS_FAILED_PRECONDITION);
    (void)zb_thread_join(&closer, NULL);

zb_cleanup:
    message_free(blocked);
    zb_channel_free(full);
    zb_channel_free(empty);
}

/* A push signals the push wakeup and a pop the pop wakeup; a failed pop
 * signals neither, and close signals both. */
static void test_wakeups(void)
{
    zb_wakeup_t *pushed = NULL;
    zb_wakeup_t *popped = NULL;
    zb_channel_t *channel = NULL;
    zb_message_t *out = NULL;
    REQUIRE_OK(zb_wakeup_new(&pushed));
    REQUIRE_OK(zb_wakeup_new(&popped));
    REQUIRE_OK(zb_channel_new(1, pushed, popped, &channel));

    CHECK_OK(zb_channel_try_push(channel, message_new(1)));
    CHECK(take_signal(pushed, TIMEOUT_MS));
    CHECK(!take_signal(popped, SHORT_TIMEOUT_MS));
    CHECK_EQ_INT(pop_value(channel), 1);
    CHECK(take_signal(popped, TIMEOUT_MS));
    CHECK(!take_signal(pushed, SHORT_TIMEOUT_MS));

    CHECK_EQ_INT(zb_channel_try_pop(channel, &out),
                 ZEROBUS_STATUS_RESOURCE_EXHAUSTED);
    CHECK(!take_signal(popped, SHORT_TIMEOUT_MS));
    CHECK_OK(zb_channel_close(channel));
    CHECK(take_signal(pushed, TIMEOUT_MS));
    CHECK(take_signal(popped, TIMEOUT_MS));

zb_cleanup:
    zb_channel_free(channel);
    zb_wakeup_free(popped);
    zb_wakeup_free(pushed);
}

/* A producer and a consumer on two threads, through a channel far smaller
 * than the stream: every message arrives once and in order, then EOF. */
static void test_threads(void)
{
    zb_channel_t *channel = NULL;
    zb_thread_t producer;
    bool started = false;
    int received = 0;
    REQUIRE_OK(zb_channel_new(4, NULL, NULL, &channel));
    REQUIRE_OK(zb_thread_create(&producer, produce, channel));
    started = true;

    for (int value; (value = pop_value(channel)) >= 0; ++received) {
        REQUIRE_EQ_INT(value, (uint8_t)received);
    }
    CHECK_EQ_INT(received, MESSAGES);

zb_cleanup:
    if (started) {
        /* Unblocks the producer if the consumer stopped early. */
        (void)zb_channel_close(channel);
        (void)zb_thread_join(&producer, NULL);
    }
    zb_channel_free(channel);
}

int main(void)
{
    test_arguments();
    test_order_and_capacity();
    test_large_capacity();
    test_close();
    test_close_wakes_waiters();
    test_wakeups();
    test_threads();
    TEST_MAIN_RETURN();
}
