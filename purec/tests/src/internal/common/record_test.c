#include <stdint.h>
#include <string.h>

#include "internal/common/record.h"
#include "test_common.h"

/* ---- tests ------------------------------------------------------------- */

static void test_request_owns_exact_length_copy(void)
{
    char data[] = {'a', '\0', 'b'};
    zb_ingest_request_t *request =
        zb_ingest_request_new((zerobus_string_view_t){data, sizeof(data)});
    REQUIRE(request != NULL);
    CHECK_EQ_INT(request->len, sizeof(data));
    CHECK(request->data != data);
    memset(data, 'x', sizeof(data));
    CHECK(memcmp(request->data, "a\0b", sizeof(data)) == 0);
    CHECK_EQ_INT(request->data[sizeof(data)], '\0');

zb_cleanup:
    zb_ingest_request_free(request);
}

static void test_empty_requests(void)
{
    zb_ingest_request_t *requests[2] = {NULL};
    for (unsigned int i = 0; i < 2; i++) {
        requests[i] = zb_ingest_request_new(
            (zerobus_string_view_t){i == 0 ? NULL : "x", 0});
        REQUIRE(requests[i] != NULL);
        CHECK_EQ_INT(requests[i]->len, 0);
        CHECK_EQ_INT(requests[i]->data[0], '\0');
    }

zb_cleanup:
    zb_ingest_request_free(requests[0]);
    zb_ingest_request_free(requests[1]);
}

static void test_null_input_and_free(void)
{
    zb_ingest_request_t *request =
        zb_ingest_request_new((zerobus_string_view_t){NULL, 1});
    CHECK(request == NULL);
    zb_ingest_request_free(request);
    zb_ingest_request_free(NULL);
}

static void test_allocation_size_overflow(void)
{
    const size_t lengths[] = {SIZE_MAX, SIZE_MAX - sizeof(zb_ingest_request_t)};
    for (unsigned int i = 0; i < 2; i++) {
        zb_ingest_request_t *request =
            zb_ingest_request_new((zerobus_string_view_t){"x", lengths[i]});
        CHECK(request == NULL);
        zb_ingest_request_free(request);
    }
}

int main(void)
{
    test_request_owns_exact_length_copy();
    test_empty_requests();
    test_null_input_and_free();
    test_allocation_size_overflow();
    TEST_MAIN_RETURN();
}
