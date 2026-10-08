#include <string.h>

#include "internal/common/record.h"
#include "test_common.h"

/* ---- tests ------------------------------------------------------------- */

static void test_record_owns_exact_length_copy(void)
{
    char json[] = {'{', '}'};
    zb_record_t *record =
        zb_record_new((zerobus_string_view_t){json, sizeof(json)});
    REQUIRE(record != NULL);
    REQUIRE(record->json != NULL);
    CHECK_EQ_INT(record->len, sizeof(json));
    CHECK(record->json != json);
    memset(json, 'x', sizeof(json));
    CHECK(memcmp(record->json, "{}", sizeof(json)) == 0);
    CHECK_EQ_INT(record->json[sizeof(json)], '\0');

zb_cleanup:
    zb_record_free(record);
}

static void test_empty_records(void)
{
    zb_record_t *records[2] = {NULL};
    for (unsigned int i = 0; i < 2; i++) {
        records[i] =
            zb_record_new((zerobus_string_view_t){i == 0 ? NULL : "x", 0});
        REQUIRE(records[i] != NULL);
        CHECK_EQ_INT(records[i]->len, 0);
        CHECK(records[i]->json == NULL);
    }

zb_cleanup:
    zb_record_free(records[0]);
    zb_record_free(records[1]);
}

static void test_null_input_and_free(void)
{
    zb_record_t *record = zb_record_new((zerobus_string_view_t){NULL, 1});
    CHECK(record == NULL);
    zb_record_free(record);
    zb_record_free(NULL);
}

int main(void)
{
    test_record_owns_exact_length_copy();
    test_empty_records();
    test_null_input_and_free();
    TEST_MAIN_RETURN();
}
