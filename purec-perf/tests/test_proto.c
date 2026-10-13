#include "proto.h"
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
  unsigned char buffer[128];
  uint64_t values[] = {0, 1, 127, 128, 16383, 16384, UINT64_MAX};
  for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
    size_t size = zbp_varint_write(buffer, values[i]);
    assert(size == zbp_varint_size(values[i]));
    const unsigned char *p = buffer;
    uint64_t value = 0;
    assert(zbp_varint_read(&p, buffer + size, &value));
    assert(value == values[i] && p == buffer + size);
    p = buffer;
    assert(!zbp_varint_read(&p, buffer + size - 1, &value));
  }
  memset(buffer, 0xff, 10);
  const unsigned char *p = buffer;
  uint64_t value = 0;
  assert(!zbp_varint_read(&p, buffer + 10, &value));
  const unsigned char single[] = {0, 0, 0, 0, 8, 0x12, 6, 8, 0, 0x1a, 2};
  assert(zbp_wire_header(buffer, 0, 2, 0) == sizeof(single));
  assert(!memcmp(buffer, single, sizeof(single)));
  const unsigned char batch[] = {0, 0, 0, 0, 10, 0x1a, 8, 8, 1, 0x1a, 4};
  assert(zbp_wire_header(buffer, 1, 4, 1) == sizeof(batch));
  assert(!memcmp(buffer, batch, sizeof(batch)));
  size_t size = 0;
  unsigned char *create = zbp_create_message("a.b.c", &size);
  const unsigned char expected[] = {0, 0,   0,   0,   11,  10,  9,  10,
                                    5, 'a', '.', 'b', '.', 'c', 32, 2};
  assert(create && size == sizeof(expected) && !memcmp(create, expected, size));
  free(create);
  const unsigned char create_response[] = {10, 3, 10, 1, 'x'};
  assert(zbp_parse_response(create_response, sizeof(create_response), &value) ==
         1);
  const unsigned char ack[] = {18, 3, 8, 0x80, 1};
  assert(zbp_parse_response(ack, sizeof(ack), &value) == 2 && value == 128);
  const unsigned char missing_ack[] = {18, 0};
  assert(zbp_parse_response(missing_ack, sizeof(missing_ack), &value) == -1);
  const unsigned char unknown[] = {32, 4, 18, 2, 8, 0};
  assert(zbp_parse_response(unknown, sizeof(unknown), &value) == 2 &&
         value == 0);
  const unsigned char truncated[] = {18, 3, 8, 128};
  assert(zbp_parse_response(truncated, sizeof(truncated), &value) == -1);
  const unsigned char invalid_wire[] = {19, 8};
  assert(zbp_parse_response(invalid_wire, sizeof(invalid_wire), &value) == -1);
  puts("proto: exact envelopes, varint boundaries, unknown fields, malformed "
       "input passed");
  return 0;
}
