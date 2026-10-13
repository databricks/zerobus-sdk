#include "proto.h"
#include <limits.h>
#include <stdlib.h>
#include <string.h>

size_t zbp_varint_size(uint64_t value) {
  size_t size = 1;
  while (value > 127) {
    size++;
    value >>= 7;
  }
  return size;
}

size_t zbp_varint_write(unsigned char *out, uint64_t value) {
  size_t n = 0;
  do {
    out[n++] = (unsigned char)((value & 127) | (value > 127 ? 128 : 0));
    value >>= 7;
  } while (value);
  return n;
}

int zbp_varint_read(const unsigned char **data, const unsigned char *end,
                    uint64_t *out) {
  uint64_t value = 0;
  for (unsigned shift = 0; shift < 64 && *data != end; shift += 7) {
    unsigned char c = *(*data)++;
    if (shift == 63 && c > 1)
      return 0;
    value |= (uint64_t)(c & 127) << shift;
    if (!(c & 128)) {
      *out = value;
      return 1;
    }
  }
  return 0;
}

size_t zbp_record_prefix(unsigned char *out, size_t length) {
  out[0] = 0x0a;
  return 1 + zbp_varint_write(out + 1, length);
}

static void grpc_header(unsigned char *out, size_t size) {
  out[0] = 0;
  out[1] = (unsigned char)(size >> 24);
  out[2] = (unsigned char)(size >> 16);
  out[3] = (unsigned char)(size >> 8);
  out[4] = (unsigned char)size;
}

size_t zbp_wire_header(unsigned char *out, uint64_t offset, size_t body,
                       int batch) {
  size_t inner = 1 + zbp_varint_size(offset) + 1 + zbp_varint_size(body) + body;
  size_t outer = 1 + zbp_varint_size(inner) + inner;
  if (outer > UINT32_MAX)
    return 0;
  grpc_header(out, outer);
  size_t n = 5;
  out[n++] = batch ? 0x1a : 0x12;
  n += zbp_varint_write(out + n, inner);
  out[n++] = 0x08;
  n += zbp_varint_write(out + n, offset);
  out[n++] = 0x1a;
  n += zbp_varint_write(out + n, body);
  return n;
}

unsigned char *zbp_create_message(const char *table, size_t *size) {
  size_t length = strlen(table);
  if (length > 65536)
    return NULL;
  size_t inner = 1 + zbp_varint_size(length) + length + 2;
  size_t outer = 1 + zbp_varint_size(inner) + inner;
  unsigned char *out = malloc(outer + 5);
  if (!out)
    return NULL;
  grpc_header(out, outer);
  size_t n = 5;
  out[n++] = 0x0a;
  n += zbp_varint_write(out + n, inner);
  out[n++] = 0x0a;
  n += zbp_varint_write(out + n, length);
  memcpy(out + n, table, length);
  n += length;
  out[n++] = 0x20;
  out[n++] = 2;
  *size = n;
  return out;
}

static int field(const unsigned char **p, const unsigned char *end,
                 uint64_t *tag, const unsigned char **bytes, size_t *length,
                 uint64_t *value) {
  uint64_t n;
  if (!zbp_varint_read(p, end, tag) || !(*tag >> 3))
    return 0;
  *bytes = NULL;
  *length = 0;
  switch (*tag & 7) {
  case 0:
    return zbp_varint_read(p, end, value);
  case 1:
    n = 8;
    break;
  case 2:
    if (!zbp_varint_read(p, end, &n))
      return 0;
    break;
  case 5:
    n = 4;
    break;
  default:
    return 0;
  }
  if (n > (uint64_t)(end - *p))
    return 0;
  *bytes = *p;
  *length = (size_t)n;
  *p += n;
  return 1;
}

int zbp_parse_response(const unsigned char *data, size_t size, uint64_t *ack) {
  const unsigned char *p = data, *end = data + size;
  int kind = 0;
  while (p != end) {
    uint64_t tag = 0, value = 0;
    const unsigned char *bytes;
    size_t length;
    if (!field(&p, end, &tag, &bytes, &length, &value))
      return -1;
    if ((tag >> 3) > 3)
      continue;
    if ((tag & 7) != 2)
      return -1;
    if (kind)
      return -1;
    kind = (int)(tag >> 3);
    const unsigned char *inner = bytes, *inner_end = bytes + length;
    int has_ack = 0;
    while (inner != inner_end) {
      if (!field(&inner, inner_end, &tag, &bytes, &length, &value))
        return -1;
      if (kind == 2 && (tag >> 3) == 1) {
        if ((tag & 7) != 0 || value > INT64_MAX)
          return -1;
        *ack = value;
        has_ack = 1;
      }
    }
    if (kind == 2 && !has_ack)
      return -1;
  }
  return kind;
}
