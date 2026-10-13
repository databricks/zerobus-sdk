#ifndef ZBP_PROTO_H
#define ZBP_PROTO_H
#include <stddef.h>
#include <stdint.h>
size_t zbp_varint_size(uint64_t value);
size_t zbp_varint_write(unsigned char *out, uint64_t value);
int zbp_varint_read(const unsigned char **data, const unsigned char *end,
                    uint64_t *out);
size_t zbp_record_prefix(unsigned char *out, size_t length);
size_t zbp_wire_header(unsigned char *out, uint64_t offset, size_t body_size,
                       int batch);
unsigned char *zbp_create_message(const char *table, size_t *size);
/* Returns 1=create, 2=ack, 3=close signal, 0=unknown, -1=malformed. */
int zbp_parse_response(const unsigned char *data, size_t size, uint64_t *ack);
#endif
