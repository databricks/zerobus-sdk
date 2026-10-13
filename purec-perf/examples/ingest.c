/* Example: queue copied records, then wait once for the cumulative ack.
 * Usage: zbp_ingest_example HOST PORT TABLE 'JSON_RECORD' [COUNT]
 * Supply a fresh bearer token in ZB_TOKEN. The JSON must match the table
 * schema.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zbperf.h>

static unsigned long parse_count(const char *value) {
  char *end;
  errno = 0;
  unsigned long n = strtoul(value, &end, 10);
  return !errno && *value && *value != '-' && !*end ? n : 0;
}

int main(int argc, char **argv) {
  if (argc < 5 || argc > 6) {
    fprintf(stderr, "usage: %s HOST PORT TABLE 'JSON_RECORD' [COUNT]\n",
            argv[0]);
    return 2;
  }
  unsigned long port = parse_count(argv[2]);
  unsigned long count = argc == 6 ? parse_count(argv[5]) : 1000;
  const char *token = getenv("ZB_TOKEN");
  if (!port || port > 65535 || !count || !token || !*token) {
    fputs("A valid port, positive count, and ZB_TOKEN are required.\n", stderr);
    return 2;
  }
  zbp_runtime_config runtime_config;
  zbp_runtime_config_init(&runtime_config);
  zbp_connection_config connection_config;
  zbp_connection_config_init(&connection_config);
  connection_config.host = argv[1];
  connection_config.port = (uint16_t)port;
  connection_config.token = token;
  zbp_stream_config stream_config;
  zbp_stream_config_init(&stream_config);
  stream_config.batching = ZBP_BATCH_AUTO;

  zbp_runtime *runtime = NULL;
  zbp_connection *connection = NULL;
  zbp_stream *stream = NULL;
  zbp_status status = zbp_runtime_create(&runtime_config, &runtime);
  if (status == ZBP_OK)
    status = zbp_connection_open(runtime, &connection_config, &connection);
  if (status == ZBP_OK)
    status = zbp_stream_open(connection, argv[3], &stream_config, &stream);
  size_t bytes = strlen(argv[4]);
  for (unsigned long i = 0; i < count && status == ZBP_OK; ++i)
    status = zbp_ingest(stream, argv[4], bytes);
  if (status == ZBP_OK)
    status = zbp_flush(stream, 60000);
  if (status == ZBP_OK)
    status = zbp_stream_close(stream, 60000);
  if (status != ZBP_OK)
    fprintf(stderr, "ingestion failed: %s\n", zbp_status_string(status));
  zbp_runtime_destroy(runtime);
  return status == ZBP_OK ? 0 : 1;
}
