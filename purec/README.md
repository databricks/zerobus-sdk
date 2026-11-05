# Zerobus Pure C SDK

A C client for Zerobus that doesn't use the Rust core. It is under early development and not released yet: the API may change before 1.0.

## OAuth tokens for other clients

`zerobus_get_token` (in `zerobus/oauth.h`) fetches the Zerobus-scoped OAuth token that streams authenticate with. Use it to authenticate clients of Zerobus' other [API protocols](https://docs.databricks.com/aws/en/ingestion/zerobus-api-protocols), such as REST, OpenTelemetry, MQTT, or Kafka. It needs no SDK handle and caches nothing: each call fetches a new token, with its expiry in Unix epoch milliseconds (0 if the server reported none). It blocks for up to 15 seconds, except for the DNS lookup, which has no limit.

Its arguments are the workspace URL, the table name as `catalog.schema.table`, the service principal's client ID and secret, and the workspace ID. See [connection URL table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id) for where to find them.

For example, a [librdkafka](https://github.com/confluentinc/librdkafka) producer can authenticate to the [Zerobus Kafka-compatible endpoint](https://docs.databricks.com/aws/en/ingestion/zerobus-kafka) with SASL/OAUTHBEARER:

```c
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <zerobus/zerobus.h>

static void refresh_token(rd_kafka_t *rk, const char *config, void *opaque)
{
    zerobus_oauth_token_t token = {0};
    zerobus_error_t *error = NULL;
    zerobus_status_t status = zerobus_get_token(
        zerobus_string_view(getenv("DATABRICKS_WORKSPACE_URL")),
        zerobus_string_view(getenv("ZEROBUS_TABLE_NAME")),
        zerobus_string_view(getenv("DATABRICKS_CLIENT_ID")),
        zerobus_string_view(getenv("DATABRICKS_CLIENT_SECRET")),
        zerobus_string_view("<workspace-id>"), &token, &error);
    if (status != ZEROBUS_STATUS_OK) {
        /* librdkafka retries later; zerobus_error_message(error) says why. */
        rd_kafka_oauthbearer_set_token_failure(rk, "Zerobus token request failed");
        zerobus_error_free(error);
        return;
    }
    /* librdkafka rejects a missing (0) expiry, which reports a failure below. */
    char errstr[512];
    if (rd_kafka_oauthbearer_set_token(rk, token.access_token,
                                       (int64_t)token.expires_at_unix_ms,
                                       getenv("DATABRICKS_CLIENT_ID"), NULL, 0,
                                       errstr, sizeof(errstr)) !=
        RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_oauthbearer_set_token_failure(rk, errstr);
    }
    zerobus_oauth_token_free(&token);
}

/* Also set bootstrap.servers as the Zerobus Kafka docs describe, and produce
 * to the table name as the topic. */
static void configure(rd_kafka_conf_t *conf, char *errstr, size_t errstr_size)
{
    rd_kafka_conf_set(conf, "security.protocol", "SASL_SSL", errstr, errstr_size);
    rd_kafka_conf_set(conf, "sasl.mechanism", "OAUTHBEARER", errstr, errstr_size);
    rd_kafka_conf_set(conf, "acks", "all", errstr, errstr_size);
    rd_kafka_conf_set_oauthbearer_token_refresh_cb(conf, refresh_token);
}
```
