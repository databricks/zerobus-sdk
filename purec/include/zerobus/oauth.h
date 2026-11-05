/*
 * Zerobus Pure C SDK — OAuth tokens for other clients.
 *
 * Fetches the Zerobus-scoped OAuth token that streams authenticate with, for
 * any client that takes an OAuth token, such as a Kafka client using
 * SASL/OAUTHBEARER against the Zerobus Kafka-compatible endpoint. Needs no SDK
 * handle.
 */
#ifndef ZEROBUS_OAUTH_H
#define ZEROBUS_OAUTH_H

#include "zerobus/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A Zerobus-scoped OAuth access token. Release it with
 * zerobus_oauth_token_free. */
typedef struct zerobus_oauth_token {
    /* NUL-terminated, sent as "Bearer <access_token>". */
    char *access_token;
    /* Expiry in Unix epoch milliseconds, counted from just before the request
     * was sent, or 0 if the server reported no lifetime. */
    uint64_t expires_at_unix_ms;
} zerobus_oauth_token_t;

/*
 * Fetches a token that lets client_id write to table_name, as
 * catalog.schema.table, in the workspace with workspace_id, with the OAuth
 * client credentials grant. workspace_url is the workspace URL, a bare
 * http(s)://host[:port]. Every call fetches a new token: nothing is cached, so
 * call it when the client needs one, such as shortly before the last one
 * expires. May be called from any thread.
 *
 * Blocks until the token endpoint answers, within 15 seconds except for the DNS
 * lookup, which has no limit. An https workspace_url loads the system's
 * trusted roots on each call.
 *
 * On success fills *out_token, overwriting it without freeing a token it held.
 * Otherwise leaves *out_token untouched and returns INVALID_ARGUMENT for an
 * invalid argument, without sending a request; UNAVAILABLE for a 5xx response
 * or a failed connection, and DEADLINE_EXCEEDED on timeout, which may succeed
 * on retry; UNAUTHENTICATED for any other non-2xx response or a response
 * without a usable access token, which will not. zerobus_error_is_retryable
 * tells these and any other failure apart.
 */
ZEROBUS_API zerobus_status_t ZEROBUS_CALL zerobus_get_token(
    zerobus_string_view_t workspace_url, zerobus_string_view_t table_name,
    zerobus_string_view_t client_id, zerobus_string_view_t client_secret,
    zerobus_string_view_t workspace_id, zerobus_oauth_token_t *out_token,
    zerobus_error_t **out_error);

/* Zeroes and frees the access token, and leaves *token empty. Accepts NULL and
 * an empty token, so freeing twice is harmless. */
ZEROBUS_API void ZEROBUS_CALL
zerobus_oauth_token_free(zerobus_oauth_token_t *token);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_OAUTH_H */
