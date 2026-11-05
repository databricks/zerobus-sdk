//! JNI binding for the standalone OAuth token utility.

use crate::errors::{throw_from_zerobus_error, throw_non_retriable_exception};
use crate::runtime::block_on;
use databricks_zerobus_ingest_sdk::DefaultTokenFactory;
use jni::objects::{JClass, JLongArray, JString};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use std::ptr;
use std::time::UNIX_EPOCH;

/// Fetch a Zerobus-scoped OAuth token. Returns the access token and writes its
/// expiry (Unix epoch milliseconds, or 0 if unknown) into `expiresAtOut[0]`.
///
/// # JNI Signature
/// ```java
/// private static native String nativeGetToken(
///     String workspaceUrl,
///     String tableName,
///     String clientId,
///     String clientSecret,
///     String workspaceId,
///     long[] expiresAtOut
/// ) throws ZerobusException;
/// ```
#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_ZerobusSdk_nativeGetToken<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    workspace_url: JString<'local>,
    table_name: JString<'local>,
    client_id: JString<'local>,
    client_secret: JString<'local>,
    workspace_id: JString<'local>,
    expires_at_out: JLongArray<'local>,
) -> jstring {
    let mut arg = |value: &JString<'local>, name: &str| -> Option<String> {
        match env.get_string(value) {
            Ok(s) => Some(s.into()),
            Err(e) => {
                throw_non_retriable_exception(&mut env, &format!("Invalid {name}: {e}"));
                None
            }
        }
    };
    let Some(workspace_url) = arg(&workspace_url, "workspaceUrl") else {
        return ptr::null_mut();
    };
    let Some(table_name) = arg(&table_name, "tableName") else {
        return ptr::null_mut();
    };
    let Some(client_id) = arg(&client_id, "clientId") else {
        return ptr::null_mut();
    };
    let Some(client_secret) = arg(&client_secret, "clientSecret") else {
        return ptr::null_mut();
    };
    let Some(workspace_id) = arg(&workspace_id, "workspaceId") else {
        return ptr::null_mut();
    };

    let token = match block_on(DefaultTokenFactory::get_token_with_expiry(
        &workspace_url,
        &table_name,
        &client_id,
        &client_secret,
        &workspace_id,
    )) {
        Ok(token) => token,
        Err(e) => {
            throw_from_zerobus_error(&mut env, &e);
            return ptr::null_mut();
        }
    };

    let expires_at_ms: jlong = token
        .expires_at
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| jlong::try_from(d.as_millis()).unwrap_or(jlong::MAX))
        .unwrap_or(0);
    if let Err(e) = env.set_long_array_region(&expires_at_out, 0, &[expires_at_ms]) {
        throw_non_retriable_exception(&mut env, &format!("Failed to write token expiry: {e}"));
        return ptr::null_mut();
    }

    match env.new_string(token.access_token) {
        Ok(s) => s.into_raw(),
        Err(e) => {
            throw_non_retriable_exception(&mut env, &format!("Failed to return token: {e}"));
            ptr::null_mut()
        }
    }
}
