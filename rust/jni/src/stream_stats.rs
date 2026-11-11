//! Conversion of `StreamStats` snapshots into `com.databricks.zerobus.StreamStats`.

use std::time::{SystemTime, UNIX_EPOCH};

use databricks_zerobus_ingest_sdk::StreamStats;
use jni::objects::{JObject, JValue};
use jni::JNIEnv;

use crate::class_cache::{as_jclass, get_class_cache};

/// Builds a Java `StreamStats`. Returns null with a pending exception if construction fails.
pub fn stats_to_java<'local>(env: &mut JNIEnv<'local>, stats: &StreamStats) -> JObject<'local> {
    let message: JObject = match stats.last_error.as_ref() {
        Some(error) => match env.new_string(&error.message) {
            Ok(s) => s.into(),
            Err(_) => return JObject::null(),
        },
        None => JObject::null(),
    };
    let (retryable, timestamp_millis) = match stats.last_error.as_ref() {
        Some(error) => (error.is_retryable, epoch_millis(error.timestamp)),
        None => (false, 0),
    };
    let class = as_jclass(&get_class_cache().stream_stats_class);
    let args = [
        JValue::Long(stats.records_in_flight as i64),
        JValue::Long(stats.last_acked_offset.unwrap_or(-1)),
        JValue::Long(stats.reconnects as i64),
        JValue::Object(&message),
        JValue::Bool(u8::from(retryable)),
        JValue::Long(timestamp_millis),
    ];
    env.new_object(class, "(JJJLjava/lang/String;ZJ)V", &args)
        .unwrap_or_else(|_| JObject::null())
}

fn epoch_millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_millis_converts_and_clamps_pre_epoch_times() {
        let t = UNIX_EPOCH + std::time::Duration::from_millis(1_234_567);
        assert_eq!(epoch_millis(t), 1_234_567);
        assert_eq!(epoch_millis(UNIX_EPOCH), 0);
        assert_eq!(
            epoch_millis(UNIX_EPOCH - std::time::Duration::from_secs(1)),
            0
        );
    }
}
