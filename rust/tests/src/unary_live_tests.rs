//! Live tests of the unary `InsertRecords` client against a real Zerobus server.
//!
//! Like the Java, TypeScript and C++ integration tests, they run only when these
//! environment variables are set, and otherwise pass without doing anything:
//!
//! * `ZEROBUS_SERVER_ENDPOINT` - the Zerobus endpoint URL
//! * `DATABRICKS_WORKSPACE_URL` - the workspace URL
//! * `ZEROBUS_TABLE_NAME` - the target table, `catalog.schema.table`, with the
//!   columns `device_name STRING, temp INT, humidity BIGINT`
//! * `DATABRICKS_CLIENT_ID` - a service principal with MODIFY and SELECT on the table
//! * `DATABRICKS_CLIENT_SECRET` - its secret
//!
//! Run with `cargo test -p tests --test unary_live_tests -- --nocapture`.

use databricks_zerobus_ingest_sdk::{
    GrpcCode, JsonString, JsonValue, UnaryClient, UnaryClientBuilder, ZerobusError, ZerobusSdk,
};
use serde::Serialize;

struct Live {
    endpoint: String,
    workspace_url: String,
    table_name: String,
    client_id: String,
    client_secret: String,
}

impl Live {
    /// The connection settings, or `None` when any of them is unset.
    fn from_env() -> Option<Self> {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let live = Self {
            endpoint: var("ZEROBUS_SERVER_ENDPOINT")?,
            workspace_url: var("DATABRICKS_WORKSPACE_URL")?,
            table_name: var("ZEROBUS_TABLE_NAME")?,
            client_id: var("DATABRICKS_CLIENT_ID")?,
            client_secret: var("DATABRICKS_CLIENT_SECRET")?,
        };
        Some(live)
    }

    fn sdk(&self) -> ZerobusSdk {
        ZerobusSdk::builder()
            .endpoint(&self.endpoint)
            .unity_catalog_url(&self.workspace_url)
            .build()
            .expect("build SDK")
    }

    async fn client(
        &self,
        sdk: &ZerobusSdk,
        configure: impl FnOnce(UnaryClientBuilder<'_>) -> UnaryClientBuilder<'_>,
    ) -> UnaryClient {
        let builder = sdk
            .unary_client_builder()
            .table(&self.table_name)
            .oauth(&self.client_id, &self.client_secret);
        configure(builder)
            .build()
            .await
            .expect("build unary client")
    }
}

/// Returns the settings, or skips the calling test when they are missing.
macro_rules! live_or_skip {
    () => {
        match Live::from_env() {
            Some(live) => live,
            None => {
                eprintln!(
                    "skipping: set ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, \
                     ZEROBUS_TABLE_NAME, DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET"
                );
                return;
            }
        }
    };
}

#[derive(Serialize)]
struct Reading {
    device_name: String,
    temp: i32,
    humidity: i64,
}

fn readings(device: &str, count: i32) -> Vec<JsonValue<Reading>> {
    (0..count)
        .map(|i| {
            JsonValue(Reading {
                device_name: format!("rust-unary-{device}-{i}"),
                temp: 20 + i,
                humidity: 50 + i64::from(i),
            })
        })
        .collect()
}

#[tokio::test]
async fn json_array_batches_are_ingested() {
    let live = live_or_skip!();
    let sdk = live.sdk();
    let client = live.client(&sdk, |b| b.json()).await;

    client
        .ingest_records(readings("array-single", 1))
        .await
        .unwrap();
    client
        .ingest_records(readings("array-batch", 10))
        .await
        .unwrap();
}

#[tokio::test]
async fn json_lines_batches_are_ingested() {
    let live = live_or_skip!();
    let sdk = live.sdk();
    let client = live.client(&sdk, |b| b.json_lines()).await;

    client
        .ingest_records(readings("lines-batch", 10))
        .await
        .unwrap();
    // A pretty-printed record: the client turns its line breaks into spaces.
    let pretty = JsonString(
        "{\n  \"device_name\": \"rust-unary-lines-pretty\",\n  \"temp\": 1,\n  \"humidity\": 2\n}"
            .to_string(),
    );
    client.ingest_records(vec![pretty]).await.unwrap();
}

#[tokio::test]
async fn ingest_record_sends_one_record() {
    let live = live_or_skip!();
    let sdk = live.sdk();
    let client = live.client(&sdk, |b| b.json()).await;

    let record = readings("record", 1).pop().unwrap();
    client.ingest_record(record).await.unwrap();
}

#[tokio::test]
async fn record_that_does_not_match_the_table_is_rejected_without_retry() {
    let live = live_or_skip!();
    let sdk = live.sdk();
    let client = live.client(&sdk, |b| b.json()).await;
    let bad = JsonString(
        r#"{"device_name": "rust-unary-bad", "temp": "not a number", "humidity": 1}"#.to_string(),
    );

    let error = client.ingest_records(vec![bad]).await.unwrap_err();

    assert!(!error.is_retryable(), "{error:?}");
    match error {
        ZerobusError::UnaryIngestError(status) => {
            assert_eq!(status.code(), GrpcCode::InvalidArgument, "{status:?}")
        }
        other => panic!("expected UnaryIngestError, got {other:?}"),
    }
}

#[tokio::test]
async fn wrong_client_secret_is_rejected_without_retry() {
    let live = live_or_skip!();
    let live = Live {
        client_secret: "not-the-secret".to_string(),
        ..live
    };
    let sdk = live.sdk();
    let client = live.client(&sdk, |b| b.json()).await;

    let error = client
        .ingest_records(readings("wrong-secret", 1))
        .await
        .unwrap_err();

    // The token endpoint answers 401 before anything reaches Zerobus.
    assert!(
        matches!(error, ZerobusError::InvalidUCTokenError(_)),
        "{error:?}"
    );
    assert!(!error.is_retryable());
}
