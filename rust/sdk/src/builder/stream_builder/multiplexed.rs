//! Multiplexed gRPC stream construction.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use rand::Rng;
use tracing::{error, warn};

#[cfg(feature = "arrow-flight")]
use super::FormatConfig;
use super::StreamBuilder;
use crate::headers_provider::HeadersProvider;
use crate::{MultiplexedStream, TableProperties, ZerobusError, ZerobusResult, ZerobusStream};

pub(super) const MAX_MULTIPLEXED_JITTER_MS: u64 = 1_000;

/// Terminal builder for a [`MultiplexedStream`].
///
/// Obtain this from [`StreamBuilder::multiplexed`] after setting all ordinary
/// stream options. Construction opens every sub-stream concurrently after an
/// independently sampled delay of at most one second when more than one stream
/// is requested. A single stream opens immediately. Construction is atomic: if
/// any open fails, every successfully opened sub-stream is closed and the error
/// from the lowest failing stream index is returned.
///
/// Dropping the build future cancels delayed and active opens and tears down
/// any sub-streams that already opened.
///
/// # Beta
///
/// Multiplexed streams are a Beta API.
#[must_use = "a MultiplexedStreamBuilder does nothing until `.build()` is called"]
pub struct MultiplexedStreamBuilder<'a> {
    inner: StreamBuilder<'a>,
    stream_count: usize,
}

impl fmt::Debug for MultiplexedStreamBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiplexedStreamBuilder")
            .field("stream_count", &self.stream_count)
            .field("stream", &self.inner)
            .finish()
    }
}

#[allow(clippy::result_large_err)]
impl<'a> MultiplexedStreamBuilder<'a> {
    pub(super) fn new(inner: StreamBuilder<'a>, stream_count: usize) -> Self {
        Self {
            inner,
            stream_count,
        }
    }

    /// Validate the common stream configuration, mux-compatible format,
    /// callback selection, and stream count without opening a connection.
    ///
    /// This is a configuration check, not a guarantee that [`build`](Self::build)
    /// succeeds: the Avro writer schema is parsed in `build()` (before any
    /// connection), so a malformed schema surfaces there, not here.
    pub fn validate(&self) -> ZerobusResult<()> {
        self.inner.validate_common()?;
        let max_streams = crate::multiplexed_stream::MAX_STREAMS;
        if !(1..=max_streams).contains(&self.stream_count) {
            return Err(ZerobusError::InvalidArgument(format!(
                "multiplexed stream_count must be between 1 and {max_streams}"
            )));
        }

        if self.max_inflight_requests_per_stream().is_none() {
            return Err(ZerobusError::InvalidArgument(format!(
                "max_inflight_requests ({}) must be at least stream_count ({}) for multiplexed streams",
                self.inner.grpc_config.max_inflight_requests, self.stream_count
            )));
        }

        #[cfg(feature = "arrow-flight")]
        if matches!(self.inner.format, Some(FormatConfig::Arrow(_))) {
            return Err(ZerobusError::InvalidArgument(
                "Arrow format is not supported for multiplexed streams; use .build_arrow() on an ordinary StreamBuilder"
                    .into(),
            ));
        }

        if self.inner.grpc_config.ack_callback.is_some() {
            return Err(ZerobusError::InvalidArgument(
                "ack_callback is only valid for ordinary streams; use multiplexed_ack_callback before .multiplexed(...)"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(super) fn max_inflight_requests_per_stream(&self) -> Option<usize> {
        self.inner
            .grpc_config
            .max_inflight_requests
            .checked_div(self.stream_count)
            .filter(|&capacity| capacity > 0)
    }

    pub(super) fn resolve_headers_providers(&self) -> ZerobusResult<Vec<Arc<dyn HeadersProvider>>> {
        (0..self.stream_count)
            .map(|_| self.inner.resolve_headers_provider())
            .collect()
    }

    pub(super) fn sample_jitter_delays(&self) -> Vec<Duration> {
        if self.stream_count == 1 {
            return vec![Duration::ZERO];
        }

        let mut rng = rand::rng();
        (0..self.stream_count)
            .map(|_| Duration::from_millis(rng.random_range(0..=MAX_MULTIPLEXED_JITTER_MS)))
            .collect()
    }

    /// Build all sub-streams concurrently and return the complete mux.
    ///
    /// No partially constructed mux is returned. If several opens fail, the
    /// error belonging to the lowest preassigned sub-stream index wins; all
    /// other creation and cleanup errors are logged.
    pub async fn build(mut self) -> ZerobusResult<MultiplexedStream> {
        self.validate()?;

        // OAuth providers keep per-stream rejection-generation state, while
        // sharing the SDK's token cache. Custom providers preserve their Arc
        // identity because cloning their state may not be meaningful.
        let headers_providers = self.resolve_headers_providers()?;
        let delays = self.sample_jitter_delays();
        let multiplexed_callback = self.inner.multiplexed_callback.take();

        // Parse Avro once before opening any lane. TableProperties clones share the same
        // immutable writer schema allocation across lanes, supervisors, and retries.
        let format = self
            .inner
            .format
            .take()
            .expect("format was validated")
            .into_grpc()?;
        self.inner.grpc_config.record_type = format.record_type;

        let table_properties = TableProperties {
            table_name: self.inner.table_name.clone(),
            descriptor_proto: format.descriptor_proto,
            message_descriptor: format.message_descriptor,
            #[cfg(feature = "avro")]
            avro_schema: format.avro_schema,
        };
        let sdk = self.inner.sdk;
        let max_inflight_requests_per_stream = self
            .max_inflight_requests_per_stream()
            .expect("in-flight capacity was validated");
        let mut options = self.inner.grpc_config;
        options.max_inflight_requests = max_inflight_requests_per_stream;

        let opens = delays.into_iter().zip(headers_providers).enumerate().map(
            |(stream_index, (delay, headers_provider))| {
                let table_properties = table_properties.clone();
                let mut options = options.clone();
                let multiplexed_callback = multiplexed_callback.clone();

                async move {
                    tokio::time::sleep(delay).await;
                    let channel = sdk.get_or_create_channel_zerobus_client().await?;
                    options.ack_callback = multiplexed_callback.map(|callback| {
                        crate::multiplexed_stream::multiplexed_ack_callback(stream_index, callback)
                    });
                    ZerobusStream::new_stream(channel, table_properties, headers_provider, options)
                        .await
                }
            },
        );

        let streams = collect_opened(opens, |mut stream: ZerobusStream| async move {
            stream.close().await
        })
        .await?;
        debug_assert_eq!(streams.len(), self.stream_count);
        // This is one user-initiated logical stream creation. Counting each
        // internal lane would make the churn warning flag intentional muxes.
        crate::client_warnings::record_stream_creation(&table_properties.table_name);
        Ok(MultiplexedStream::from_streams(streams))
    }
}

/// Preserves preassigned indices, waits for every open, and closes all successes
/// before returning the lowest-index construction error. Cancelling this future
/// drops pending opens and successfully constructed lanes together.
async fn collect_opened<S, O, C, F>(
    opens: impl IntoIterator<Item = O>,
    close: C,
) -> ZerobusResult<Vec<S>>
where
    O: std::future::Future<Output = ZerobusResult<S>>,
    C: Fn(S) -> F,
    F: std::future::Future<Output = ZerobusResult<()>>,
{
    // `join_all` polls every open concurrently and preserves input order,
    // so completion timing cannot change the assigned stream indices.
    let results = join_all(opens).await;
    let mut indexed_streams = Vec::new();
    let mut first_error = None;
    for (stream_index, result) in results.into_iter().enumerate() {
        match result {
            Ok(stream) => indexed_streams.push((stream_index, stream)),
            Err(err) => {
                error!(stream_index, error = %err, "Failed to create multiplexed sub-stream");
                first_error.get_or_insert(err);
            }
        }
    }

    if let Some(first_error) = first_error {
        let cleanup_results = join_all(indexed_streams.into_iter().map(
            |(successful_stream_index, stream)| {
                let close = &close;
                async move { (successful_stream_index, close(stream).await) }
            },
        ))
        .await;
        for (successful_stream_index, result) in cleanup_results {
            if let Err(err) = result {
                warn!(
                    stream_index = successful_stream_index,
                    error = %err,
                    "Failed to clean up multiplexed sub-stream after construction failure"
                );
            }
        }
        return Err(first_error);
    }

    let streams: Vec<_> = indexed_streams
        .into_iter()
        .map(|(_stream_index, stream)| stream)
        .collect();
    Ok(streams)
}

#[cfg(test)]
mod construction_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Debug)]
    struct Opened(usize, Arc<AtomicUsize>);
    impl Drop for Opened {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn completion_order_does_not_change_lane_indices() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let opens = (0..3).map(|i| {
            let dropped = dropped.clone();
            async move {
                tokio::time::sleep(Duration::from_millis((3 - i) as u64 * 100)).await;
                Ok(Opened(i, dropped))
            }
        });
        let lanes = collect_opened(opens, |_| async { Ok(()) }).await.unwrap();
        assert_eq!(lanes.iter().map(|s| s.0).collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(lanes);
        assert_eq!(dropped.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn lowest_index_failure_wins_after_all_opens_and_cleanup_settle() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let cleaned = Arc::new(AtomicUsize::new(0));
        let opens = (0..4).map(|i| {
            let dropped = dropped.clone();
            async move {
                tokio::time::sleep(Duration::from_millis((4 - i) as u64 * 100)).await;
                if i % 2 == 0 {
                    Ok(Opened(i, dropped))
                } else {
                    Err(ZerobusError::InvalidArgument(format!("lane-{i}")))
                }
            }
        });
        let result = collect_opened(opens, |lane| {
            let cleaned = cleaned.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                cleaned.fetch_add(1, Ordering::SeqCst);
                drop(lane);
                Err(ZerobusError::InvalidArgument("cleanup error".into()))
            }
        })
        .await;
        assert!(matches!(result, Err(ZerobusError::InvalidArgument(msg)) if msg == "lane-1"));
        assert_eq!(cleaned.load(Ordering::SeqCst), 2);
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancellation_drops_completed_and_in_progress_opens() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let reached = Arc::new(Notify::new());
        let task = {
            let dropped = dropped.clone();
            let reached = reached.clone();
            tokio::spawn(async move {
                let opens = (0..3).map(|i| {
                    let dropped = dropped.clone();
                    let reached = reached.clone();
                    async move {
                        let lane = Opened(i, dropped);
                        if i == 1 {
                            reached.notify_one();
                            std::future::pending::<()>().await;
                        }
                        if i == 2 {
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                        }
                        Ok(lane)
                    }
                });
                collect_opened(opens, |_| async { Ok(()) }).await
            })
        };
        reached.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(dropped.load(Ordering::SeqCst), 3);
    }
}
