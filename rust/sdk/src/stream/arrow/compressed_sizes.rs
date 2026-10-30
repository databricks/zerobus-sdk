//! Batch wire sizes after gRPC compression.
//!
//! tonic compresses each request message only after the request stream yields it, so
//! the stream can't see the compressed size. [`CompressedSizeChannel`] wraps the request
//! body tonic writes and reads each message's gRPC length prefix. [`CompressedSizes`]
//! pairs those lengths, in order, with the frames the request stream yielded, since
//! tonic encodes exactly one message per yielded frame.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tonic::codegen::{http, Service};
use tonic::transport::Channel;
use tonic::Status;

use crate::offset_generator::OffsetId;
use crate::stats::{BatchStats, StatsExporter, StreamStat};

/// gRPC length-prefixed message header: a compressed flag and a big-endian `u32` length.
const GRPC_HEADER_LEN: usize = 5;

/// A frame the request stream yielded, waiting for its message's compressed length.
pub(super) enum SentFrame {
    /// The schema frame, or a frame of a batch whose stats are not reported.
    Untracked,
    /// A data frame of a reported batch.
    Batch {
        offset: OffsetId,
        attempt: u32,
        /// Set on the batch's final frame. `approximate_wire_bytes` is filled in once that
        /// frame's message is measured.
        completed: Option<BatchStats>,
    },
}

/// Shared by one DoPut request stream, which records frames as it yields them, and the
/// request body that carries them, which measures their messages.
pub(super) struct CompressedSizes {
    exporter: Arc<dyn StatsExporter>,
    state: StdMutex<CompressedSizesState>,
}

#[derive(Default)]
struct CompressedSizesState {
    /// Yielded frames whose messages haven't been measured yet, in yield order.
    frames: VecDeque<SentFrame>,
    /// The `(offset, attempt)` whose messages are being summed, and their bytes so far.
    batch: Option<(OffsetId, u32)>,
    bytes: u64,
}

impl CompressedSizes {
    pub(super) fn new(exporter: Arc<dyn StatsExporter>) -> Self {
        Self {
            exporter,
            state: StdMutex::new(CompressedSizesState::default()),
        }
    }

    /// Records a frame as it is yielded to tonic, before its message is encoded.
    pub(super) fn push_frame(&self, frame: SentFrame) {
        self.lock().frames.push_back(frame);
    }

    /// Accounts the compressed payload length of the next message, emitting `BatchSent`
    /// when it is the final message of a batch.
    fn record_message(&self, len: u64) {
        let (offset, attempt, stats) = {
            let mut state = self.lock();
            let Some(SentFrame::Batch {
                offset,
                attempt,
                completed,
            }) = state.frames.pop_front()
            else {
                return;
            };
            // A batch whose stats were abandoned mid-transmission leaves partial bytes;
            // the next batch starts from zero.
            if state.batch != Some((offset, attempt)) {
                state.batch = Some((offset, attempt));
                state.bytes = 0;
            }
            state.bytes += len;
            let Some(mut stats) = completed else {
                return;
            };
            stats.approximate_wire_bytes = state.bytes;
            state.batch = None;
            (offset, attempt, stats)
        };
        crate::stats::dispatch(
            self.exporter.as_ref(),
            StreamStat::BatchSent {
                offset,
                attempt,
                stats,
            },
        );
    }

    /// Locks the state, recovering the guard if a previous holder panicked. Telemetry is
    /// best-effort, so it must never panic the request body.
    fn lock(&self) -> std::sync::MutexGuard<'_, CompressedSizesState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Reads gRPC message lengths from a request body split into arbitrary chunks.
#[derive(Default)]
struct MessageLengthReader {
    header: [u8; GRPC_HEADER_LEN],
    header_read: usize,
    /// Payload bytes of the current message not yet seen.
    payload_remaining: usize,
}

impl MessageLengthReader {
    /// Calls `on_message` with the payload length of each message completed by `chunk`.
    fn read(&mut self, mut chunk: &[u8], mut on_message: impl FnMut(u64)) {
        while !chunk.is_empty() {
            if self.payload_remaining > 0 {
                let n = self.payload_remaining.min(chunk.len());
                self.payload_remaining -= n;
                chunk = &chunk[n..];
                if self.payload_remaining == 0 {
                    on_message(self.message_len() as u64);
                }
                continue;
            }
            let n = (GRPC_HEADER_LEN - self.header_read).min(chunk.len());
            self.header[self.header_read..self.header_read + n].copy_from_slice(&chunk[..n]);
            self.header_read += n;
            chunk = &chunk[n..];
            if self.header_read == GRPC_HEADER_LEN {
                self.header_read = 0;
                self.payload_remaining = self.message_len();
                if self.payload_remaining == 0 {
                    on_message(0);
                }
            }
        }
    }

    fn message_len(&self) -> usize {
        u32::from_be_bytes([
            self.header[1],
            self.header[2],
            self.header[3],
            self.header[4],
        ]) as usize
    }
}

/// The Flight transport. When batch stats are reported with gRPC compression, it measures
/// each DoPut request message as tonic writes it.
#[derive(Clone)]
pub(super) struct CompressedSizeChannel {
    inner: Channel,
    sizes: Option<Arc<CompressedSizes>>,
}

impl CompressedSizeChannel {
    pub(super) fn new(inner: Channel, sizes: Option<Arc<CompressedSizes>>) -> Self {
        Self { inner, sizes }
    }
}

impl Service<http::Request<tonic::body::Body>> for CompressedSizeChannel {
    type Response = <Channel as Service<http::Request<tonic::body::Body>>>::Response;
    type Error = <Channel as Service<http::Request<tonic::body::Body>>>::Error;
    type Future = <Channel as Service<http::Request<tonic::body::Body>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let request = match &self.sizes {
            Some(sizes) => request.map(|inner| {
                tonic::body::Body::new(MessageSizeBody {
                    inner,
                    reader: MessageLengthReader::default(),
                    sizes: Arc::clone(sizes),
                })
            }),
            None => request,
        };
        self.inner.call(request)
    }
}

/// A request body that passes data through unchanged while measuring its messages.
struct MessageSizeBody {
    inner: tonic::body::Body,
    reader: MessageLengthReader,
    sizes: Arc<CompressedSizes>,
}

impl Body for MessageSizeBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &frame {
            if let Some(data) = frame.data_ref() {
                let sizes = &this.sizes;
                this.reader.read(data, |len| sizes.record_message(len));
            }
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    use crate::stats::channel_exporter;

    fn message(len: usize) -> Vec<u8> {
        let mut m = vec![1];
        m.extend_from_slice(&(len as u32).to_be_bytes());
        m.extend(std::iter::repeat_n(0xAB, len));
        m
    }

    fn lengths(chunks: &[&[u8]]) -> Vec<u64> {
        let mut reader = MessageLengthReader::default();
        let mut out = Vec::new();
        for chunk in chunks {
            reader.read(chunk, |len| out.push(len));
        }
        out
    }

    #[test]
    fn reads_lengths_across_any_chunking() {
        let body: Vec<u8> = [message(3), message(0), message(300)].concat();
        assert_eq!(lengths(&[&body]), [3, 0, 300]);
        let bytes: Vec<&[u8]> = body.chunks(1).collect();
        assert_eq!(lengths(&bytes), [3, 0, 300]);
        let (a, b) = body.split_at(GRPC_HEADER_LEN + 1);
        assert_eq!(lengths(&[a, b]), [3, 0, 300]);
    }

    fn sent(stats: &mut tokio::sync::mpsc::Receiver<StreamStat>) -> Vec<(OffsetId, u32, u64)> {
        let mut out = Vec::new();
        while let Ok(StreamStat::BatchSent {
            offset,
            attempt,
            stats,
        }) = stats.try_recv()
        {
            out.push((offset, attempt, stats.approximate_wire_bytes));
        }
        out
    }

    #[tokio::test]
    async fn sums_each_batchs_messages_and_skips_untracked_frames() {
        let (exporter, mut rx) = channel_exporter(NonZeroUsize::new(16).unwrap());
        let sizes = CompressedSizes::new(exporter);
        let frame = |offset, completed: bool| SentFrame::Batch {
            offset,
            attempt: 0,
            completed: completed.then(|| BatchStats::new(10, 0, 999)),
        };
        sizes.push_frame(SentFrame::Untracked);
        sizes.push_frame(frame(0, false));
        sizes.push_frame(frame(0, true));
        // Batch 1 is abandoned before its final frame; batch 2 must not include its bytes.
        sizes.push_frame(frame(1, false));
        sizes.push_frame(SentFrame::Untracked);
        sizes.push_frame(frame(2, true));
        for len in [100, 7, 8, 50, 60, 9] {
            sizes.record_message(len);
        }
        assert_eq!(sent(&mut rx), [(0, 0, 15), (2, 0, 9)]);
    }
}
