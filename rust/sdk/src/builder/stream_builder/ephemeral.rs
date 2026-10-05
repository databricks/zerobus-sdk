//! Ephemeral gRPC stream construction.

use super::{GrpcStreamParts, StreamBuilder};
use crate::{ZerobusResult, ZerobusStream};

impl StreamBuilder<'_> {
    /// Build and open the configured stream (any record format except Arrow Flight).
    ///
    /// Returns an error if table name, authentication, or format has not been set,
    /// or if an Arrow format was selected (use `build_arrow()` instead).
    pub async fn build(self) -> ZerobusResult<ZerobusStream> {
        let GrpcStreamParts {
            channel,
            table_properties,
            headers_provider,
            config,
        } = self.prepare_grpc().await?;
        let stream =
            ZerobusStream::new_stream(channel, table_properties, headers_provider, config).await?;
        crate::client_warnings::record_stream_creation(stream.table_properties.table_name.as_str());
        Ok(stream)
    }
}
