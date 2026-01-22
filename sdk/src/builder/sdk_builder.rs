//! Builder for creating [`ZerobusSdk`] instances.

use crate::{ZerobusError, ZerobusResult, ZerobusSdk};

/// Builder for creating a [`ZerobusSdk`] instance with fluent configuration.
///
/// # Examples
///
/// ```no_run
/// use databricks_zerobus_ingest_sdk::ZerobusSdkBuilder;
///
/// let sdk = ZerobusSdkBuilder::new()
///     .endpoint("https://workspace.zerobus.databricks.com")
///     .unity_catalog_url("https://workspace.cloud.databricks.com")
///     .build()?;
/// # Ok::<(), databricks_zerobus_ingest_sdk::ZerobusError>(())
/// ```
#[derive(Debug, Default, Clone)]
pub struct ZerobusSdkBuilder {
    zerobus_endpoint: Option<String>,
    unity_catalog_url: Option<String>,
    use_tls: bool,
}

impl ZerobusSdkBuilder {
    /// Creates a new SDK builder with default settings.
    ///
    /// TLS is enabled by default.
    pub fn new() -> Self {
        Self {
            zerobus_endpoint: None,
            unity_catalog_url: None,
            use_tls: true,
        }
    }

    /// Sets the Zerobus API endpoint URL.
    ///
    /// This is required. The workspace ID is automatically extracted from this URL.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The Zerobus endpoint URL (e.g., "https://workspace-id.zerobus.region.cloud.databricks.com")
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.zerobus_endpoint = Some(endpoint.into());
        self
    }

    /// Sets the Unity Catalog endpoint URL.
    ///
    /// This is required for OAuth authentication.
    ///
    /// # Arguments
    ///
    /// * `url` - The Unity Catalog URL (e.g., "https://workspace.cloud.databricks.com")
    pub fn unity_catalog_url(mut self, url: impl Into<String>) -> Self {
        self.unity_catalog_url = Some(url.into());
        self
    }

    /// Disables TLS for the connection.
    ///
    /// **Warning**: This should only be used for local testing. Production
    /// environments should always use TLS.
    pub fn disable_tls(mut self) -> Self {
        self.use_tls = false;
        self
    }

    /// Builds the [`ZerobusSdk`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The endpoint is not set
    /// - The Unity Catalog URL is not set
    /// - The workspace ID cannot be extracted from the endpoint
    #[allow(clippy::result_large_err)]
    pub fn build(self) -> ZerobusResult<ZerobusSdk> {
        let zerobus_endpoint = self.zerobus_endpoint.ok_or_else(|| {
            ZerobusError::InvalidArgument("endpoint is required".to_string())
        })?;

        let unity_catalog_url = self.unity_catalog_url.ok_or_else(|| {
            ZerobusError::InvalidArgument("unity_catalog_url is required".to_string())
        })?;

        let workspace_id = zerobus_endpoint
            .strip_prefix("https://")
            .or_else(|| zerobus_endpoint.strip_prefix("http://"))
            .and_then(|s| s.split('.').next())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                ZerobusError::ChannelCreationError(
                    "Failed to extract workspace_id from zerobus_endpoint".to_string(),
                )
            })?;

        Ok(ZerobusSdk::new_with_config(
            zerobus_endpoint,
            unity_catalog_url,
            self.use_tls,
            workspace_id,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_with_all_fields() {
        let sdk = ZerobusSdkBuilder::new()
            .endpoint("https://my-workspace.zerobus.us-east-1.cloud.databricks.com")
            .unity_catalog_url("https://my-workspace.cloud.databricks.com")
            .build()
            .expect("should build successfully");

        assert_eq!(
            sdk.zerobus_endpoint,
            "https://my-workspace.zerobus.us-east-1.cloud.databricks.com"
        );
        assert_eq!(
            sdk.unity_catalog_url,
            "https://my-workspace.cloud.databricks.com"
        );
        assert!(sdk.use_tls);
    }

    #[test]
    fn test_builder_disable_tls() {
        let sdk = ZerobusSdkBuilder::new()
            .endpoint("http://localhost:8080")
            .unity_catalog_url("http://localhost:8081")
            .disable_tls()
            .build()
            .expect("should build successfully");

        assert!(!sdk.use_tls);
    }

    #[test]
    fn test_builder_missing_endpoint() {
        let result = ZerobusSdkBuilder::new()
            .unity_catalog_url("https://workspace.cloud.databricks.com")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_missing_unity_catalog_url() {
        let result = ZerobusSdkBuilder::new()
            .endpoint("https://workspace.zerobus.databricks.com")
            .build();

        assert!(result.is_err());
    }
}
