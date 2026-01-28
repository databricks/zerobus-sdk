#[cfg(test)]
mod tests {
    use crate::{
        intern_header_key, validate_sdk_ptr, validate_stream_ptr, write_error_result,
        write_success_result, zerobus_free_error_message, zerobus_get_default_config, CHeaders,
        CResult, CStreamConfigurationOptions, CallbackHeadersProvider, RecordType,
        StreamConfigurationOptions, ZerobusError,
    };
    use databricks_zerobus_ingest_sdk::HeadersProvider;
    use std::ffi::{CStr, CString};
    use std::ptr;

    // Helper for c_str_to_string since it's private
    unsafe fn test_c_str_to_string(
        c_str: *const std::os::raw::c_char,
    ) -> Result<String, &'static str> {
        if c_str.is_null() {
            return Err("Null pointer passed");
        }
        CStr::from_ptr(c_str)
            .to_str()
            .map(|s| s.to_string())
            .map_err(|_| "Invalid UTF-8 string")
    }

    // ========================================================================
    // Safety Wrapper Tests
    // ========================================================================

    #[test]
    fn test_validate_sdk_ptr_null() {
        let result = validate_sdk_ptr(ptr::null_mut());
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "SDK pointer is null");
    }

    #[test]
    fn test_validate_stream_ptr_null() {
        let result = validate_stream_ptr(ptr::null_mut());
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "Stream pointer is null");
    }

    #[test]
    fn test_write_error_result() {
        let mut result = CResult {
            success: true,
            error_message: ptr::null_mut(),
            is_retryable: false,
        };

        write_error_result(&mut result as *mut CResult, "Test error", true);

        assert!(!result.success);
        assert!(!result.error_message.is_null());
        assert!(result.is_retryable);

        // Clean up
        unsafe {
            if !result.error_message.is_null() {
                let _ = CString::from_raw(result.error_message);
            }
        }
    }

    #[test]
    fn test_write_success_result() {
        let mut result = CResult {
            success: false,
            error_message: CString::new("error").unwrap().into_raw(),
            is_retryable: true,
        };

        write_success_result(&mut result as *mut CResult);

        assert!(result.success);
        assert!(result.error_message.is_null());
        assert!(!result.is_retryable);
    }

    #[test]
    fn test_write_error_result_with_null_pointer() {
        // Should not panic when result pointer is null
        write_error_result(ptr::null_mut(), "Test error", false);
        // If we get here, test passed
    }

    #[test]
    fn test_write_success_result_with_null_pointer() {
        // Should not panic when result pointer is null
        write_success_result(ptr::null_mut());
        // If we get here, test passed
    }

    // ========================================================================
    // Header Key Cache Tests
    // ========================================================================

    #[test]
    fn test_intern_header_key_caches_keys() {
        // First call - should create new entry
        let key1 = intern_header_key("Authorization".to_string());

        // Second call with same string - should return cached entry
        let key2 = intern_header_key("Authorization".to_string());

        // Should be the same pointer (same address in memory)
        assert_eq!(key1.as_ptr(), key2.as_ptr());
    }

    #[test]
    fn test_intern_header_key_different_keys() {
        let key1 = intern_header_key("Authorization".to_string());
        let key2 = intern_header_key("Content-Type".to_string());

        // Different keys should have different pointers
        assert_ne!(key1.as_ptr(), key2.as_ptr());
        assert_eq!(key1, "Authorization");
        assert_eq!(key2, "Content-Type");
    }

    #[test]
    fn test_intern_header_key_prevents_duplicate_leaks() {
        // Clear the cache first (can't actually do this safely in test, but we can verify behavior)
        let initial_key = intern_header_key("X-Test-Header".to_string());

        // Call many times
        for _ in 0..100 {
            let key = intern_header_key("X-Test-Header".to_string());
            // All should point to the same memory location
            assert_eq!(initial_key.as_ptr(), key.as_ptr());
        }
    }

    // ========================================================================
    // CResult Tests
    // ========================================================================

    #[test]
    fn test_cresult_success() {
        let result = CResult::success();
        assert!(result.success);
        assert!(result.error_message.is_null());
        assert!(!result.is_retryable);
    }

    #[test]
    fn test_cresult_error() {
        let error = ZerobusError::InvalidArgument("Test error".to_string());
        let result = CResult::error(error);

        assert!(!result.success);
        assert!(!result.error_message.is_null());

        // Verify error message
        let msg = unsafe { CStr::from_ptr(result.error_message).to_string_lossy() };
        assert!(msg.contains("Test error"));

        // Clean up
        unsafe {
            let _ = CString::from_raw(result.error_message);
        }
    }

    // ========================================================================
    // Configuration Tests
    // ========================================================================

    #[test]
    fn test_stream_config_conversion() {
        let c_config = CStreamConfigurationOptions {
            max_inflight_requests: 100,
            recovery: true,
            recovery_timeout_ms: 5000,
            recovery_backoff_ms: 1000,
            recovery_retries: 3,
            server_lack_of_ack_timeout_ms: 10000,
            flush_timeout_ms: 2000,
            record_type: 1, // Proto
            stream_paused_max_wait_time_ms: 0,
            has_stream_paused_max_wait_time_ms: false,
            callback_max_wait_time_ms: 0,
            has_callback_max_wait_time_ms: false,
        };

        let rust_config: StreamConfigurationOptions = c_config.into();

        assert_eq!(rust_config.max_inflight_requests, 100);
        assert_eq!(rust_config.recovery, true);
        assert_eq!(rust_config.recovery_timeout_ms, 5000);
        assert_eq!(rust_config.recovery_retries, 3);
        assert_eq!(rust_config.record_type, RecordType::Proto);
    }

    #[test]
    fn test_stream_config_record_type_json() {
        let c_config = CStreamConfigurationOptions {
            max_inflight_requests: 50,
            recovery: false,
            recovery_timeout_ms: 0,
            recovery_backoff_ms: 0,
            recovery_retries: 0,
            server_lack_of_ack_timeout_ms: 0,
            flush_timeout_ms: 0,
            record_type: 2, // Json
            stream_paused_max_wait_time_ms: 0,
            has_stream_paused_max_wait_time_ms: false,
            callback_max_wait_time_ms: 0,
            has_callback_max_wait_time_ms: false,
        };

        let rust_config: StreamConfigurationOptions = c_config.into();
        assert_eq!(rust_config.record_type, RecordType::Json);
    }

    #[test]
    fn test_stream_config_record_type_unspecified() {
        let c_config = CStreamConfigurationOptions {
            max_inflight_requests: 50,
            recovery: false,
            recovery_timeout_ms: 0,
            recovery_backoff_ms: 0,
            recovery_retries: 0,
            server_lack_of_ack_timeout_ms: 0,
            flush_timeout_ms: 0,
            record_type: 999, // Invalid
            stream_paused_max_wait_time_ms: 0,
            has_stream_paused_max_wait_time_ms: false,
            callback_max_wait_time_ms: 0,
            has_callback_max_wait_time_ms: false,
        };

        let rust_config: StreamConfigurationOptions = c_config.into();
        assert_eq!(rust_config.record_type, RecordType::Unspecified);
    }

    #[test]
    fn test_get_default_config() {
        let config = zerobus_get_default_config();

        // Verify it returns reasonable defaults
        assert!(config.max_inflight_requests > 0);
        assert_eq!(config.record_type, 1); // Proto
    }

    // ========================================================================
    // C String Conversion Tests
    // ========================================================================

    #[test]
    fn test_c_str_to_string_valid() {
        let test_str = CString::new("Hello, World!").unwrap();
        let result = unsafe { test_c_str_to_string(test_str.as_ptr()) };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello, World!");
    }

    #[test]
    fn test_c_str_to_string_null() {
        let result = unsafe { test_c_str_to_string(ptr::null()) };
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Null pointer passed");
    }

    #[test]
    fn test_c_str_to_string_empty() {
        let test_str = CString::new("").unwrap();
        let result = unsafe { test_c_str_to_string(test_str.as_ptr()) };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    // ========================================================================
    // Memory Management Tests
    // ========================================================================

    #[test]
    fn test_zerobus_free_error_message_null() {
        // Should not panic with null pointer
        zerobus_free_error_message(ptr::null_mut());
    }

    #[test]
    fn test_zerobus_free_error_message_valid() {
        let msg = CString::new("Test error").unwrap().into_raw();
        zerobus_free_error_message(msg);
        // If we get here without crashing, test passed
    }

    // ========================================================================
    // Thread Safety Tests
    // ========================================================================

    #[test]
    fn test_callback_headers_provider_sequential() {
        extern "C" fn test_callback(_user_data: *mut std::ffi::c_void) -> CHeaders {
            CHeaders {
                headers: ptr::null_mut(),
                count: 0,
                error_message: ptr::null_mut(),
            }
        }

        let provider = CallbackHeadersProvider::new(test_callback, ptr::null_mut());

        // Sequential calls should work fine
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result1 = rt.block_on(provider.get_headers());
        assert!(result1.is_ok());

        let result2 = rt.block_on(provider.get_headers());
        assert!(result2.is_ok());
    }

    #[test]
    fn test_callback_headers_provider_returns_headers() {
        extern "C" fn test_callback(_user_data: *mut std::ffi::c_void) -> CHeaders {
            // Create simple test headers
            let auth_key = CString::new("Authorization").unwrap().into_raw();
            let auth_val = CString::new("Bearer test-token").unwrap().into_raw();

            let header = Box::new(crate::CHeader {
                key: auth_key,
                value: auth_val,
            });

            CHeaders {
                headers: Box::into_raw(header),
                count: 1,
                error_message: ptr::null_mut(),
            }
        }

        let provider = CallbackHeadersProvider::new(test_callback, ptr::null_mut());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(provider.get_headers());

        assert!(result.is_ok());
        let headers = result.unwrap();
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key("Authorization"));
    }
}
