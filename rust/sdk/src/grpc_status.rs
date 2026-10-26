//! SDK-owned gRPC status types.
//!
//! [`ZerobusError`](crate::ZerobusError) reports server failures with these
//! types instead of `tonic::Status` and `tonic::Code`. `tonic` is pre-1.0, so
//! naming its types in the public API would make every `tonic` upgrade a
//! breaking change for this crate.

use std::fmt;

/// A gRPC status code.
///
/// The variants match the [gRPC status codes]. The `Display` output of each
/// variant is the same text that `tonic::Code` prints.
///
/// [gRPC status codes]: https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GrpcCode {
    /// The operation completed successfully.
    Ok,
    /// The operation was cancelled.
    Cancelled,
    /// Unknown error.
    Unknown,
    /// Client specified an invalid argument.
    InvalidArgument,
    /// Deadline expired before operation could complete.
    DeadlineExceeded,
    /// Some requested entity was not found.
    NotFound,
    /// Some entity that we attempted to create already exists.
    AlreadyExists,
    /// The caller does not have permission to execute the specified operation.
    PermissionDenied,
    /// Some resource has been exhausted.
    ResourceExhausted,
    /// The system is not in a state required for the operation's execution.
    FailedPrecondition,
    /// The operation was aborted.
    Aborted,
    /// Operation was attempted past the valid range.
    OutOfRange,
    /// Operation is not implemented or not supported.
    Unimplemented,
    /// Internal error.
    Internal,
    /// The service is currently unavailable.
    Unavailable,
    /// Unrecoverable data loss or corruption.
    DataLoss,
    /// The request does not have valid authentication credentials.
    Unauthenticated,
}

impl GrpcCode {
    /// Maps a `tonic::Code` to the SDK code.
    ///
    /// The match has no wildcard arm on purpose. A `tonic` upgrade that adds a
    /// code fails to compile here, so the new code gets a mapping.
    pub(crate) fn from_tonic(code: tonic::Code) -> Self {
        match code {
            tonic::Code::Ok => GrpcCode::Ok,
            tonic::Code::Cancelled => GrpcCode::Cancelled,
            tonic::Code::Unknown => GrpcCode::Unknown,
            tonic::Code::InvalidArgument => GrpcCode::InvalidArgument,
            tonic::Code::DeadlineExceeded => GrpcCode::DeadlineExceeded,
            tonic::Code::NotFound => GrpcCode::NotFound,
            tonic::Code::AlreadyExists => GrpcCode::AlreadyExists,
            tonic::Code::PermissionDenied => GrpcCode::PermissionDenied,
            tonic::Code::ResourceExhausted => GrpcCode::ResourceExhausted,
            tonic::Code::FailedPrecondition => GrpcCode::FailedPrecondition,
            tonic::Code::Aborted => GrpcCode::Aborted,
            tonic::Code::OutOfRange => GrpcCode::OutOfRange,
            tonic::Code::Unimplemented => GrpcCode::Unimplemented,
            tonic::Code::Internal => GrpcCode::Internal,
            tonic::Code::Unavailable => GrpcCode::Unavailable,
            tonic::Code::DataLoss => GrpcCode::DataLoss,
            tonic::Code::Unauthenticated => GrpcCode::Unauthenticated,
        }
    }

    fn description(&self) -> &'static str {
        match self {
            GrpcCode::Ok => "The operation completed successfully",
            GrpcCode::Cancelled => "The operation was cancelled",
            GrpcCode::Unknown => "Unknown error",
            GrpcCode::InvalidArgument => "Client specified an invalid argument",
            GrpcCode::DeadlineExceeded => "Deadline expired before operation could complete",
            GrpcCode::NotFound => "Some requested entity was not found",
            GrpcCode::AlreadyExists => "Some entity that we attempted to create already exists",
            GrpcCode::PermissionDenied => {
                "The caller does not have permission to execute the specified operation"
            }
            GrpcCode::ResourceExhausted => "Some resource has been exhausted",
            GrpcCode::FailedPrecondition => {
                "The system is not in a state required for the operation's execution"
            }
            GrpcCode::Aborted => "The operation was aborted",
            GrpcCode::OutOfRange => "Operation was attempted past the valid range",
            GrpcCode::Unimplemented => "Operation is not implemented or not supported",
            GrpcCode::Internal => "Internal error",
            GrpcCode::Unavailable => "The service is currently unavailable",
            GrpcCode::DataLoss => "Unrecoverable data loss or corruption",
            GrpcCode::Unauthenticated => {
                "The request does not have valid authentication credentials"
            }
        }
    }
}

impl fmt::Display for GrpcCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.description(), f)
    }
}

/// A gRPC status that the Zerobus server or the SDK reports for a failed stream.
///
/// `ZerobusError::CreateStreamError` and `ZerobusError::StreamClosedError`
/// carry this type. Use [`code`](Self::code) to branch on the failure and
/// [`message`](Self::message) to read the text.
///
/// The `Display` output is the same text that `tonic::Status` prints:
/// `code: '<description>'`, then `, message: "<message>"` when the message is
/// not empty, then `, source: <cause>` when the status wraps an underlying error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrpcStatus {
    code: GrpcCode,
    message: String,
    /// Debug output of the underlying error that the transport attached, if any.
    source: Option<String>,
}

impl GrpcStatus {
    /// Creates a status with a code and a message.
    pub fn new(code: GrpcCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    /// Returns the status code.
    pub fn code(&self) -> GrpcCode {
        self.code
    }

    /// Returns the status message. The message can be empty.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Converts a `tonic::Status`, keeping its code, message, and cause.
    ///
    /// This is a function and not a `From` impl on purpose. A public `From`
    /// impl would name `tonic::Status` in the public API.
    pub(crate) fn from_tonic(status: &tonic::Status) -> Self {
        Self {
            code: GrpcCode::from_tonic(status.code()),
            message: status.message().to_string(),
            source: std::error::Error::source(status).map(|source| format!("{source:?}")),
        }
    }

    /// Converts a Flight error through `tonic::Status`.
    ///
    /// The conversion keeps the inner gRPC code of a `FlightError::Tonic`.
    /// `tonic::Status::from_error` would flatten it to `Unknown` and break
    /// the retry and authentication classification.
    #[cfg(feature = "arrow-flight")]
    pub(crate) fn from_flight_error(error: arrow_flight::error::FlightError) -> Self {
        Self::from_tonic(&tonic::Status::from(error))
    }

    pub(crate) fn ok(message: impl Into<String>) -> Self {
        Self::new(GrpcCode::Ok, message)
    }

    #[cfg(feature = "arrow-flight")]
    pub(crate) fn unknown(message: impl Into<String>) -> Self {
        Self::new(GrpcCode::Unknown, message)
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(GrpcCode::InvalidArgument, message)
    }

    pub(crate) fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::new(GrpcCode::DeadlineExceeded, message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(GrpcCode::Internal, message)
    }

    #[cfg(any(test, feature = "arrow-flight"))]
    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(GrpcCode::Unavailable, message)
    }

    #[cfg(test)]
    pub(crate) fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(GrpcCode::PermissionDenied, message)
    }

    #[cfg(test)]
    pub(crate) fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(GrpcCode::Unauthenticated, message)
    }
}

impl fmt::Display for GrpcStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "code: '{}'", self.code)?;

        if !self.message.is_empty() {
            write!(f, ", message: {:?}", self.message)?;
        }
        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for GrpcStatus {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    const ALL_TONIC_CODES: [tonic::Code; 17] = [
        tonic::Code::Ok,
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
        tonic::Code::InvalidArgument,
        tonic::Code::DeadlineExceeded,
        tonic::Code::NotFound,
        tonic::Code::AlreadyExists,
        tonic::Code::PermissionDenied,
        tonic::Code::ResourceExhausted,
        tonic::Code::FailedPrecondition,
        tonic::Code::Aborted,
        tonic::Code::OutOfRange,
        tonic::Code::Unimplemented,
        tonic::Code::Internal,
        tonic::Code::Unavailable,
        tonic::Code::DataLoss,
        tonic::Code::Unauthenticated,
    ];

    /// An error that `tonic::Status::from_error` can wrap as a cause.
    #[derive(Debug)]
    struct Cause(&'static str);

    impl fmt::Display for Cause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl Error for Cause {}

    /// `GrpcCode` must print the same text as `tonic::Code` for every code.
    #[test]
    fn code_display_matches_tonic_for_every_code() {
        for tonic_code in ALL_TONIC_CODES {
            assert_eq!(
                GrpcCode::from_tonic(tonic_code).to_string(),
                tonic_code.to_string(),
                "Display mismatch for {tonic_code:?}"
            );
        }
    }

    /// The code mapping is one-to-one and keeps the numeric gRPC value order.
    #[test]
    fn code_mapping_is_one_to_one() {
        let mapped: Vec<GrpcCode> = ALL_TONIC_CODES
            .iter()
            .map(|code| GrpcCode::from_tonic(*code))
            .collect();
        for (i, a) in mapped.iter().enumerate() {
            for b in &mapped[i + 1..] {
                assert_ne!(a, b);
            }
        }
        assert_eq!(GrpcCode::from_tonic(tonic::Code::Ok), GrpcCode::Ok);
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::Unauthenticated),
            GrpcCode::Unauthenticated
        );
    }

    /// `GrpcStatus` must print the same text as `tonic::Status` for every code,
    /// with and without a message.
    #[test]
    fn status_display_matches_tonic_for_every_code() {
        for code in ALL_TONIC_CODES {
            for message in ["", "boom", "has \"quotes\" and a\nnewline"] {
                let tonic_status = tonic::Status::new(code, message);
                let status = GrpcStatus::from_tonic(&tonic_status);
                assert_eq!(
                    status.to_string(),
                    tonic_status.to_string(),
                    "Display mismatch for {code:?} / {message:?}"
                );
            }
        }
    }

    /// A status that wraps a cause prints the cause the way `tonic::Status` does.
    #[test]
    fn status_display_matches_tonic_when_status_has_a_source() {
        let mut tonic_status = tonic::Status::new(tonic::Code::Unavailable, "transport failed");
        tonic_status.set_source(std::sync::Arc::new(Cause("connection refused")));
        assert!(tonic_status.source().is_some());

        let status = GrpcStatus::from_tonic(&tonic_status);

        assert_eq!(status.to_string(), tonic_status.to_string());
        assert!(
            status.to_string().contains("connection refused"),
            "the cause must appear in the text: {status}"
        );
    }

    #[test]
    fn from_tonic_keeps_code_and_message() {
        let status = GrpcStatus::from_tonic(&tonic::Status::permission_denied("no access"));

        assert_eq!(status.code(), GrpcCode::PermissionDenied);
        assert_eq!(status.message(), "no access");
    }

    #[test]
    fn new_status_has_no_source() {
        let status = GrpcStatus::new(GrpcCode::Internal, "x");

        assert_eq!(status.code(), GrpcCode::Internal);
        assert_eq!(status.message(), "x");
        assert_eq!(status.to_string(), tonic::Status::internal("x").to_string());
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn from_flight_error_keeps_the_inner_grpc_code() {
        use arrow_flight::error::FlightError;

        let status = GrpcStatus::from_flight_error(FlightError::Tonic(Box::new(
            tonic::Status::permission_denied("denied"),
        )));

        assert_eq!(status.code(), GrpcCode::PermissionDenied);
        assert_eq!(status.message(), "denied");
    }
}
