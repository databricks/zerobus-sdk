//! Tokio runtime management for JNI.
//!
//! This module manages the global Tokio runtime used for all async operations
//! in the Zerobus SDK. The runtime is initialized when the JNI library is loaded
//! and persists for the lifetime of the JVM.

use jni::JavaVM;
use std::cell::Cell;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Global Tokio runtime for async operations.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Global JVM reference for attaching threads in async callbacks.
static JVM: OnceLock<JavaVM> = OnceLock::new();

/// Number of worker threads for the Tokio runtime.
const WORKER_THREADS: usize = 4;

/// Initialize the global Tokio runtime.
///
/// This function is idempotent - calling it multiple times has no effect after
/// the first successful initialization.
pub fn init_runtime() {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKER_THREADS)
            .enable_all()
            .thread_name("zerobus-tokio")
            .build()
            .expect("Failed to create Tokio runtime")
    });
}

/// Get a reference to the global Tokio runtime.
///
/// # Panics
///
/// Panics if the runtime has not been initialized (JNI_OnLoad not called).
pub fn get_runtime() -> &'static Runtime {
    RUNTIME
        .get()
        .expect("Tokio runtime not initialized - JNI_OnLoad must be called first")
}

/// Set the global JVM reference.
///
/// # Errors
///
/// Returns an error if the JVM has already been set.
pub fn set_jvm(vm: JavaVM) -> Result<(), &'static str> {
    JVM.set(vm).map_err(|_| "JVM already set")
}

/// Get a reference to the global JVM.
///
/// # Panics
///
/// Panics if the JVM has not been set (JNI_OnLoad not called).
pub fn get_jvm() -> &'static JavaVM {
    JVM.get()
        .expect("JVM not set - JNI_OnLoad must be called first")
}

thread_local! {
    static IN_CALLBACK: Cell<bool> = const { Cell::new(false) };
}

/// Marks the current thread as running user Java callback code until dropped.
///
/// Hold one around every call into user Java code made from a Tokio worker
/// thread, so that a blocking call from that code fails with a clear error.
pub(crate) struct CallbackScope {
    previous: bool,
}

impl CallbackScope {
    pub(crate) fn enter() -> Self {
        Self {
            previous: IN_CALLBACK.with(|flag| flag.replace(true)),
        }
    }
}

impl Drop for CallbackScope {
    fn drop(&mut self) {
        IN_CALLBACK.with(|flag| flag.set(self.previous));
    }
}

/// Unwind payload raised by [`block_on`] inside a [`CallbackScope`].
///
/// [`crate::errors::guard`] converts it into an `IllegalStateException`.
pub(crate) struct BlockingCallInCallback;

pub(crate) const BLOCKING_CALL_IN_CALLBACK_MESSAGE: &str =
    "Blocking stream methods (ingestRecordOffset, ingestRecordsOffset, ingestBatch, \
     waitForOffset, flush, close, isClosed, getUnackedRecords, getUnackedBatches, and \
     ZerobusSdk.recreateStream / recreateArrowStream) cannot be called from an AckCallback: callbacks run on SDK threads that must not block. Hand the work \
     to another thread instead.";

/// Execute a blocking operation on the Tokio runtime.
///
/// This function blocks the current thread until the future completes.
/// Inside a [`CallbackScope`] it unwinds with [`BlockingCallInCallback`]
/// instead, since the thread is a Tokio worker where blocking would panic.
pub fn block_on<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    if IN_CALLBACK.with(Cell::get) {
        std::panic::resume_unwind(Box::new(BlockingCallInCallback));
    }
    get_runtime().block_on(future)
}

/// Spawn a future on the Tokio runtime.
///
/// The future will run asynchronously on the runtime's thread pool.
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    get_runtime().spawn(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_on_inside_callback_scope_unwinds_with_marker() {
        init_runtime();
        let scope = CallbackScope::enter();
        let payload = std::panic::catch_unwind(|| block_on(async {})).unwrap_err();
        assert!(payload.is::<BlockingCallInCallback>());

        drop(scope);
        block_on(async {});
    }

    #[test]
    fn callback_scope_resets_when_callback_unwinds() {
        init_runtime();
        let unwound = std::panic::catch_unwind(|| {
            let _scope = CallbackScope::enter();
            std::panic::resume_unwind(Box::new("callback failed"));
        });
        assert!(unwound.is_err());
        block_on(async {});
    }
}
