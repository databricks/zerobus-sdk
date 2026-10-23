//! Locates a `protoc` binary for the proto-compiling paths in this tool.
//!
//! `tonic_prost_build::compile_protos` shells out to `protoc` and only honors
//! the `PROTOC` environment variable. The main SDK builds fine without a
//! system protoc because its build script sets `PROTOC` from
//! `protoc-bin-vendored`; the CLI and tests in this crate compile protos too,
//! but nothing sets it for them, so they need a system protoc on PATH.
//!
//! Resolution order:
//! 1. `PROTOC` env var, if set — an explicit user override always wins.
//! 2. Vendored protoc from `protoc-bin-vendored`.
//!
//! This keeps the tool and its tests working on machines without a system
//! protoc, without overriding a protoc the caller explicitly chose.

use anyhow::{Context, Result};

pub fn resolve_protoc() -> Result<()> {
    if std::env::var_os("PROTOC").is_some() {
        return Ok(());
    }

    let vendored = protoc_bin_vendored::protoc_bin_path()
        .context("PROTOC is not set and no vendored protoc is available for this platform")?;
    // Edition 2024 marks `set_var` unsafe; single-threaded CLI and build-time
    // use here, matching the sdk build script's usage.
    unsafe {
        std::env::set_var("PROTOC", vendored);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env manipulation must be serialized across tests that touch PROTOC.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn saved_protoc() -> Option<std::ffi::OsString> {
        std::env::var_os("PROTOC")
    }

    fn restore_protoc(saved: Option<std::ffi::OsString>) {
        // Edition 2024: set_var is unsafe. Tests run single-threaded w.r.t.
        // the env lock above, so the write is not racing anything here.
        unsafe {
            match saved {
                Some(v) => std::env::set_var("PROTOC", v),
                None => std::env::remove_var("PROTOC"),
            }
        }
    }

    /// With PROTOC unset, resolution must succeed via the vendored binary on
    /// every supported platform and leave PROTOC pointing at a real file.
    #[test]
    fn falls_back_to_vendored_when_unset() {
        let _guard = env_lock();
        let saved = saved_protoc();
        unsafe {
            std::env::remove_var("PROTOC");
        }
        resolve_protoc().expect("vendored protoc should resolve on supported platforms");
        let resolved = std::env::var_os("PROTOC").expect("resolve must set PROTOC");
        restore_protoc(saved);
        assert!(std::path::Path::new(&resolved).exists());
    }

    /// An explicitly set PROTOC must never be overwritten. The probe value is
    /// the vendored binary itself so a concurrent proto-compiling test that
    /// happens to read PROTOC mid-test still sees a working protoc.
    #[test]
    fn honors_existing_protoc_env() {
        let _guard = env_lock();
        let saved = saved_protoc();
        let vendored = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
        unsafe {
            std::env::set_var("PROTOC", &vendored);
        }
        resolve_protoc().expect("set PROTOC must short-circuit");
        let after = std::env::var_os("PROTOC").expect("PROTOC still set");
        restore_protoc(saved);
        assert_eq!(after, vendored, "existing PROTOC must not be overwritten");
    }
}
