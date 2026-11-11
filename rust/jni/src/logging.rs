//! Forwards core `tracing` events to SLF4J.
//!
//! Events from the SDK crates pass a level gate whose threshold Java pushes (see
//! `NativeLogBridge`), then go through a bounded queue to one daemon thread attached to
//! the JVM, which calls `NativeLogBridge.log`. Producers never block: a full queue drops
//! the event and the forwarder reports how many were dropped. The forwarder ignores its
//! own events, so a log emitted while forwarding cannot re-enter the queue.

use std::cell::Cell;
use std::fmt::{self, Debug, Write as _};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use jni::objects::{JStaticMethodID, JValue};
use jni::signature::{Primitive, ReturnType};
use jni::sys::jint;
use jni::JNIEnv;
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter, Layer, SubscriberExt};
use tracing_subscriber::Registry;

use crate::class_cache::{as_jclass, get_class_cache};
use crate::runtime::get_jvm;

const LEVEL_OFF: u8 = 0;
const LEVEL_ERROR: u8 = 1;
const LEVEL_WARN: u8 = 2;
const LEVEL_INFO: u8 = 3;
const LEVEL_DEBUG: u8 = 4;
const LEVEL_TRACE: u8 = 5;

/// Crates whose events are forwarded. Third-party crates (hyper, h2, tonic) are rejected
/// per callsite so their debug output never reaches the queue.
const FORWARDED_CRATES: &[&str] = &["databricks_zerobus_ingest_sdk", "zerobus_jni"];

/// Target for the forwarder's own dropped-events report.
const FORWARDER_TARGET: &str = "zerobus_jni";

const QUEUE_CAPACITY: usize = 4096;

/// Upper bound on how often the forwarder reports dropped events.
const DROP_REPORT_INTERVAL: Duration = Duration::from_secs(1);

const LOG_SIGNATURE: &str = "(ILjava/lang/String;Ljava/lang/String;)V";

/// Highest level code forwarded; `LEVEL_OFF` forwards nothing. Written by Java.
static MAX_LEVEL: AtomicU8 = AtomicU8::new(LEVEL_OFF);

thread_local! {
    static IS_FORWARDER: Cell<bool> = const { Cell::new(false) };
}

/// Sets the highest forwarded level. Codes: 0 off, 1 error, 2 warn, 3 info, 4 debug,
/// 5 trace. Unknown codes disable forwarding rather than widen it.
pub fn set_max_level(code: jint) {
    let code = if (i32::from(LEVEL_ERROR)..=i32::from(LEVEL_TRACE)).contains(&code) {
        code as u8
    } else {
        LEVEL_OFF
    };
    // The global max level and the callsite interest cache are derived from the gate's hint
    // and only refresh on rebuild.
    if MAX_LEVEL.swap(code, Ordering::Relaxed) != code {
        tracing::callsite::rebuild_interest_cache();
    }
}

/// Installs the global subscriber and starts the forwarder thread. Idempotent; a failure
/// leaves core logging unforwarded rather than failing library load.
pub fn install() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let (tx, rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let subscriber = build_subscriber(tx, dropped.clone(), &MAX_LEVEL);
        if tracing::subscriber::set_global_default(subscriber).is_err() {
            return;
        }
        let _ = std::thread::Builder::new()
            .name("zerobus-native-log".into())
            .spawn(move || run_forwarder(rx, dropped));
    });
}

fn build_subscriber(
    tx: SyncSender<LogRecord>,
    dropped: Arc<AtomicU64>,
    max_level: &'static AtomicU8,
) -> impl Subscriber + Send + Sync + 'static {
    let layer = ForwardLayer { tx, dropped };
    Registry::default().with(layer.with_filter(LevelGate { max_level }))
}

#[no_mangle]
pub extern "system" fn Java_com_databricks_zerobus_NativeLogBridge_nativeSetLogLevel<'local>(
    _env: JNIEnv<'local>,
    _class: jni::objects::JClass<'local>,
    level: jint,
) {
    set_max_level(level);
}

fn level_code(level: &Level) -> u8 {
    if *level == Level::ERROR {
        LEVEL_ERROR
    } else if *level == Level::WARN {
        LEVEL_WARN
    } else if *level == Level::INFO {
        LEVEL_INFO
    } else if *level == Level::DEBUG {
        LEVEL_DEBUG
    } else {
        LEVEL_TRACE
    }
}

fn is_forwarded_target(target: &str) -> bool {
    FORWARDED_CRATES.iter().any(|krate| {
        target == *krate
            || target
                .strip_prefix(krate)
                .is_some_and(|rest| rest.starts_with("::"))
    })
}

struct LogRecord {
    level: u8,
    target: &'static str,
    message: String,
}

fn level_filter(code: u8) -> LevelFilter {
    match code {
        LEVEL_ERROR => LevelFilter::ERROR,
        LEVEL_WARN => LevelFilter::WARN,
        LEVEL_INFO => LevelFilter::INFO,
        LEVEL_DEBUG => LevelFilter::DEBUG,
        LEVEL_TRACE => LevelFilter::TRACE,
        _ => LevelFilter::OFF,
    }
}

/// Admits events at or below the level in `max_level`. The hint follows the threshold, so
/// disabled levels short-circuit at the macro. `set_max_level` rebuilds the cache on change.
struct LevelGate {
    max_level: &'static AtomicU8,
}

impl<S: Subscriber> Filter<S> for LevelGate {
    fn enabled(&self, meta: &Metadata<'_>, _: &Context<'_, S>) -> bool {
        is_forwarded_target(meta.target())
            && level_code(meta.level()) <= self.max_level.load(Ordering::Relaxed)
    }

    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        if is_forwarded_target(meta.target()) {
            Interest::sometimes()
        } else {
            Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(level_filter(self.max_level.load(Ordering::Relaxed)))
    }
}

struct ForwardLayer {
    tx: SyncSender<LogRecord>,
    dropped: Arc<AtomicU64>,
}

impl<S: Subscriber> Layer<S> for ForwardLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if IS_FORWARDER.with(Cell::get) {
            return;
        }
        let meta = event.metadata();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let record = LogRecord {
            level: level_code(meta.level()),
            target: meta.target(),
            message: visitor.into_message(),
        };
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) = self.tx.try_send(record)
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Renders an event as `message key=value ...`. Fields other than `message` keep their
/// order and are appended after the message.
#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl MessageVisitor {
    fn push_field(&mut self, name: &str, value: fmt::Arguments<'_>) {
        if !self.fields.is_empty() {
            self.fields.push(' ');
        }
        let _ = write!(self.fields, "{name}={value}");
    }

    fn into_message(self) -> String {
        if self.fields.is_empty() {
            self.message
        } else if self.message.is_empty() {
            self.fields
        } else {
            let mut out = self.message;
            out.push(' ');
            out.push_str(&self.fields);
            out
        }
    }
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            self.push_field(field.name(), format_args!("{value}"));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            self.push_field(field.name(), format_args!("{value:?}"));
        }
    }
}

fn run_forwarder(rx: Receiver<LogRecord>, dropped: Arc<AtomicU64>) {
    IS_FORWARDER.with(|flag| flag.set(true));
    let jvm = get_jvm();
    let mut env = match jvm.attach_current_thread_as_daemon() {
        Ok(env) => env,
        Err(_) => return,
    };
    let class = as_jclass(&get_class_cache().native_log_bridge_class);
    if env
        .call_static_method(&class, "initForwarderThread", "()V", &[])
        .is_err()
    {
        let _ = env.exception_clear();
    }
    let method = match env.get_static_method_id(&class, "log", LOG_SIGNATURE) {
        Ok(method) => method,
        Err(_) => {
            let _ = env.exception_clear();
            return;
        }
    };

    let mut last_report = Instant::now();
    loop {
        // Wake on a timeout only while drops are waiting to be reported.
        let next = if dropped.load(Ordering::Relaxed) > 0 {
            rx.recv_timeout(DROP_REPORT_INTERVAL)
        } else {
            rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
        };
        match next {
            Ok(record) => forward(
                &mut env,
                method,
                record.level,
                record.target,
                &record.message,
            ),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        if last_report.elapsed() >= DROP_REPORT_INTERVAL {
            let n = dropped.swap(0, Ordering::Relaxed);
            if n > 0 {
                let message =
                    format!("dropped {n} native log events because the forwarding queue was full");
                forward(&mut env, method, LEVEL_WARN, FORWARDER_TARGET, &message);
            }
            last_report = Instant::now();
        }
    }
}

fn forward(env: &mut JNIEnv, method: JStaticMethodID, level: u8, target: &str, message: &str) {
    let class = as_jclass(&get_class_cache().native_log_bridge_class);
    let result = env.with_local_frame(4, |env| -> jni::errors::Result<()> {
        let target = env.new_string(target)?;
        let message = env.new_string(message)?;
        let args = [
            JValue::Int(i32::from(level)).as_jni(),
            JValue::Object(&target).as_jni(),
            JValue::Object(&message).as_jni(),
        ];
        // Safety: `method` was resolved from NativeLogBridge.log with LOG_SIGNATURE and the
        // arguments above match it.
        unsafe {
            env.call_static_method_unchecked(
                &class,
                method,
                ReturnType::Primitive(Primitive::Void),
                &args,
            )?;
        }
        Ok(())
    });
    if result.is_err() {
        let _ = env.exception_clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture(
        max_level: &'static AtomicU8,
        capacity: usize,
        emit: impl FnOnce(),
    ) -> (Vec<LogRecord>, u64) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let subscriber = build_subscriber(tx, dropped.clone(), max_level);
        tracing::subscriber::with_default(subscriber, emit);
        (rx.try_iter().collect(), dropped.load(Ordering::Relaxed))
    }

    #[test]
    fn gate_admits_only_levels_up_to_threshold() {
        static MAX: AtomicU8 = AtomicU8::new(LEVEL_WARN);
        let (records, dropped) = capture(&MAX, 64, || {
            tracing::error!(target: "databricks_zerobus_ingest_sdk::gate", "e");
            tracing::warn!(target: "databricks_zerobus_ingest_sdk::gate", "w");
            tracing::info!(target: "databricks_zerobus_ingest_sdk::gate", "i");
            tracing::debug!(target: "databricks_zerobus_ingest_sdk::gate", "d");
            tracing::trace!(target: "databricks_zerobus_ingest_sdk::gate", "t");
        });
        let levels: Vec<u8> = records.iter().map(|r| r.level).collect();
        assert_eq!(levels, vec![LEVEL_ERROR, LEVEL_WARN]);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn gate_off_forwards_nothing() {
        static MAX: AtomicU8 = AtomicU8::new(LEVEL_OFF);
        let (records, _) = capture(&MAX, 64, || {
            tracing::error!(target: "databricks_zerobus_ingest_sdk::off", "e");
        });
        assert!(records.is_empty());
    }

    #[test]
    fn gate_rejects_third_party_targets_even_at_trace() {
        static MAX: AtomicU8 = AtomicU8::new(LEVEL_TRACE);
        let (records, _) = capture(&MAX, 64, || {
            tracing::debug!(target: "h2::codec", "frame");
            tracing::info!(target: "databricks_zerobus_ingest_sdk::sdk", "kept");
            tracing::info!(target: "databricks_zerobus_ingest_sdkx::nope", "dropped");
        });
        let targets: Vec<&str> = records.iter().map(|r| r.target).collect();
        assert_eq!(targets, vec!["databricks_zerobus_ingest_sdk::sdk"]);
    }

    #[test]
    fn message_comes_first_then_fields_in_order() {
        static MAX: AtomicU8 = AtomicU8::new(LEVEL_TRACE);
        let (records, _) = capture(&MAX, 64, || {
            tracing::warn!(
                target: "databricks_zerobus_ingest_sdk::fmt",
                table = "main.default.t1",
                attempt = 2u32,
                error = %"boom",
                "connection failed"
            );
            tracing::info!(target: "databricks_zerobus_ingest_sdk::fmt", table = "x");
        });
        assert_eq!(
            records[0].message,
            "connection failed table=main.default.t1 attempt=2 error=boom"
        );
        assert_eq!(records[1].message, "table=x");
    }

    #[test]
    fn full_queue_drops_and_counts_without_blocking() {
        static MAX: AtomicU8 = AtomicU8::new(LEVEL_TRACE);
        let (records, dropped) = capture(&MAX, 1, || {
            for i in 0..3u32 {
                tracing::info!(target: "databricks_zerobus_ingest_sdk::queue", n = i, "event");
            }
        });
        assert_eq!(records.len(), 1);
        assert_eq!(dropped, 2);
    }

    // Uses the global threshold, so it is the only test that touches MAX_LEVEL.
    #[test]
    fn level_change_after_install_takes_effect() {
        let previous = MAX_LEVEL.load(Ordering::Relaxed);
        set_max_level(9);
        assert_eq!(MAX_LEVEL.load(Ordering::Relaxed), LEVEL_OFF);

        let (tx, rx) = mpsc::sync_channel(64);
        let subscriber = build_subscriber(tx, Arc::new(AtomicU64::new(0)), &MAX_LEVEL);
        tracing::subscriber::with_default(subscriber, || {
            set_max_level(i32::from(LEVEL_WARN));
            tracing::debug!(target: "databricks_zerobus_ingest_sdk::level", "below");
            set_max_level(i32::from(LEVEL_DEBUG));
            tracing::debug!(target: "databricks_zerobus_ingest_sdk::level", "above");
        });
        set_max_level(i32::from(previous));

        let messages: Vec<String> = rx.try_iter().map(|r| r.message).collect();
        assert_eq!(messages, vec!["above".to_string()]);
    }
}
