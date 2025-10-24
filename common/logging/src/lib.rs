use std::{
    fs,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Local, NaiveDate};
use parking_lot::Mutex;
use tracing_appender::non_blocking::{self, WorkerGuard};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

const GLOBAL_LOG_DIR_ENV: &str = "EVENTDBX_LOG_DIR";
const PLUGIN_LOG_DIR_ENV: &str = "EVENTDBX_PLUGIN_LOG_DIR";

static FILE_GUARD: OnceLock<WorkerGuard> = OnceLock::new();
static PANIC_HOOK: OnceLock<()> = OnceLock::new();

/// Configuration for initializing logging inside a plugin.
pub struct LoggingOptions<'a> {
    pub plugin_name: &'a str,
    pub default_filter: &'a str,
    pub log_dir: Option<PathBuf>,
}

impl<'a> LoggingOptions<'a> {
    pub fn new(plugin_name: &'a str, default_filter: &'a str) -> Self {
        Self {
            plugin_name,
            default_filter,
            log_dir: None,
        }
    }

    pub fn with_log_dir(mut self, dir: PathBuf) -> Self {
        self.log_dir = Some(dir);
        self
    }
}

/// Initialize tracing for a plugin with daily file rotation and stdout forwarding.
pub fn init(options: LoggingOptions<'_>) -> Result<()> {
    if FILE_GUARD.get().is_some() {
        return Ok(());
    }

    let log_dir = resolve_log_dir(options.plugin_name, options.log_dir)?;
    let writer = DailyRotatingWriter::new(log_dir, options.plugin_name)?;
    let (file_writer, guard) = non_blocking::NonBlockingBuilder::default()
        .lossy(false)
        .finish(writer);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(options.default_filter));

    let stdout_layer = fmt::layer().with_target(false);
    let file_layer = fmt::layer()
        .with_writer(file_writer.clone())
        .with_target(true)
        .with_ansi(false);

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer);

    match subscriber.try_init() {
        Ok(_) => {
            let _ = FILE_GUARD.set(guard);
            install_panic_hook();
        }
        Err(_) => {
            drop(guard);
        }
    }

    Ok(())
}

/// Convenience helper with default configuration.
pub fn init_default(plugin_name: &str, default_filter: &str) -> Result<()> {
    init(LoggingOptions::new(plugin_name, default_filter))
}

#[derive(Clone)]
struct DailyRotatingWriter {
    inner: Arc<WriterInner>,
}

struct WriterInner {
    state: Mutex<WriterState>,
    log_dir: PathBuf,
    rotated_prefix: String,
    active_filename: String,
}

struct WriterState {
    file: Option<BufWriter<fs::File>>,
    current_day: NaiveDate,
    period_start: DateTime<Local>,
}

impl DailyRotatingWriter {
    fn new<P: Into<PathBuf>>(dir: P, plugin_name: &str) -> Result<Self> {
        let log_dir = dir.into();
        fs::create_dir_all(&log_dir)
            .with_context(|| format!("failed to create log directory {}", log_dir.display()))?;

        let active_filename = format!("{}.log", plugin_name);
        let rotated_prefix = plugin_name.to_string();
        let active_path = log_dir.join(&active_filename);
        let now = Local::now();

        Self::rotate_stale_file(&log_dir, &active_path, now, &rotated_prefix)?;

        let file = Some(Self::open_writer(&active_path)?);
        let state = WriterState {
            file,
            current_day: now.date_naive(),
            period_start: now,
        };

        Ok(Self {
            inner: Arc::new(WriterInner {
                state: Mutex::new(state),
                log_dir,
                rotated_prefix,
                active_filename,
            }),
        })
    }

    fn rotate_stale_file(
        log_dir: &Path,
        active_path: &Path,
        now: DateTime<Local>,
        prefix: &str,
    ) -> Result<()> {
        let metadata = match fs::metadata(active_path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "unable to inspect existing log file {}",
                        active_path.display()
                    )
                });
            }
        };

        let modified = metadata.modified().ok().map(DateTime::<Local>::from);
        if let Some(modified_at) = modified {
            if modified_at.date_naive() == now.date_naive() {
                return Ok(());
            }
            let rotated_path = Self::unique_rotated_path(log_dir, prefix, modified_at);
            fs::rename(active_path, &rotated_path).with_context(|| {
                format!(
                    "failed to rotate stale log {} -> {}",
                    active_path.display(),
                    rotated_path.display()
                )
            })?;
            return Ok(());
        }

        let rotated_path = Self::unique_rotated_path(log_dir, prefix, now);
        fs::rename(active_path, &rotated_path).with_context(|| {
            format!(
                "failed to rotate stale log {} -> {}",
                active_path.display(),
                rotated_path.display()
            )
        })?;
        Ok(())
    }

    fn open_writer(path: &Path) -> Result<BufWriter<fs::File>> {
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open log file {}", path.display()))?;
        Ok(BufWriter::new(file))
    }

    fn rotate(&self, state: &mut WriterState, now: DateTime<Local>) -> Result<()> {
        if let Some(mut writer) = state.file.take() {
            if let Err(err) = writer.flush() {
                eprintln!("failed to flush log file before rotation: {err}");
            }
        }

        let active_path = self.active_path();
        if active_path.exists() {
            let rotated_path = Self::unique_rotated_path(
                &self.inner.log_dir,
                &self.inner.rotated_prefix,
                state.period_start,
            );
            fs::rename(&active_path, &rotated_path).with_context(|| {
                format!(
                    "failed to rotate log {} -> {}",
                    active_path.display(),
                    rotated_path.display()
                )
            })?;
        }

        state.file = Some(Self::open_writer(&active_path)?);
        state.current_day = now.date_naive();
        state.period_start = now;

        Ok(())
    }

    fn unique_rotated_path(dir: &Path, prefix: &str, timestamp: DateTime<Local>) -> PathBuf {
        let base = format!("{}_{}", prefix, timestamp.format("%Y-%m-%d_%H-%M-%S"));
        let mut candidate = dir.join(format!("{}.log", base));
        let mut counter = 1;
        while candidate.exists() {
            candidate = dir.join(format!("{}-{}.log", base, counter));
            counter += 1;
        }
        candidate
    }

    fn active_path(&self) -> PathBuf {
        self.inner.log_dir.join(&self.inner.active_filename)
    }
}

impl Write for DailyRotatingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let now = Local::now();
        let mut state = self.inner.state.lock();

        if now.date_naive() != state.current_day {
            if let Err(err) = self.rotate(&mut state, now) {
                eprintln!("failed to rotate logs: {err:?}");
            }
        }

        if state.file.is_none() {
            state.file = Some(
                Self::open_writer(&self.active_path())
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?,
            );
            state.current_day = now.date_naive();
            state.period_start = now;
        }

        let writer = state
            .file
            .as_mut()
            .expect("log writer must be available after rotation");
        writer.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self.inner.state.lock();
        if let Some(writer) = state.file.as_mut() {
            writer.flush()
        } else {
            Ok(())
        }
    }
}

fn resolve_log_dir(plugin_name: &str, override_dir: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(dir) = override_dir {
        return absolutize(dir);
    }

    if let Ok(dir) = std::env::var(PLUGIN_LOG_DIR_ENV) {
        return absolutize(PathBuf::from(dir));
    }

    if let Ok(dir) = std::env::var(GLOBAL_LOG_DIR_ENV) {
        let base = absolutize(PathBuf::from(dir))?;
        return Ok(base.join("plugins").join(plugin_name));
    }

    let home = dirs::home_dir().context("unable to locate user home directory")?;
    Ok(home
        .join(".eventdbx")
        .join("logs")
        .join("plugins")
        .join(plugin_name))
}

fn absolutize(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        let base =
            std::env::current_dir().context("failed to resolve current working directory")?;
        Ok(base.join(path))
    }
}

fn install_panic_hook() {
    PANIC_HOOK.get_or_init(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Some(location) = info.location() {
                tracing::error!(
                    target: "panic",
                    file = location.file(),
                    line = location.line(),
                    message = %info
                );
            } else {
                tracing::error!(target: "panic", message = %info);
            }
            default_hook(info);
        }));
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Days;
    use tempfile::tempdir;

    #[test]
    fn rotates_when_forced() {
        let temp = tempdir().unwrap();
        let dir = temp.path().to_path_buf();
        let mut writer = DailyRotatingWriter::new(dir.clone(), "test_plugin").unwrap();

        writer.write_all(b"first line\n").unwrap();
        writer.flush().unwrap();

        {
            let mut state = writer.inner.state.lock();
            state.current_day = state.current_day - Days::new(1);
        }

        writer.write_all(b"second line\n").unwrap();
        writer.flush().unwrap();

        let entries: Vec<_> = fs::read_dir(dir).unwrap().collect();
        assert!(
            entries.len() >= 2,
            "expected rotated log file to exist after forcing rotation"
        );
    }
}
