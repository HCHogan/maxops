use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use clap::Parser;
use maxops_proto::{
    Facts, LogEntry, LogParams, Snapshot, UnitStatus, now,
    transport::{self, ApiError, ApiResult, Token},
    valid_host, valid_unit,
};
use serde::Deserialize;
use std::{
    collections::BTreeSet, net::SocketAddr, path::PathBuf, process::Stdio, sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command, sync::Semaphore};

#[derive(Parser)]
#[command(version, about = "Read-only Linux host agent")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    host: String,
    listen: SocketAddr,
    token_file: PathBuf,
    #[serde(default)]
    readable_units: BTreeSet<String>,
    #[serde(default)]
    allow_logs: bool,
    #[serde(default = "journalctl")]
    journalctl: PathBuf,
}
fn journalctl() -> PathBuf {
    "journalctl".into()
}

struct App {
    config: Config,
    token: Token,
    bus: zbus::Connection,
    slots: Semaphore,
}

type ListedUnit = (
    String,
    String,
    String,
    String,
    String,
    String,
    zbus::zvariant::OwnedObjectPath,
    u32,
    String,
    zbus::zvariant::OwnedObjectPath,
);

#[zbus::proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait Manager {
    fn list_units(&self) -> zbus::Result<Vec<ListedUnit>>;
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    color_eyre::eyre::ensure!(
        cfg!(target_os = "linux"),
        "maxops-agent requires Linux with systemd"
    );
    let config: Config = serde_json::from_slice(&std::fs::read(args.config)?)?;
    transport::validate_listen(config.listen)?;
    color_eyre::eyre::ensure!(valid_host(&config.host), "invalid host name");
    color_eyre::eyre::ensure!(
        config.readable_units.iter().all(|u| valid_unit(u)),
        "readable_units must contain exact service names"
    );
    let token = Token::read(&config.token_file)?;
    let listen = config.listen;
    let app = Arc::new(App {
        config,
        token,
        bus: zbus::Connection::system().await?,
        slots: Semaphore::new(8),
    });
    let router = Router::new()
        .route("/healthz", get(transport::health))
        .route("/v1/snapshot", get(snapshot))
        .route("/v1/logs", post(logs))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "agent listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(transport::shutdown())
        .await?;
    Ok(())
}

fn authorize(app: &App, headers: &HeaderMap) -> Result<(), ApiError> {
    if !app.token.matches(headers) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "unauthorized"));
    }
    Ok(())
}

async fn collect(app: &App) -> color_eyre::eyre::Result<Snapshot> {
    let manager = ManagerProxy::new(&app.bus).await?;
    let listed = manager.list_units().await?;
    let units = app
        .config
        .readable_units
        .iter()
        .map(|name| match listed.iter().find(|u| &u.0 == name) {
            Some(u) => UnitStatus {
                unit: u.0.clone(),
                description: u.1.clone(),
                load_state: u.2.clone(),
                active_state: u.3.clone(),
                sub_state: u.4.clone(),
            },
            None => UnitStatus {
                unit: name.clone(),
                description: String::new(),
                load_state: "not-loaded".into(),
                active_state: "unknown".into(),
                sub_state: "unknown".into(),
            },
        })
        .collect();
    let uptime = std::fs::read_to_string("/proc/uptime")?;
    let uptime_seconds = uptime
        .split_whitespace()
        .next()
        .ok_or_else(|| color_eyre::eyre::eyre!("missing uptime"))?
        .parse()?;
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")?
        .trim()
        .to_owned();
    let system_closure = std::fs::read_link("/run/current-system")
        .ok()
        .map(|p| p.to_string_lossy().into_owned());
    Ok(Snapshot {
        host: app.config.host.clone(),
        observed_at: now(),
        facts: Facts {
            kernel,
            uptime_seconds,
            system_closure,
        },
        units,
    })
}

async fn snapshot(State(app): State<Arc<App>>, headers: HeaderMap) -> ApiResult<Snapshot> {
    authorize(&app, &headers)?;
    let _slot = app
        .slots
        .try_acquire()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "agent busy"))?;
    match tokio::time::timeout(Duration::from_secs(5), collect(&app)).await {
        Ok(Ok(value)) => Ok(Json(value)),
        _ => Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "host observation unavailable",
        )),
    }
}

async fn read_logs(
    binary: &std::path::Path,
    params: &LogParams,
) -> color_eyre::eyre::Result<Vec<LogEntry>> {
    let mut child = Command::new(binary)
        .args([
            "--no-pager",
            "--quiet",
            "--output=json",
            "--all",
            "--unit",
            &params.unit,
            "--lines",
            &params.lines.to_string(),
            "--since",
            &format!("-{}s", params.since_seconds),
        ])
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("LANG", "C.UTF-8")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut bytes = Vec::new();
    child
        .stdout
        .take()
        .ok_or_else(|| color_eyre::eyre::eyre!("missing journal output"))?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    color_eyre::eyre::ensure!(bytes.len() <= 1024 * 1024, "journal output exceeds limit");
    color_eyre::eyre::ensure!(child.wait().await?.success(), "journal query failed");
    parse_logs(&bytes)
}

fn parse_logs(bytes: &[u8]) -> color_eyre::eyre::Result<Vec<LogEntry>> {
    std::str::from_utf8(bytes)?
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line)?;
            Ok(LogEntry {
                timestamp_us: value["__REALTIME_TIMESTAMP"].as_str().map(str::to_owned),
                priority: value["PRIORITY"].as_str().map(str::to_owned),
                message: value
                    .get("MESSAGE")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            })
        })
        .collect()
}

async fn logs(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(params): Json<LogParams>,
) -> ApiResult<serde_json::Value> {
    authorize(&app, &headers)?;
    params
        .validate()
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    if params.host != app.config.host
        || !app.config.allow_logs
        || !app.config.readable_units.contains(&params.unit)
    {
        return Err(ApiError(StatusCode::FORBIDDEN, "logs not permitted"));
    }
    let _slot = app
        .slots
        .try_acquire()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "agent busy"))?;
    match tokio::time::timeout(
        Duration::from_secs(5),
        read_logs(&app.config.journalctl, &params),
    )
    .await
    {
        Ok(Ok(entries)) => Ok(Json(
            serde_json::json!({"observed_at": now(), "host": params.host, "unit": params.unit, "entries": entries}),
        )),
        _ => Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "journal query unavailable or exceeds output limit",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn journal_output_exposes_only_selected_fields() {
        let logs = parse_logs(br#"{"__REALTIME_TIMESTAMP":"123","PRIORITY":"3","MESSAGE":"failed","SECRET_FIELD":"hidden"}
{"MESSAGE":[0,1,2]}
"#).unwrap();
        let output = serde_json::to_string(&logs).unwrap();
        assert!(!output.contains("SECRET_FIELD"));
        assert!(!output.contains("hidden"));
        assert_eq!(logs.len(), 2);
        assert!(parse_logs(b"not json").is_err());
    }
}
