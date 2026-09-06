use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use clap::Parser;
use maxops_proto::{
    ExecutorRequest, ExecutorResponse, Facts, LogEntry, LogParams, Snapshot, UnitActionParams,
    UnitDetails, UnitObservation, UnitParams, UnitStatus, now,
    transport::{self, ApiError, ApiResult, Token},
    valid_host, valid_unit,
};
use serde::Deserialize;
use std::{
    collections::BTreeSet, net::SocketAddr, path::PathBuf, process::Stdio, sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command, sync::Semaphore};

const JOURNAL_TIMEOUT: Duration = Duration::from_secs(10);

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
    execution_token_file: Option<PathBuf>,
    #[serde(default)]
    executor_socket: Option<PathBuf>,
    #[serde(default)]
    readable_units: BTreeSet<String>,
    #[serde(default)]
    manageable_units: BTreeSet<String>,
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
    execution_token: Option<Token>,
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

#[zbus::proxy(
    interface = "org.freedesktop.systemd1.Unit",
    default_service = "org.freedesktop.systemd1"
)]
trait SystemdUnit {
    #[zbus(property, name = "InvocationID")]
    fn invocation_id(&self) -> zbus::Result<Vec<u8>>;
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
    color_eyre::eyre::ensure!(
        config
            .manageable_units
            .iter()
            .all(|unit| { valid_unit(unit) && config.readable_units.contains(unit) }),
        "manageable_units must be valid readable service names"
    );
    let token = Token::read(&config.token_file)?;
    color_eyre::eyre::ensure!(
        config.execution_token_file.is_some() == config.executor_socket.is_some(),
        "execution_token_file and executor_socket must be configured together"
    );
    let execution_token = config
        .execution_token_file
        .as_deref()
        .map(Token::read)
        .transpose()?;
    if let Some(execution_token) = &execution_token {
        color_eyre::eyre::ensure!(
            !execution_token.same_as(&token),
            "observation and execution credentials must differ"
        );
    }
    let listen = config.listen;
    let app = Arc::new(App {
        config,
        token,
        execution_token,
        bus: zbus::Connection::system().await?,
        slots: Semaphore::new(8),
    });
    let router = Router::new()
        .route("/healthz", get(transport::health))
        .route("/v1/snapshot", get(snapshot))
        .route("/v1/unit", post(unit_status))
        .route("/v1/logs", post(logs))
        .route(
            "/v1/manage",
            post(manage).layer(DefaultBodyLimit::max(maxops_proto::transport::MAX_BODY)),
        )
        .layer(DefaultBodyLimit::max(128 * 1024))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "agent listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(transport::shutdown())
        .await?;
    Ok(())
}

async fn manage(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<ExecutorRequest>,
) -> ApiResult<ExecutorResponse> {
    let token = app.execution_token.as_ref().ok_or(ApiError(
        StatusCode::NOT_FOUND,
        "management endpoint disabled",
    ))?;
    if !token.matches(&headers) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "unauthorized"));
    }
    if let ExecutorRequest::Submit { job, .. } = &request {
        if job.host != app.config.host {
            return Err(ApiError(StatusCode::FORBIDDEN, "job targets another host"));
        }
        if job.operation.starts_with("units.") {
            let params: UnitActionParams = serde_json::from_value(job.spec.clone())
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid service action"))?;
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            if params.host != app.config.host || !app.config.manageable_units.contains(&params.unit)
            {
                return Err(ApiError(StatusCode::FORBIDDEN, "service is not manageable"));
            }
        }
    }
    let socket = app
        .config
        .executor_socket
        .as_ref()
        .expect("validated socket");
    match tokio::time::timeout(
        Duration::from_secs(10),
        transport::executor_request(socket, &request),
    )
    .await
    {
        Ok(Ok(response)) => Ok(Json(response)),
        Ok(Err(error)) => {
            tracing::warn!(%error, "executor rejected management request");
            if let Some(rejected) = error.downcast_ref::<transport::ExecutorRejected>() {
                return Err(match rejected.code.as_str() {
                    "workspace_revision_conflict" => {
                        ApiError(StatusCode::CONFLICT, "workspace revision changed")
                    }
                    "workspace_not_found" => ApiError(StatusCode::NOT_FOUND, "workspace not found"),
                    "invalid_workspace_path" => {
                        ApiError(StatusCode::UNPROCESSABLE_ENTITY, "invalid workspace path")
                    }
                    _ => ApiError(StatusCode::SERVICE_UNAVAILABLE, "executor unavailable"),
                });
            }
            Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "executor unavailable",
            ))
        }
        Err(_) => {
            tracing::warn!("executor management request timed out");
            Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "executor unavailable",
            ))
        }
    }
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
                details: None,
            },
            None => UnitStatus {
                unit: name.clone(),
                description: String::new(),
                load_state: "not-loaded".into(),
                active_state: "unknown".into(),
                sub_state: "unknown".into(),
                details: None,
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
    let profile_link = std::fs::read_link("/nix/var/nix/profiles/system").ok();
    let system_profile = std::fs::canonicalize("/nix/var/nix/profiles/system")
        .ok()
        .map(|path| path.to_string_lossy().into_owned());
    let profile_generation = profile_link.as_deref().and_then(generation);
    let profile_matches_running = system_closure
        .as_ref()
        .zip(system_profile.as_ref())
        .map(|(running, profile)| running == profile);
    Ok(Snapshot {
        host: app.config.host.clone(),
        observed_at: now(),
        facts: Facts {
            kernel,
            uptime_seconds,
            system_closure,
            system_profile,
            profile_generation,
            profile_matches_running,
        },
        units,
    })
}

fn generation(path: &std::path::Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_prefix("system-")?
        .strip_suffix("-link")?
        .parse()
        .ok()
}

async fn unit_status(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(params): Json<UnitParams>,
) -> ApiResult<UnitObservation> {
    authorize(&app, &headers)?;
    if params.host != app.config.host
        || !valid_unit(&params.unit)
        || !app.config.readable_units.contains(&params.unit)
    {
        return Err(ApiError(StatusCode::FORBIDDEN, "service not permitted"));
    }
    let _slot = app
        .slots
        .try_acquire()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "agent busy"))?;
    match tokio::time::timeout(Duration::from_secs(5), collect_unit(&app, &params.unit)).await {
        Ok(Ok(unit)) => Ok(Json(UnitObservation {
            host: app.config.host.clone(),
            observed_at: now(),
            unit,
        })),
        _ => Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "service observation unavailable",
        )),
    }
}

async fn collect_unit(app: &App, name: &str) -> color_eyre::eyre::Result<UnitStatus> {
    let listed = ManagerProxy::new(&app.bus).await?.list_units().await?;
    let Some(unit) = listed.iter().find(|unit| unit.0 == name) else {
        return Ok(UnitStatus {
            unit: name.into(),
            description: String::new(),
            load_state: "not-loaded".into(),
            active_state: "unknown".into(),
            sub_state: "unknown".into(),
            details: None,
        });
    };
    let proxy = zbus::fdo::PropertiesProxy::builder(&app.bus)
        .destination("org.freedesktop.systemd1")?
        .path(unit.6.clone())?
        .build()
        .await?;
    let properties = proxy
        .get_all("org.freedesktop.systemd1.Service".try_into()?)
        .await?;
    let unit_proxy = SystemdUnitProxy::builder(&app.bus)
        .path(unit.6.clone())?
        .build()
        .await?;
    let invocation = unit_proxy.invocation_id().await?;
    let details = UnitDetails {
        main_pid: properties
            .get("MainPID")
            .and_then(|value| u32::try_from(value).ok()),
        memory_current_bytes: properties
            .get("MemoryCurrent")
            .and_then(|value| u64::try_from(value).ok())
            .filter(|value| *value != u64::MAX),
        restarts: properties
            .get("NRestarts")
            .and_then(|value| u32::try_from(value).ok()),
        exec_main_code: properties
            .get("ExecMainCode")
            .and_then(|value| i32::try_from(value).ok()),
        exec_main_status: properties
            .get("ExecMainStatus")
            .and_then(|value| i32::try_from(value).ok()),
        invocation_id: (!invocation.is_empty()).then(|| bytes_to_hex(&invocation)),
    };
    Ok(UnitStatus {
        unit: unit.0.clone(),
        description: unit.1.clone(),
        load_state: unit.2.clone(),
        active_state: unit.3.clone(),
        sub_state: unit.4.clone(),
        details: Some(details),
    })
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0xf) as usize] as char);
    }
    output
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
    match tokio::time::timeout(JOURNAL_TIMEOUT, read_logs(&app.config.journalctl, &params)).await {
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
    fn journal_deadline_leaves_time_for_http_response() {
        assert!(JOURNAL_TIMEOUT + Duration::from_secs(2) <= transport::REQUEST_TIMEOUT);
    }

    #[test]
    fn generation_is_only_a_profile_link_number() {
        assert_eq!(generation(std::path::Path::new("system-42-link")), Some(42));
        assert_eq!(
            generation(std::path::Path::new("/nix/store/arbitrary-system")),
            None
        );
        assert_eq!(
            generation(std::path::Path::new("system-invalid-link")),
            None
        );
    }
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
