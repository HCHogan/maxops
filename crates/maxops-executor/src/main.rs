use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use clap::Parser;
use color_eyre::eyre::{Context, Result, ensure, eyre};
use maxops_executor::{RunnerResult, RunnerSpec};
use maxops_proto::{
    ExecRunParams, ExecutorRequest, ExecutorResponse, ExecutorWireResponse, JobId, JobLogsResponse,
    JobRecord, JobState, UnitActionParams,
};
use maxops_store::Store;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom},
    net::{UnixListener, UnixStream},
    process::Command,
};

mod workspace;

const MAX_REQUEST_BYTES: u64 = 2 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const RECONCILE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Parser)]
#[command(version, about = "Durable local maxops execution service")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    host: String,
    socket_path: PathBuf,
    state_file: PathBuf,
    spec_directory: PathBuf,
    spool_root: PathBuf,
    systemd_run: PathBuf,
    systemctl: PathBuf,
    runner: PathBuf,
    git: PathBuf,
    workspace_root: PathBuf,
    repository_root: PathBuf,
    #[serde(default)]
    manageable_units: BTreeSet<String>,
    #[serde(default)]
    credential_sources: BTreeMap<String, PathBuf>,
    profiles: BTreeMap<String, Profile>,
    #[serde(default)]
    repositories: BTreeMap<String, RepositoryConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepositoryConfig {
    url: String,
    default_ref: String,
    #[serde(default)]
    publish_refs: BTreeSet<String>,
    check_profile: String,
    #[serde(default)]
    checks: BTreeMap<String, Vec<String>>,
    author_name: String,
    author_email: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    user: String,
    #[serde(default = "default_interpreter")]
    interpreter: PathBuf,
    #[serde(default = "default_timeout")]
    timeout_seconds: u32,
    #[serde(default = "default_output_limit")]
    output_limit_bytes: u64,
    #[serde(default)]
    working_roots: Vec<PathBuf>,
    #[serde(default)]
    environment: BTreeMap<String, String>,
    #[serde(default)]
    allowed_credentials: Vec<String>,
    #[serde(default)]
    privileged: bool,
    #[serde(default = "default_tasks_max")]
    tasks_max: u32,
    #[serde(default)]
    memory_max_bytes: Option<u64>,
}

fn default_interpreter() -> PathBuf {
    "/bin/sh".into()
}
fn default_timeout() -> u32 {
    300
}
fn default_output_limit() -> u64 {
    16 * 1024 * 1024
}
fn default_tasks_max() -> u32 {
    256
}

struct App {
    config: Config,
    store: Store,
    bus: zbus::Connection,
    service_workers: Mutex<HashSet<JobId>>,
    workspace_workers: Mutex<HashSet<JobId>>,
    workspace_serial: tokio::sync::Mutex<()>,
}

#[zbus::proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait SystemdManager {
    fn get_unit(&self, name: &str) -> zbus::Result<zbus::zvariant::OwnedObjectPath>;
    fn get_job(&self, id: u32) -> zbus::Result<zbus::zvariant::OwnedObjectPath>;
    fn start_unit(&self, name: &str, mode: &str) -> zbus::Result<zbus::zvariant::OwnedObjectPath>;
    fn stop_unit(&self, name: &str, mode: &str) -> zbus::Result<zbus::zvariant::OwnedObjectPath>;
    fn restart_unit(&self, name: &str, mode: &str)
    -> zbus::Result<zbus::zvariant::OwnedObjectPath>;
    fn reload_unit(&self, name: &str, mode: &str) -> zbus::Result<zbus::zvariant::OwnedObjectPath>;
}

#[zbus::proxy(
    interface = "org.freedesktop.systemd1.Unit",
    default_service = "org.freedesktop.systemd1"
)]
trait SystemdUnit {
    #[zbus(property)]
    fn active_state(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn sub_state(&self) -> zbus::Result<String>;
    #[zbus(property, name = "InvocationID")]
    fn invocation_id(&self) -> zbus::Result<Vec<u8>>;
}

#[zbus::proxy(
    interface = "org.freedesktop.systemd1.Service",
    default_service = "org.freedesktop.systemd1"
)]
trait SystemdService {
    #[zbus(property)]
    fn result(&self) -> zbus::Result<String>;
    #[zbus(property, name = "ReloadResult")]
    fn reload_result(&self) -> zbus::Result<String>;
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ServiceAction {
    Start,
    Stop,
    Restart,
    Reload,
}

impl ServiceAction {
    fn from_operation(operation: &str) -> Option<Self> {
        match operation {
            "units.start" => Some(Self::Start),
            "units.stop" => Some(Self::Stop),
            "units.restart" => Some(Self::Restart),
            "units.reload" => Some(Self::Reload),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ServiceUnitState {
    active_state: String,
    sub_state: String,
    invocation_id: Option<String>,
    service_result: Option<String>,
    reload_result: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ServiceProgress {
    action: ServiceAction,
    unit: String,
    before: ServiceUnitState,
    #[serde(default)]
    manager_job: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    ensure!(
        cfg!(target_os = "linux"),
        "maxops-executor requires Linux with systemd"
    );
    let config: Config = serde_json::from_slice(&std::fs::read(Args::parse().config)?)?;
    validate_config(&config)?;
    prepare_socket(&config.socket_path)?;
    std::fs::create_dir_all(&config.spec_directory)?;
    std::fs::create_dir_all(&config.workspace_root)?;
    std::fs::create_dir_all(&config.repository_root)?;
    let listener = UnixListener::bind(&config.socket_path)?;
    std::fs::set_permissions(&config.socket_path, std::fs::Permissions::from_mode(0o660))?;
    let store = Store::open(&config.state_file).await?;
    let bus = zbus::Connection::system().await?;
    let app = Arc::new(App {
        config,
        store,
        bus,
        service_workers: Mutex::new(HashSet::new()),
        workspace_workers: Mutex::new(HashSet::new()),
        workspace_serial: tokio::sync::Mutex::new(()),
    });
    for job in app.store.nonterminal_jobs().await? {
        recover_job(app.clone(), job);
    }
    tracing::info!(socket = %app.config.socket_path.display(), "executor listening");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let app = app.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_connection(app, stream).await {
                        tracing::warn!(%error, "executor request failed");
                    }
                });
            }
            _ = maxops_proto::transport::shutdown() => break,
        }
    }
    Ok(())
}

fn validate_config(config: &Config) -> Result<()> {
    ensure!(
        maxops_proto::valid_host(&config.host),
        "invalid executor host"
    );
    for path in [
        &config.socket_path,
        &config.state_file,
        &config.spec_directory,
        &config.spool_root,
        &config.systemd_run,
        &config.systemctl,
        &config.runner,
        &config.git,
        &config.workspace_root,
        &config.repository_root,
    ] {
        ensure!(path.is_absolute(), "executor paths must be absolute");
    }
    ensure!(
        !config.profiles.is_empty(),
        "at least one execution profile is required"
    );
    ensure!(
        config.workspace_root != config.repository_root
            && !config.workspace_root.starts_with(&config.repository_root)
            && !config.repository_root.starts_with(&config.workspace_root),
        "workspace and repository roots must be separate"
    );
    ensure!(
        config.spool_root == Path::new("/var/lib/maxops-jobs"),
        "spool_root must match systemd StateDirectory root /var/lib/maxops-jobs"
    );
    ensure!(
        config
            .manageable_units
            .iter()
            .all(|unit| maxops_proto::valid_unit(unit)),
        "manageable_units must contain exact service names"
    );
    for (name, path) in &config.credential_sources {
        ensure!(
            valid_credential_name(name),
            "invalid credential source name"
        );
        ensure!(
            path.is_absolute(),
            "credential source paths must be absolute"
        );
        ensure!(
            !path.to_string_lossy().contains(['\n', '\r', ':']),
            "credential source paths contain unsupported characters"
        );
    }
    for (name, profile) in &config.profiles {
        ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
            "invalid profile name"
        );
        ensure!(
            !profile.user.is_empty()
                && profile.user.len() <= 64
                && profile
                    .user
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
            "invalid profile user"
        );
        ensure!(
            profile.interpreter.is_absolute(),
            "profile interpreter must be absolute"
        );
        ensure!(
            profile.timeout_seconds > 0,
            "profile timeout must be positive"
        );
        ensure!(
            profile.output_limit_bytes > 0,
            "profile output limit must be positive"
        );
        ensure!(profile.tasks_max > 0, "profile tasks_max must be positive");
        ensure!(
            profile.allowed_credentials.iter().all(|name| {
                valid_credential_name(name) && config.credential_sources.contains_key(name)
            }),
            "profile references an unknown credential source"
        );
        ensure!(
            profile.working_roots.iter().all(|path| path.is_absolute()),
            "working roots must be absolute"
        );
    }
    for (name, repository) in &config.repositories {
        ensure!(
            maxops_proto::valid_repository_id(name),
            "invalid repository ID"
        );
        ensure!(
            !repository.url.is_empty()
                && repository.url.len() <= 4096
                && !repository.url.contains(['\0', '\n', '\r']),
            "invalid repository URL"
        );
        ensure!(
            valid_git_ref(&repository.default_ref)
                && repository.publish_refs.iter().all(|reference| {
                    valid_git_ref(reference) && reference.starts_with("refs/heads/")
                }),
            "invalid repository ref"
        );
        ensure!(
            config.profiles.contains_key(&repository.check_profile),
            "repository references an unknown check profile"
        );
        ensure!(
            !repository.author_name.trim().is_empty()
                && !repository.author_email.trim().is_empty()
                && !repository.author_name.contains(['\0', '\n', '\r'])
                && !repository.author_email.contains(['\0', '\n', '\r']),
            "invalid configured Git author"
        );
        ensure!(
            repository.checks.iter().all(|(check, argv)| {
                maxops_proto::valid_check_id(check)
                    && !argv.is_empty()
                    && argv.len() <= 256
                    && argv
                        .iter()
                        .all(|argument| argument.len() <= 16 * 1024 && !argument.contains('\0'))
            }),
            "invalid configured repository check"
        );
    }
    Ok(())
}

fn valid_git_ref(reference: &str) -> bool {
    reference.starts_with("refs/")
        && reference.len() <= 512
        && !reference.contains("..")
        && !reference.contains("@{")
        && !reference.ends_with(['/', '.'])
        && !reference
            .bytes()
            .any(|byte| byte <= b' ' || b"~^:?*[\\".contains(&byte))
}

fn valid_credential_name(value: &str) -> bool {
    !value.is_empty()
        && value != "spec"
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn prepare_socket(path: &Path) -> Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        ensure!(
            metadata.file_type().is_socket(),
            "executor socket path exists and is not a socket"
        );
        std::fs::remove_file(path)?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

async fn serve_connection(app: Arc<App>, stream: UnixStream) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut bytes = Vec::new();
    BufReader::new(reader)
        .take(MAX_REQUEST_BYTES + 1)
        .read_until(b'\n', &mut bytes)
        .await?;
    ensure!(
        bytes.len() as u64 <= MAX_REQUEST_BYTES,
        "executor request too large"
    );
    let request: ExecutorRequest = serde_json::from_slice(&bytes)?;
    let response = match handle(app, request).await {
        Ok(response) => ExecutorWireResponse::Ok {
            response: Box::new(response),
        },
        Err(error) => {
            let message = format!("{error:#}");
            ExecutorWireResponse::Error {
                code: executor_error_code(&message).into(),
                message,
            }
        }
    };
    let mut response = serde_json::to_vec(&response)?;
    ensure!(
        response.len() <= MAX_RESPONSE_BYTES,
        "executor response too large"
    );
    response.push(b'\n');
    writer.write_all(&response).await?;
    writer.shutdown().await?;
    Ok(())
}

fn executor_error_code(message: &str) -> &'static str {
    if message.contains("workspace revision changed") {
        "workspace_revision_conflict"
    } else if message.contains("workspace not found") {
        "workspace_not_found"
    } else if message.contains("workspace path")
        || message.contains("workspace target")
        || message.contains("workspace ancestor")
        || message.contains("workspace file exceeds")
    {
        "invalid_workspace_path"
    } else {
        "executor_request_failed"
    }
}

async fn handle(app: Arc<App>, request: ExecutorRequest) -> Result<ExecutorResponse> {
    match request {
        ExecutorRequest::Submit { job_id, job } => submit_job(app, job_id, job).await,
        ExecutorRequest::Status(params) => {
            let job = app.store.get_job(&params.job_id).await?;
            if is_runner_job(&job.handle.operation) {
                reconcile_once(&app, &params.job_id).await?;
            } else if ServiceAction::from_operation(&job.handle.operation).is_some()
                && !job.handle.state.is_terminal()
            {
                spawn_service_job(app.clone(), job.handle.job_id.clone());
            } else if workspace::is_workspace_job(&job.handle.operation)
                && !job.handle.state.is_terminal()
            {
                workspace::spawn(app.clone(), job.handle.job_id.clone());
            }
            Ok(ExecutorResponse::Job(
                app.store.get_job(&params.job_id).await?,
            ))
        }
        ExecutorRequest::Logs(params) => {
            let job = app.store.get_job(&params.job_id).await?;
            let response = read_logs(&app.config, &job, &params).await?;
            Ok(ExecutorResponse::Logs(response))
        }
        ExecutorRequest::Cancel(params) => {
            let existing = app.store.get_job(&params.job_id).await?;
            if ServiceAction::from_operation(&existing.handle.operation).is_some() {
                return cancel_service_job(&app, existing, params).await;
            }
            if workspace::is_workspace_job(&existing.handle.operation)
                && existing.handle.operation != "workspace.check"
            {
                return workspace::cancel(&app, params).await;
            }
            let requested = app
                .store
                .request_cancel(&params.job_id, params.expected_revision, &params.reason)
                .await?;
            let status = Command::new(&app.config.systemctl)
                .args(["stop", &unit_name(&params.job_id)])
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
            ensure!(status.success(), "systemd did not accept cancellation");
            let cancelled = app
                .store
                .transition_job(
                    &params.job_id,
                    requested.handle.revision,
                    JobState::Cancelled,
                    &json!({"unit":unit_name(&params.job_id)}),
                    Some(&json!({"cancelled":true})),
                )
                .await?;
            Ok(ExecutorResponse::Job(cancelled))
        }
        ExecutorRequest::Workspace { principal, request } => Ok(ExecutorResponse::Workspace(
            workspace::handle(&app, &principal, request).await?,
        )),
    }
}

async fn submit_job(
    app: Arc<App>,
    job_id: JobId,
    job: maxops_proto::NewJob,
) -> Result<ExecutorResponse> {
    ensure!(job.host == app.config.host, "job targets another host");
    let service_action = ServiceAction::from_operation(&job.operation);
    ensure!(
        job.operation == "exec.run"
            || service_action.is_some()
            || workspace::is_workspace_job(&job.operation),
        "unsupported executor operation"
    );
    let accepted = app.store.accept_job(&job_id, &job).await?;
    if service_action.is_some() {
        if accepted.created {
            let params: UnitActionParams = match serde_json::from_value(job.spec.clone())
                .map_err(color_eyre::Report::from)
                .and_then(|params: UnitActionParams| {
                    params.validate().map_err(|message| eyre!(message))?;
                    ensure!(
                        params.host == app.config.host,
                        "service action targets another host"
                    );
                    ensure!(
                        app.config.manageable_units.contains(&params.unit),
                        "service is not manageable"
                    );
                    Ok(params)
                }) {
                Ok(params) => params,
                Err(error) => {
                    tracing::warn!(job_id = %job_id, %error, "service job validation failed");
                    let failed = app.store.transition_job(
                        &job_id,
                        accepted.job.handle.revision,
                        JobState::Failed,
                        &json!({"phase":"validation"}),
                        Some(&json!({"phase":"validation","error":"service action is not permitted by the target"})),
                    ).await?;
                    return Ok(ExecutorResponse::Job(failed));
                }
            };
            drop(params);
        }
        if !accepted.job.handle.state.is_terminal() {
            spawn_service_job(app, job_id);
        }
        return Ok(ExecutorResponse::Job(accepted.job));
    }
    if workspace::is_workspace_job(&job.operation) {
        if accepted.created
            && let Err(error) = workspace::validate_job(&app, &job).await
        {
            tracing::warn!(job_id = %job_id, %error, "workspace job validation failed");
            let failed = app
                .store
                .transition_job(
                    &job_id,
                    accepted.job.handle.revision,
                    JobState::Failed,
                    &json!({"phase":"validation"}),
                    Some(&json!({"error":"workspace job is not permitted by the target"})),
                )
                .await?;
            return Ok(ExecutorResponse::Job(failed));
        }
        if job.operation == "workspace.check" {
            return submit_command_job(app, job_id, job, accepted).await;
        }
        if !accepted.job.handle.state.is_terminal() {
            workspace::spawn(app, job_id);
        }
        return Ok(ExecutorResponse::Job(accepted.job));
    }
    submit_command_job(app, job_id, job, accepted).await
}

async fn submit_command_job(
    app: Arc<App>,
    job_id: JobId,
    job: maxops_proto::NewJob,
    accepted: maxops_store::SubmitResult,
) -> Result<ExecutorResponse> {
    if accepted.created {
        let prepared = async {
            let params = runner_params(&app, &job.principal, &job.operation, &job.spec).await?;
            let prepared = prepare_runner_spec(&app.config, &params)?;
            Result::<_>::Ok((params, prepared))
        }
        .await;
        let (params, prepared) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                tracing::warn!(job_id = %job_id, %error, "job validation failed");
                let failed = app.store.transition_job(
                    &job_id,
                    accepted.job.handle.revision,
                    JobState::Failed,
                    &json!({"phase":"validation"}),
                    Some(&json!({"phase":"validation","error":"job is not permitted by the target profile"})),
                ).await?;
                return Ok(ExecutorResponse::Job(failed));
            }
        };
        if let Err(error) = persist_runner_spec(&app.config, &job_id, &prepared).await {
            tracing::warn!(job_id = %job_id, %error, "persisting job specification failed");
            let failed = app.store.transition_job(
                &job_id,
                accepted.job.handle.revision,
                JobState::Failed,
                &json!({"phase":"prepare"}),
                Some(&json!({"phase":"prepare","error":"target could not persist the job specification"})),
            ).await?;
            return Ok(ExecutorResponse::Job(failed));
        }
        let dispatching = app
            .store
            .transition_job(
                &job_id,
                accepted.job.handle.revision,
                JobState::Dispatching,
                &json!({"launcher":"systemd"}),
                None,
            )
            .await?;
        if launch(&app.config, &job_id, &params, &prepared, job.deadline)
            .await
            .is_err()
        {
            let failed = app
                .store
                .transition_job(
                    &job_id,
                    dispatching.handle.revision,
                    JobState::Failed,
                    &json!({"phase":"launch"}),
                    Some(&json!({"phase":"launch","error":"executor launch failed"})),
                )
                .await?;
            return Ok(ExecutorResponse::Job(failed));
        }
        let running = app
            .store
            .transition_job(
                &job_id,
                dispatching.handle.revision,
                JobState::Running,
                &json!({"unit":unit_name(&job_id)}),
                None,
            )
            .await?;
        monitor(app, job_id);
        Ok(ExecutorResponse::Job(running))
    } else {
        recover_job(app, accepted.job.clone());
        Ok(ExecutorResponse::Job(accepted.job))
    }
}

fn is_runner_job(operation: &str) -> bool {
    matches!(operation, "exec.run" | "workspace.check")
}

async fn runner_params(
    app: &App,
    principal: &str,
    operation: &str,
    spec: &serde_json::Value,
) -> Result<ExecRunParams> {
    if operation == "workspace.check" {
        return workspace::check_exec_params(app, principal, spec).await;
    }
    ensure!(operation == "exec.run", "unsupported runner job");
    let params: ExecRunParams = serde_json::from_value(spec.clone())?;
    params.validate().map_err(|message| eyre!(message))?;
    ensure!(
        params.host == app.config.host,
        "command targets another host"
    );
    Ok(params)
}

async fn cancel_service_job(
    app: &App,
    _existing: JobRecord,
    params: maxops_proto::JobCancelParams,
) -> Result<ExecutorResponse> {
    let requested = app
        .store
        .request_cancel(&params.job_id, params.expected_revision, &params.reason)
        .await?;
    if matches!(
        requested.handle.state,
        JobState::Queued | JobState::Dispatching
    ) {
        let cancelled = app
            .store
            .transition_job(
                &params.job_id,
                requested.handle.revision,
                JobState::Cancelled,
                &json!({"phase":"before_systemd_action"}),
                Some(&json!({"cancelled":true,"effect":"not_started"})),
            )
            .await?;
        let _ = app
            .store
            .release_resource("systemd_manager", &app.config.host, &params.job_id)
            .await;
        return Ok(ExecutorResponse::Job(cancelled));
    }
    Ok(ExecutorResponse::Job(requested))
}

fn spawn_service_job(app: Arc<App>, id: JobId) {
    let inserted = app
        .service_workers
        .lock()
        .expect("service worker lock poisoned")
        .insert(id.clone());
    if !inserted {
        return;
    }
    tokio::spawn(async move {
        loop {
            let Err(error) = run_service_job(&app, &id).await else {
                break;
            };
            tracing::warn!(job_id = %id, %error, "service job stopped");
            match record_service_worker_error(&app, &id).await {
                Ok(()) => break,
                Err(record_error) => {
                    tracing::error!(
                        job_id = %id,
                        %record_error,
                        "could not durably record service worker failure; retaining resource lock"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        app.service_workers
            .lock()
            .expect("service worker lock poisoned")
            .remove(&id);
    });
}

async fn record_service_worker_error(app: &App, id: &JobId) -> Result<()> {
    let mut job = app.store.get_job(id).await?;
    if job.handle.state.is_terminal() {
        let _ = app
            .store
            .release_resource("systemd_manager", &app.config.host, id)
            .await?;
        return Ok(());
    }

    if job.handle.state == JobState::Running {
        let progress = job.result.clone();
        job = app
            .store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Reconciling,
                &json!({"phase":"executor_error","effect":"may_have_started"}),
                progress.as_ref(),
            )
            .await?;
    }

    let (state, effect) = if job.handle.state == JobState::Reconciling {
        (JobState::OutcomeUnknown, "unknown")
    } else {
        (JobState::Failed, "not_started")
    };
    let previous = job.result.clone();
    app.store
        .transition_job(
            id,
            job.handle.revision,
            state,
            &json!({"phase":"executor_error","effect":effect}),
            Some(&json!({
                "error":"service_worker_failed",
                "effect":effect,
                "previous":previous,
            })),
        )
        .await?;
    app.store
        .release_resource("systemd_manager", &app.config.host, id)
        .await?;
    Ok(())
}

async fn run_service_job(app: &App, id: &JobId) -> Result<()> {
    let mut job = app.store.get_job(id).await?;
    let action = ServiceAction::from_operation(&job.handle.operation)
        .ok_or_else(|| eyre!("unsupported service operation"))?;
    let params: UnitActionParams = serde_json::from_value(job.spec.clone())?;
    params.validate().map_err(|message| eyre!(message))?;
    ensure!(
        app.config.manageable_units.contains(&params.unit),
        "service is no longer manageable"
    );
    while !app
        .store
        .try_acquire_resource("systemd_manager", &app.config.host, id)
        .await?
    {
        job = app.store.get_job(id).await?;
        if job.handle.state.is_terminal() {
            return Ok(());
        }
        if matches!(job.handle.state, JobState::Queued | JobState::Dispatching)
            && job
                .deadline
                .is_some_and(|deadline| deadline <= maxops_proto::now())
        {
            app.store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::TimedOut,
                    &json!({"phase":"host_queue"}),
                    Some(&json!({"error":"deadline_expired_before_systemd_action"})),
                )
                .await?;
            return Ok(());
        }
        if job.cancel_requested {
            let cancelled = app
                .store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::Cancelled,
                    &json!({"phase":"resource_queue"}),
                    Some(&json!({"cancelled":true,"effect":"not_started"})),
                )
                .await?;
            tracing::info!(job_id = %id, revision = cancelled.handle.revision, "queued service job cancelled");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    job = app.store.get_job(id).await?;
    if job.handle.state.is_terminal() {
        let _ = app
            .store
            .release_resource("systemd_manager", &app.config.host, id)
            .await?;
        return Ok(());
    }
    if matches!(job.handle.state, JobState::Queued | JobState::Dispatching)
        && job
            .deadline
            .is_some_and(|deadline| deadline <= maxops_proto::now())
    {
        app.store
            .transition_job(
                id,
                job.handle.revision,
                JobState::TimedOut,
                &json!({"phase":"before_systemd_action"}),
                Some(&json!({"error":"deadline_expired_before_systemd_action"})),
            )
            .await?;
        app.store
            .release_resource("systemd_manager", &app.config.host, id)
            .await?;
        return Ok(());
    }
    if job.cancel_requested && matches!(job.handle.state, JobState::Queued | JobState::Dispatching)
    {
        let cancelled = app
            .store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Cancelled,
                &json!({"phase":"before_systemd_action"}),
                Some(&json!({"cancelled":true,"effect":"not_started"})),
            )
            .await?;
        tracing::info!(job_id = %id, revision = cancelled.handle.revision, "service job cancelled");
        app.store
            .release_resource("systemd_manager", &app.config.host, id)
            .await?;
        return Ok(());
    }

    match job.handle.state {
        JobState::Queued => {
            let before = observe_service_unit(app, &params.unit).await?;
            if params
                .expected_invocation_id
                .as_deref()
                .is_some_and(|expected| {
                    before
                        .invocation_id
                        .as_deref()
                        .is_none_or(|observed| !expected.eq_ignore_ascii_case(observed))
                })
            {
                app.store
                    .transition_job(
                        id,
                        job.handle.revision,
                        JobState::Failed,
                        &json!({"phase":"baseline","reason":"stale_baseline"}),
                        Some(&json!({
                            "error":"stale_baseline",
                            "expected_invocation_id":params.expected_invocation_id,
                            "observed":before,
                        })),
                    )
                    .await?;
                app.store
                    .release_resource("systemd_manager", &app.config.host, id)
                    .await?;
                return Ok(());
            }
            let progress = ServiceProgress {
                action,
                unit: params.unit.clone(),
                before,
                manager_job: None,
            };
            job = app
                .store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::Dispatching,
                    &json!({"resource":"systemd_manager","host":app.config.host,"unit":params.unit}),
                    Some(&serde_json::to_value(&progress)?),
                )
                .await?;
            start_service_action(app, id, job, progress).await?;
        }
        JobState::Dispatching => {
            let progress: ServiceProgress = serde_json::from_value(
                job.result
                    .clone()
                    .ok_or_else(|| eyre!("service baseline is missing"))?,
            )?;
            start_service_action(app, id, job, progress).await?;
        }
        JobState::Running => {
            recover_uncertain_service_action(app, id, job).await?;
        }
        JobState::Reconciling => {
            let progress: ServiceProgress = serde_json::from_value(
                job.result
                    .clone()
                    .ok_or_else(|| eyre!("service progress is missing"))?,
            )?;
            finish_accepted_service_action(app, id, job, progress).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn start_service_action(
    app: &App,
    id: &JobId,
    job: JobRecord,
    mut progress: ServiceProgress,
) -> Result<()> {
    let running = app
        .store
        .transition_job(
            id,
            job.handle.revision,
            JobState::Running,
            &json!({"phase":"systemd_call_may_have_started"}),
            Some(&serde_json::to_value(&progress)?),
        )
        .await?;
    let manager = SystemdManagerProxy::new(&app.bus).await?;
    let called = match progress.action {
        ServiceAction::Start => manager.start_unit(&progress.unit, "replace").await,
        ServiceAction::Stop => manager.stop_unit(&progress.unit, "replace").await,
        ServiceAction::Restart => manager.restart_unit(&progress.unit, "replace").await,
        ServiceAction::Reload => manager.reload_unit(&progress.unit, "replace").await,
    };
    let manager_job = match called {
        Ok(path) => path.to_string(),
        Err(error) if explicit_systemd_rejection(&error) => {
            app.store
                .transition_job(
                    id,
                    running.handle.revision,
                    JobState::Failed,
                    &json!({"phase":"systemd_call","rejected":true}),
                    Some(&json!({
                        "action":progress.action,
                        "unit":progress.unit,
                        "before":progress.before,
                        "error":"systemd_rejected_action",
                    })),
                )
                .await?;
            app.store
                .release_resource("systemd_manager", &app.config.host, id)
                .await?;
            return Ok(());
        }
        Err(error) => {
            tracing::warn!(job_id = %id, %error, "systemd action acknowledgement unavailable");
            let reconciling = app
                .store
                .transition_job(
                    id,
                    running.handle.revision,
                    JobState::Reconciling,
                    &json!({"phase":"systemd_call","acknowledgement":"unavailable"}),
                    Some(&serde_json::to_value(&progress)?),
                )
                .await?;
            transition_service_unknown(
                app,
                id,
                reconciling,
                &progress,
                "systemd_acknowledgement_unavailable",
            )
            .await?;
            return Ok(());
        }
    };
    progress.manager_job = Some(manager_job);
    let reconciling = app
        .store
        .transition_job(
            id,
            running.handle.revision,
            JobState::Reconciling,
            &json!({"phase":"systemd_job_accepted"}),
            Some(&serde_json::to_value(&progress)?),
        )
        .await?;
    finish_accepted_service_action(app, id, reconciling, progress).await
}

async fn finish_accepted_service_action(
    app: &App,
    id: &JobId,
    job: JobRecord,
    progress: ServiceProgress,
) -> Result<()> {
    let Some(manager_job) = progress.manager_job.as_deref() else {
        return transition_service_unknown(app, id, job, &progress, "manager_job_missing").await;
    };
    let Some(job_number) = manager_job
        .rsplit('/')
        .next()
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return transition_service_unknown(app, id, job, &progress, "invalid_manager_job_path")
            .await;
    };
    let manager = SystemdManagerProxy::new(&app.bus).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() >= deadline {
            return transition_service_unknown(app, id, job, &progress, "systemd_job_timeout")
                .await;
        }
        match manager.get_job(job_number).await {
            Ok(_) => {}
            Err(error) if systemd_job_finished(&error) => break,
            Err(error) => {
                tracing::warn!(job_id = %id, %error, "systemd job observation unavailable");
                return transition_service_unknown(
                    app,
                    id,
                    job,
                    &progress,
                    "systemd_job_observation_unavailable",
                )
                .await;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Keep maxops's host-level manager lock briefly after systemd finishes so
    // the final properties have settled before the next maxops action observes them.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let after = observe_service_unit(app, &progress.unit).await?;
    let succeeded = match progress.action {
        ServiceAction::Start => after.active_state == "active",
        ServiceAction::Stop => after.active_state == "inactive",
        ServiceAction::Restart => {
            after.active_state == "active"
                && after.invocation_id.is_some()
                && after.invocation_id != progress.before.invocation_id
        }
        ServiceAction::Reload => {
            after.active_state == "active" && after.reload_result.as_deref() == Some("success")
        }
    };
    let state = if succeeded {
        JobState::Succeeded
    } else {
        JobState::Failed
    };
    app.store
        .transition_job(
            id,
            job.handle.revision,
            state,
            &json!({"phase":"systemd_job_completed","observed_state":after.active_state}),
            Some(&json!({
                "action":progress.action,
                "unit":progress.unit,
                "before":progress.before,
                "after":after,
                "manager_job":progress.manager_job,
                "attribution":"systemd_job_accepted_and_target_observed",
                "success":succeeded,
            })),
        )
        .await?;
    app.store
        .release_resource("systemd_manager", &app.config.host, id)
        .await?;
    Ok(())
}

async fn recover_uncertain_service_action(app: &App, id: &JobId, job: JobRecord) -> Result<()> {
    let progress: ServiceProgress = serde_json::from_value(
        job.result
            .clone()
            .ok_or_else(|| eyre!("service progress is missing"))?,
    )?;
    let after = observe_service_unit(app, &progress.unit).await?;
    let effect_observed = match progress.action {
        ServiceAction::Start => {
            progress.before.active_state != "active" && after.active_state == "active"
        }
        ServiceAction::Stop => {
            progress.before.active_state != "inactive" && after.active_state == "inactive"
        }
        ServiceAction::Restart => {
            after.active_state == "active"
                && after.invocation_id.is_some()
                && after.invocation_id != progress.before.invocation_id
        }
        ServiceAction::Reload => false,
    };
    if effect_observed {
        app.store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Succeeded,
                &json!({"phase":"recovered_by_observation"}),
                Some(&json!({
                    "action":progress.action,
                    "unit":progress.unit,
                    "before":progress.before,
                    "after":after,
                    "attribution":"observed_after_executor_restart_external_change_not_excluded",
                    "success":true,
                })),
            )
            .await?;
        app.store
            .release_resource("systemd_manager", &app.config.host, id)
            .await?;
        return Ok(());
    }
    let reconciling = app
        .store
        .transition_job(
            id,
            job.handle.revision,
            JobState::Reconciling,
            &json!({"phase":"recovery","effect":"not_attributable"}),
            Some(&serde_json::to_value(&progress)?),
        )
        .await?;
    transition_service_unknown(app, id, reconciling, &progress, "effect_not_attributable").await
}

async fn transition_service_unknown(
    app: &App,
    id: &JobId,
    job: JobRecord,
    progress: &ServiceProgress,
    reason: &str,
) -> Result<()> {
    app.store
        .transition_job(
            id,
            job.handle.revision,
            JobState::OutcomeUnknown,
            &json!({"phase":"recovery","reason":reason}),
            Some(&json!({
                "action":progress.action,
                "unit":progress.unit,
                "before":progress.before,
                "manager_job":progress.manager_job,
                "effect":"unknown",
                "reason":reason,
            })),
        )
        .await?;
    app.store
        .release_resource("systemd_manager", &app.config.host, id)
        .await?;
    Ok(())
}

fn explicit_systemd_rejection(error: &zbus::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    [
        "not applicable",
        "not supported",
        "not loaded",
        "not found",
        "no such unit",
        "invalid name",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn systemd_job_finished(error: &zbus::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("nosuchjob")
        || message.contains("no such job")
        || message.contains("no job") && message.contains("known")
}

async fn observe_service_unit(app: &App, unit: &str) -> Result<ServiceUnitState> {
    let manager = SystemdManagerProxy::new(&app.bus).await?;
    let path = match manager.get_unit(unit).await {
        Ok(path) => path,
        Err(error) if explicit_systemd_rejection(&error) => {
            return Ok(ServiceUnitState {
                active_state: "not_loaded".into(),
                sub_state: "not_loaded".into(),
                invocation_id: None,
                service_result: None,
                reload_result: None,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let proxy = SystemdUnitProxy::builder(&app.bus)
        .path(path.clone())?
        .build()
        .await?;
    let service = SystemdServiceProxy::builder(&app.bus)
        .path(path)?
        .build()
        .await?;
    let bytes = proxy.invocation_id().await?;
    Ok(ServiceUnitState {
        active_state: proxy.active_state().await?,
        sub_state: proxy.sub_state().await?,
        invocation_id: (!bytes.is_empty()).then(|| bytes_to_hex(&bytes)),
        service_result: service.result().await.ok(),
        reload_result: service.reload_result().await.ok(),
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

fn prepare_runner_spec(config: &Config, params: &ExecRunParams) -> Result<RunnerSpec> {
    let profile = config
        .profiles
        .get(&params.profile)
        .ok_or_else(|| eyre!("execution profile is not configured"))?;
    ensure!(
        params
            .credential_refs
            .iter()
            .all(|reference| profile.allowed_credentials.contains(reference)),
        "credential is not permitted by the execution profile"
    );
    let timeout = params.timeout_seconds.unwrap_or(profile.timeout_seconds);
    ensure!(
        timeout <= profile.timeout_seconds,
        "requested timeout exceeds profile limit"
    );
    let cwd = params.cwd.as_deref().map(PathBuf::from);
    if let Some(cwd) = &cwd {
        ensure!(cwd.is_absolute(), "working directory must be absolute");
        let canonical = cwd.canonicalize().wrap_err("resolve working directory")?;
        ensure!(
            profile
                .working_roots
                .iter()
                .filter_map(|root| root.canonicalize().ok())
                .any(|root| canonical.starts_with(root)),
            "working directory is outside configured roots"
        );
    }
    let mut env = profile.environment.clone();
    env.extend(params.env.clone());
    Ok(RunnerSpec {
        command: params.command.clone(),
        interpreter: profile.interpreter.clone(),
        cwd,
        env,
        credential_refs: params.credential_refs.clone(),
        output_limit_bytes: profile.output_limit_bytes,
    })
}

async fn persist_runner_spec(config: &Config, id: &JobId, spec: &RunnerSpec) -> Result<()> {
    let path = spec_path(config, id);
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec(spec)?;
    let mut file = tokio::fs::File::create(&temporary).await?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

async fn launch(
    config: &Config,
    id: &JobId,
    params: &ExecRunParams,
    _spec: &RunnerSpec,
    deadline: Option<jiff::Timestamp>,
) -> Result<()> {
    let profile = &config.profiles[&params.profile];
    let requested_timeout = params.timeout_seconds.unwrap_or(profile.timeout_seconds);
    let timeout = match deadline {
        Some(deadline) => {
            let remaining = deadline.as_second() - maxops_proto::now().as_second();
            ensure!(remaining > 0, "job deadline already expired");
            requested_timeout.min(u32::try_from(remaining).unwrap_or(u32::MAX))
        }
        None => requested_timeout,
    };
    let state_directory = format!("maxops-jobs/{}", id.as_str());
    let output = config.spool_root.join(id.as_str());
    let mut command = Command::new(&config.systemd_run);
    command
        .arg("--unit")
        .arg(unit_name(id))
        .arg("--property=Type=exec")
        .arg("--property=ExitType=cgroup")
        .arg("--property=KillMode=control-group")
        .arg("--property=SendSIGKILL=yes")
        .arg(format!("--property=RuntimeMaxSec={timeout}s"))
        .arg(format!("--property=TasksMax={}", profile.tasks_max))
        .arg(format!("--property=User={}", profile.user))
        .arg(format!("--property=StateDirectory={state_directory}"))
        .arg(format!(
            "--property=LoadCredential=spec:{}",
            spec_path(config, id).display()
        ));
    if let Some(bytes) = profile.memory_max_bytes {
        command.arg(format!("--property=MemoryMax={bytes}"));
    }
    for reference in &params.credential_refs {
        command.arg(format!(
            "--property=LoadCredential={reference}:{}",
            config.credential_sources[reference].display()
        ));
    }
    if !profile.privileged {
        command
            .arg("--property=NoNewPrivileges=yes")
            .arg("--property=ProtectSystem=strict")
            .arg("--property=ProtectHome=yes")
            .arg("--property=PrivateTmp=yes")
            .arg("--property=PrivateDevices=yes")
            .arg("--property=ProtectKernelTunables=yes")
            .arg("--property=ProtectKernelModules=yes")
            .arg("--property=ProtectControlGroups=yes")
            .arg("--property=RestrictSUIDSGID=yes")
            .arg("--property=CapabilityBoundingSet=");
    }
    let status = command
        .arg("--no-block")
        .arg("--")
        .arg(&config.runner)
        .arg("--credential")
        .arg("spec")
        .arg("--output-directory")
        .arg(output)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    ensure!(status.success(), "systemd-run failed");
    Ok(())
}

fn recover_job(app: Arc<App>, job: JobRecord) {
    if ServiceAction::from_operation(&job.handle.operation).is_some() {
        spawn_service_job(app, job.handle.job_id);
        return;
    }
    if workspace::is_workspace_job(&job.handle.operation)
        && job.handle.operation != "workspace.check"
    {
        workspace::spawn(app, job.handle.job_id);
        return;
    }
    let id = job.handle.job_id;
    tokio::spawn(async move {
        if job.handle.state == JobState::Queued {
            let recovered = async {
                let params =
                    runner_params(&app, &job.principal, &job.handle.operation, &job.spec).await?;
                let prepared = prepare_runner_spec(&app.config, &params)?;
                persist_runner_spec(&app.config, &id, &prepared).await?;
                let dispatching = app
                    .store
                    .transition_job(
                        &id,
                        job.handle.revision,
                        JobState::Dispatching,
                        &json!({"recovered":true,"launcher":"systemd"}),
                        None,
                    )
                    .await?;
                launch(&app.config, &id, &params, &prepared, job.deadline).await?;
                app.store
                    .transition_job(
                        &id,
                        dispatching.handle.revision,
                        JobState::Running,
                        &json!({"unit":unit_name(&id),"recovered":true}),
                        None,
                    )
                    .await
            }
            .await;
            if let Err(error) = recovered {
                tracing::warn!(job_id = %id, %error, "queued job recovery failed");
                let _ = app
                    .store
                    .transition_job(
                        &id,
                        job.handle.revision,
                        JobState::Failed,
                        &json!({"phase":"recovery"}),
                        Some(&json!({"phase":"recovery","error":"target could not recover the queued job"})),
                    )
                    .await;
                return;
            }
        } else if job.handle.state == JobState::Dispatching {
            let recovered = async {
                match systemd_state(&app.config, &id).await {
                    Ok(Some(state))
                        if matches!(
                            state.active_state.as_str(),
                            "active" | "activating" | "reloading"
                        ) =>
                    {
                        app.store
                            .transition_job(
                                &id,
                                job.handle.revision,
                                JobState::Running,
                                &json!({"unit":unit_name(&id),"recovered":true}),
                                None,
                            )
                            .await?;
                    }
                    Ok(None) => {
                        let params =
                            runner_params(&app, &job.principal, &job.handle.operation, &job.spec)
                                .await?;
                        let prepared = prepare_runner_spec(&app.config, &params)?;
                        persist_runner_spec(&app.config, &id, &prepared).await?;
                        launch(&app.config, &id, &params, &prepared, job.deadline).await?;
                        app.store
                            .transition_job(
                                &id,
                                job.handle.revision,
                                JobState::Running,
                                &json!({"unit":unit_name(&id),"recovered":true}),
                                None,
                            )
                            .await?;
                    }
                    Ok(Some(_)) => {
                        reconcile_once(&app, &id).await?;
                    }
                    Err(error) => return Err(error),
                }
                Result::<()>::Ok(())
            }
            .await;
            if let Err(error) = recovered {
                tracing::warn!(job_id = %id, %error, "dispatched job recovery failed");
                return;
            }
        }
        monitor(app, id);
    });
}

fn monitor(app: Arc<App>, id: JobId) {
    tokio::spawn(async move {
        loop {
            match reconcile_once(&app, &id).await {
                Ok(job) if job.handle.state.is_terminal() => break,
                Ok(_) => tokio::time::sleep(RECONCILE_INTERVAL).await,
                Err(error) => {
                    tracing::warn!(job_id = %id, %error, "job reconciliation failed");
                    tokio::time::sleep(RECONCILE_INTERVAL).await;
                }
            }
        }
    });
}

async fn reconcile_once(app: &App, id: &JobId) -> Result<JobRecord> {
    let job = app.store.get_job(id).await?;
    if job.handle.state.is_terminal() && job.handle.state != JobState::OutcomeUnknown {
        return Ok(job);
    }
    let result_path = app.config.spool_root.join(id.as_str()).join("result.json");
    if let Ok(bytes) = tokio::fs::read(&result_path).await {
        let result: RunnerResult = serde_json::from_slice(&bytes)?;
        let state = if result.success {
            JobState::Succeeded
        } else {
            JobState::Failed
        };
        return app
            .store
            .transition_job(
                id,
                job.handle.revision,
                state,
                &json!({"completion":"runner_result"}),
                Some(&serde_json::to_value(result)?),
            )
            .await;
    }
    let unit = match systemd_state(&app.config, id).await {
        Ok(Some(unit)) => unit,
        Ok(None) if job.handle.state == JobState::Reconciling => {
            return app
                .store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::OutcomeUnknown,
                    &json!({"reason":"unit and completion record unavailable"}),
                    Some(&json!({"effect":"unknown"})),
                )
                .await;
        }
        Ok(None) if matches!(job.handle.state, JobState::Running | JobState::Dispatching) => {
            return app
                .store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::Reconciling,
                    &json!({"reason":"unit temporarily unavailable"}),
                    None,
                )
                .await;
        }
        Ok(None) => return Ok(job),
        Err(error) => return Err(error),
    };
    match unit.active_state.as_str() {
        "active" | "activating" | "reloading" => Ok(job),
        "failed" => {
            let state = if unit.result == "timeout" {
                JobState::TimedOut
            } else {
                JobState::Failed
            };
            app.store
                .transition_job(
                    id,
                    job.handle.revision,
                    state,
                    &json!({"unit_result":unit.result,"invocation_id":unit.invocation_id}),
                    Some(&json!({"unit_result":unit.result})),
                )
                .await
        }
        "inactive" if job.cancel_requested => {
            app.store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::Cancelled,
                    &json!({"unit_result":unit.result}),
                    Some(&json!({"cancelled":true})),
                )
                .await
        }
        "inactive" if job.handle.state == JobState::Reconciling => {
            app.store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::OutcomeUnknown,
                    &json!({"unit_result":unit.result}),
                    Some(&json!({"effect":"unknown"})),
                )
                .await
        }
        "inactive" if job.handle.state == JobState::Running => {
            app.store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::Reconciling,
                    &json!({"unit_result":unit.result}),
                    None,
                )
                .await
        }
        "inactive" if job.handle.state == JobState::Dispatching => {
            app.store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::Reconciling,
                    &json!({"unit_result":unit.result}),
                    None,
                )
                .await
        }
        _ => Ok(job),
    }
}

struct UnitState {
    active_state: String,
    result: String,
    invocation_id: String,
}

async fn systemd_state(config: &Config, id: &JobId) -> Result<Option<UnitState>> {
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(&config.systemctl)
            .args([
                "show",
                &unit_name(id),
                "--no-pager",
                "--property=ActiveState",
                "--property=Result",
                "--property=InvocationID",
            ])
            .env_clear()
            .stdin(Stdio::null())
            .output(),
    )
    .await
    .wrap_err("systemctl query timed out")??;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
        if stderr.contains("not found") || stderr.contains("could not be found") {
            return Ok(None);
        }
        return Err(eyre!("systemd unit state unavailable"));
    }
    let text = std::str::from_utf8(&output.stdout)?;
    let properties: BTreeMap<_, _> = text
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    Ok(Some(UnitState {
        active_state: properties
            .get("ActiveState")
            .copied()
            .unwrap_or("unknown")
            .into(),
        result: properties
            .get("Result")
            .copied()
            .unwrap_or("unknown")
            .into(),
        invocation_id: properties.get("InvocationID").copied().unwrap_or("").into(),
    }))
}

async fn read_logs(
    config: &Config,
    job: &JobRecord,
    params: &maxops_proto::JobLogsParams,
) -> Result<JobLogsResponse> {
    ensure!(
        (1..=64 * 1024).contains(&params.limit),
        "invalid output read limit"
    );
    let directory = config.spool_root.join(params.job_id.as_str());
    let stdout = read_at(
        &directory.join("stdout.bin"),
        params.stdout_offset,
        params.limit,
    )
    .await?;
    let stderr = read_at(
        &directory.join("stderr.bin"),
        params.stderr_offset,
        params.limit,
    )
    .await?;
    let (stdout_discarded, stderr_discarded) = job
        .result
        .as_ref()
        .map(|result| {
            (
                result["stdout_discarded_bytes"].as_u64().unwrap_or(0),
                result["stderr_discarded_bytes"].as_u64().unwrap_or(0),
            )
        })
        .unwrap_or_default();
    Ok(JobLogsResponse {
        job_id: params.job_id.clone(),
        encoding: "base64".into(),
        stdout_base64: BASE64.encode(&stdout),
        stderr_base64: BASE64.encode(&stderr),
        next_stdout_offset: params.stdout_offset + stdout.len() as u64,
        next_stderr_offset: params.stderr_offset + stderr.len() as u64,
        complete: job.handle.state.is_terminal(),
        truncated: stdout_discarded > 0 || stderr_discarded > 0,
    })
}

async fn read_at(path: &Path, offset: u64, limit: u32) -> Result<Vec<u8>> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    file.seek(SeekFrom::Start(offset)).await?;
    let mut output = Vec::new();
    file.take(u64::from(limit)).read_to_end(&mut output).await?;
    Ok(output)
}

fn unit_name(id: &JobId) -> String {
    format!("maxops-job-{}.service", id.as_str())
}

fn spec_path(config: &Config, id: &JobId) -> PathBuf {
    config.spec_directory.join(format!("{}.json", id.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_relative_privileged_paths() {
        let config = Config {
            host: "host-a".into(),
            socket_path: "/run/maxops-executor/control.sock".into(),
            state_file: "/var/lib/maxops-executor/state.db".into(),
            spec_directory: "/var/lib/maxops-executor/specs".into(),
            spool_root: "/var/lib/maxops-jobs".into(),
            systemd_run: "systemd-run".into(),
            systemctl: "/run/current-system/sw/bin/systemctl".into(),
            runner: "/nix/store/example/bin/maxops-job-runner".into(),
            git: "/run/current-system/sw/bin/git".into(),
            workspace_root: "/var/lib/maxops-workspaces".into(),
            repository_root: "/var/lib/maxops-executor/repositories".into(),
            manageable_units: BTreeSet::new(),
            credential_sources: BTreeMap::new(),
            profiles: BTreeMap::from([(
                "diagnostic".into(),
                Profile {
                    user: "maxops-runner".into(),
                    interpreter: "/bin/sh".into(),
                    timeout_seconds: 30,
                    output_limit_bytes: 1024,
                    working_roots: Vec::new(),
                    environment: BTreeMap::new(),
                    allowed_credentials: Vec::new(),
                    privileged: false,
                    tasks_max: 16,
                    memory_max_bytes: None,
                },
            )]),
            repositories: BTreeMap::new(),
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn command_validation_keeps_argv_structured() {
        let params: ExecRunParams = serde_json::from_value(json!({
            "host":"host-a",
            "profile":"diagnostic",
            "command":{"argv":["/bin/echo","hello; reboot"]}
        }))
        .unwrap();
        assert!(matches!(params.command, maxops_proto::CommandSpec::Argv(_)));
        params.validate().unwrap();
    }
}
