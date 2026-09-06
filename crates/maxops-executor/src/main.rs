use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use clap::Parser;
use color_eyre::eyre::{Context, Result, ensure, eyre};
use maxops_executor::{RunnerResult, RunnerSpec};
use maxops_proto::{
    ExecRunParams, ExecutorRequest, ExecutorResponse, ExecutorWireResponse, JobId, JobLogsResponse,
    JobRecord, JobState,
};
use maxops_store::Store;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::BTreeMap,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom},
    net::{UnixListener, UnixStream},
    process::Command,
};

const MAX_REQUEST_BYTES: u64 = 128 * 1024;
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
    #[serde(default)]
    credential_sources: BTreeMap<String, PathBuf>,
    profiles: BTreeMap<String, Profile>,
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
    let listener = UnixListener::bind(&config.socket_path)?;
    std::fs::set_permissions(&config.socket_path, std::fs::Permissions::from_mode(0o660))?;
    let store = Store::open(&config.state_file).await?;
    let app = Arc::new(App { config, store });
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
    ] {
        ensure!(path.is_absolute(), "executor paths must be absolute");
    }
    ensure!(
        !config.profiles.is_empty(),
        "at least one execution profile is required"
    );
    ensure!(
        config.spool_root == Path::new("/var/lib/maxops-jobs"),
        "spool_root must match systemd StateDirectory root /var/lib/maxops-jobs"
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
    Ok(())
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
        Err(error) => ExecutorWireResponse::Error {
            code: "executor_request_failed".into(),
            message: error.to_string(),
        },
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

async fn handle(app: Arc<App>, request: ExecutorRequest) -> Result<ExecutorResponse> {
    match request {
        ExecutorRequest::Submit { job_id, job } => {
            ensure!(job.host == app.config.host, "job targets another host");
            ensure!(
                job.operation == "exec.run",
                "unsupported executor operation"
            );
            let accepted = app.store.accept_job(&job_id, &job).await?;
            if accepted.created {
                let prepared = (|| {
                    let params: ExecRunParams = serde_json::from_value(job.spec.clone())?;
                    params.validate().map_err(|message| eyre!(message))?;
                    ensure!(
                        params.host == app.config.host,
                        "command targets another host"
                    );
                    let prepared = prepare_runner_spec(&app.config, &params)?;
                    Result::<_>::Ok((params, prepared))
                })();
                let (params, prepared) = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        tracing::warn!(job_id = %job_id, %error, "job validation failed");
                        let failed = app
                            .store
                            .transition_job(
                                &job_id,
                                accepted.job.handle.revision,
                                JobState::Failed,
                                &json!({"phase":"validation"}),
                                Some(&json!({"phase":"validation","error":"job is not permitted by the target profile"})),
                            )
                            .await?;
                        return Ok(ExecutorResponse::Job(failed));
                    }
                };
                if let Err(error) = persist_runner_spec(&app.config, &job_id, &prepared).await {
                    tracing::warn!(job_id = %job_id, %error, "persisting job specification failed");
                    let failed = app
                        .store
                        .transition_job(
                            &job_id,
                            accepted.job.handle.revision,
                            JobState::Failed,
                            &json!({"phase":"prepare"}),
                            Some(&json!({"phase":"prepare","error":"target could not persist the job specification"})),
                        )
                        .await?;
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
        ExecutorRequest::Status(params) => {
            reconcile_once(&app, &params.job_id).await?;
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
    }
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
    let id = job.handle.job_id;
    tokio::spawn(async move {
        if job.handle.state == JobState::Queued {
            let recovered = async {
                let params: ExecRunParams = serde_json::from_value(job.spec.clone())?;
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
                        let params: ExecRunParams = serde_json::from_value(job.spec.clone())?;
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
