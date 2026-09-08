use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderMap, Response, StatusCode, header},
    routing::{get, post},
};
use futures::future::join_all;
use maxops_proto::{
    ChangeHistoryResponse, ChangeId, ChangePlan, ChangeRecord, ChangeState, CommandSpec,
    DeliveryStage, DeployChangeParams, DeployPrepareParams, DeploymentAction, DeploymentArtifact,
    DeploymentJobSpec, DeploymentKind, DeploymentReport, DeploymentReportStatus, DiagnosticBundle,
    DiagnosticCollectParams, DiagnosticEvidence, DiagnosticRuleResult, EpisodeId, EventKind,
    EvidenceAssessment, ExecRunParams, ExecutorRequest, ExecutorResponse, IdempotencyRequirement,
    JobCancelParams, JobId, JobIdParams, JobLogsParams, JobRecord, JobState, JobsListResponse,
    LogParams, NewJob, OperationKind, PROTOCOL_VERSION, RemediationBeginParams,
    RepositoryHeadRequest, RepositoryHeadResponse, Request, RuntimeBaseline, RuntimeStateRequest,
    RuntimeStateResponse, Snapshot, SourceBaseline, UnitActionParams, WorkspaceStatusParams,
    WorkspaceTargetRequest, WorkspaceTargetResponse, now, operations,
    transport::{self, ApiError, ApiResult, Token},
    valid_host, valid_observation_unit, valid_unit,
};
use maxops_store::{
    AlertEventInput, ChangeTransition, NewFleetEvent, RemediationCompletion, Store,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use utoipa::OpenApi;

mod client_api;
mod deployment_workflow;
mod metrics;

#[derive(utoipa::OpenApi)]
#[openapi(paths(execute, catalog), components(schemas(Request)), modifiers(&BearerSecurity))]
struct ApiDoc;

struct BearerSecurity;
impl utoipa::Modify for BearerSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer",
                utoipa::openapi::security::SecurityScheme::Http(
                    utoipa::openapi::security::Http::new(
                        utoipa::openapi::security::HttpAuthScheme::Bearer,
                    ),
                ),
            );
        }
    }
}

async fn openapi(State(app): State<Arc<App>>, headers: HeaderMap) -> ApiResult<Value> {
    authenticate(&app, &headers)?;
    Ok(Json(
        serde_json::to_value(ApiDoc::openapi()).expect("serializable OpenAPI"),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    listen: SocketAddr,
    hosts: Vec<HostConfig>,
    clients: Vec<ClientConfig>,
    #[serde(default)]
    repositories: Vec<RepositoryConfig>,
    #[serde(default)]
    deployments: Vec<DeploymentConfig>,
    #[serde(default)]
    prometheus_url: Option<String>,
    #[serde(default)]
    alertmanager_url: Option<String>,
    #[serde(default)]
    alert_ingress: Option<AlertIngressConfig>,
    #[serde(default)]
    event_sinks: Vec<EventSinkConfig>,
    #[serde(default)]
    remediation_policy: RemediationPolicyConfig,
    #[serde(default)]
    state_file: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HostConfig {
    name: String,
    #[serde(default)]
    site: Option<String>,
    agent_url: String,
    agent_token_file: PathBuf,
    #[serde(default)]
    execution_token_file: Option<PathBuf>,
    #[serde(default)]
    readable_units: BTreeSet<String>,
    #[serde(default)]
    read_all_units: bool,
    #[serde(default)]
    manageable_units: BTreeSet<String>,
    #[serde(default)]
    diagnostic_profile: Option<String>,
    #[serde(default)]
    diagnostic_probes: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientConfig {
    name: String,
    token_file: PathBuf,
    hosts: BTreeSet<String>,
    capabilities: BTreeSet<String>,
    #[serde(default)]
    repositories: BTreeSet<String>,
    #[serde(default)]
    deployments: BTreeSet<String>,
    #[serde(default)]
    access: Access,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepositoryConfig {
    name: String,
    executor_host: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentConfig {
    name: String,
    repository: String,
    builder_host: String,
    target_host: String,
    kind: DeploymentKind,
    flake_attribute: String,
    source_reference: String,
    #[serde(default = "default_plan_ttl")]
    plan_ttl_seconds: u32,
}

fn default_plan_ttl() -> u32 {
    3600
}

#[derive(Clone, Copy, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Access {
    #[default]
    Observe,
    Manage,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertIngressConfig {
    token_file: PathBuf,
    sink_url: String,
    sink_token_file: Option<PathBuf>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventSinkConfig {
    id: String,
    url: String,
    #[serde(default)]
    token_file: Option<PathBuf>,
    #[serde(default)]
    hosts: BTreeSet<String>,
    #[serde(default)]
    kinds: BTreeSet<EventKind>,
    #[serde(default = "default_delivery_retry")]
    retry_seconds: u32,
}

fn default_delivery_retry() -> u32 {
    5
}

#[derive(Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RemediationPolicyConfig {
    max_attempts_per_episode: u16,
    cooldown_seconds: u32,
}

impl Default for RemediationPolicyConfig {
    fn default() -> Self {
        Self {
            max_attempts_per_episode: 2,
            cooldown_seconds: 300,
        }
    }
}

struct Host {
    config: HostConfig,
    token: Token,
    execution_token: Option<Token>,
}
struct Principal {
    name: String,
    token: Token,
    hosts: BTreeSet<String>,
    capabilities: BTreeSet<String>,
    access: Access,
    repositories: BTreeSet<String>,
    deployments: BTreeSet<String>,
}
struct AlertIngress {
    token: Token,
    sink_url: String,
    sink_token: Option<Token>,
}
struct EventSink {
    config: EventSinkConfig,
    token: Option<Token>,
}
struct App {
    hosts: BTreeMap<String, Host>,
    clients: Vec<Principal>,
    client: reqwest::Client,
    prometheus_url: Option<String>,
    alertmanager_url: Option<String>,
    alert_ingress: Option<AlertIngress>,
    event_sinks: Vec<EventSink>,
    remediation_policy: RemediationPolicyConfig,
    slots: Semaphore,
    wait_slots: Semaphore,
    store: Option<Store>,
    repositories: BTreeMap<String, String>,
    deployments: BTreeMap<String, DeploymentConfig>,
    started_at: jiff::Timestamp,
    agent_heartbeats: Mutex<BTreeMap<String, jiff::Timestamp>>,
    executor_heartbeats: Mutex<BTreeMap<String, jiff::Timestamp>>,
    local_workers: Mutex<HashSet<JobId>>,
    requests_total: AtomicU64,
    events_ingested_total: AtomicU64,
    delivery_attempts_total: AtomicU64,
    delivery_failures_total: AtomicU64,
}

pub async fn build(config: Config) -> color_eyre::eyre::Result<(SocketAddr, Router)> {
    transport::validate_listen(config.listen)?;
    for url in [&config.prometheus_url, &config.alertmanager_url]
        .into_iter()
        .flatten()
    {
        transport::validate_url(url)?;
    }
    let mut hosts = BTreeMap::new();
    for host in config.hosts {
        color_eyre::eyre::ensure!(valid_host(&host.name), "invalid inventory host name");
        color_eyre::eyre::ensure!(
            host.readable_units
                .iter()
                .all(|u| valid_observation_unit(u)),
            "invalid readable service name"
        );
        color_eyre::eyre::ensure!(
            host.manageable_units.iter().all(|unit| valid_unit(unit)
                && (host.read_all_units || host.readable_units.contains(unit))),
            "manageable services must be valid readable service names"
        );
        color_eyre::eyre::ensure!(
            host.diagnostic_probes.is_empty() || host.diagnostic_profile.is_some(),
            "diagnostic probes require an execution profile"
        );
        color_eyre::eyre::ensure!(
            host.diagnostic_probes.iter().all(|(name, argv)| {
                maxops_proto::valid_check_id(name)
                    && !argv.is_empty()
                    && argv.len() <= 256
                    && argv.first().is_some_and(|program| program.starts_with('/'))
                    && argv.iter().all(|argument| argument.len() <= 8192)
            }),
            "invalid diagnostic probe"
        );
        transport::validate_url(&host.agent_url)?;
        let token = Token::read(&host.agent_token_file)?;
        let execution_token = host
            .execution_token_file
            .as_deref()
            .map(Token::read)
            .transpose()?;
        if let Some(execution_token) = &execution_token {
            color_eyre::eyre::ensure!(
                !execution_token.same_as(&token),
                "agent observation and execution credentials must differ"
            );
        }
        color_eyre::eyre::ensure!(
            !hosts.values().any(|other: &Host| {
                other.token.same_as(&token)
                    || execution_token
                        .as_ref()
                        .is_some_and(|execution| other.token.same_as(execution))
                    || other.execution_token.as_ref().is_some_and(|execution| {
                        execution.same_as(&token)
                            || execution_token
                                .as_ref()
                                .is_some_and(|candidate| execution.same_as(candidate))
                    })
            }),
            "each agent endpoint requires distinct credentials"
        );
        color_eyre::eyre::ensure!(
            hosts
                .insert(
                    host.name.clone(),
                    Host {
                        config: host,
                        token,
                        execution_token,
                    }
                )
                .is_none(),
            "duplicate inventory host"
        );
    }
    let mut repositories = BTreeMap::new();
    for repository in config.repositories {
        color_eyre::eyre::ensure!(
            maxops_proto::valid_repository_id(&repository.name),
            "invalid repository ID"
        );
        color_eyre::eyre::ensure!(
            hosts
                .get(&repository.executor_host)
                .is_some_and(|host| host.execution_token.is_some()),
            "repository executor host is unknown or has execution disabled"
        );
        color_eyre::eyre::ensure!(
            repositories
                .insert(repository.name, repository.executor_host)
                .is_none(),
            "duplicate repository ID"
        );
    }
    let known_caps: BTreeSet<_> = operations().into_iter().map(|op| op.capability).collect();
    let mut deployments = BTreeMap::new();
    for deployment in config.deployments {
        color_eyre::eyre::ensure!(
            maxops_proto::valid_check_id(&deployment.name),
            "invalid deployment profile name"
        );
        color_eyre::eyre::ensure!(
            repositories.get(&deployment.repository) == Some(&deployment.builder_host),
            "deployment builder must own its configured repository"
        );
        color_eyre::eyre::ensure!(
            hosts
                .get(&deployment.target_host)
                .is_some_and(|host| host.execution_token.is_some()),
            "deployment target is unknown or has execution disabled"
        );
        color_eyre::eyre::ensure!(
            !deployment.flake_attribute.is_empty()
                && !deployment.source_reference.is_empty()
                && deployment.plan_ttl_seconds > 0,
            "invalid deployment profile"
        );
        color_eyre::eyre::ensure!(
            deployments
                .insert(deployment.name.clone(), deployment)
                .is_none(),
            "duplicate deployment profile"
        );
    }
    let management_caps: BTreeSet<_> = operations()
        .into_iter()
        .filter(|operation| operation.kind != OperationKind::Observation)
        .map(|operation| operation.capability)
        .collect();
    let mut clients: Vec<Principal> = Vec::new();
    for client in config.clients {
        color_eyre::eyre::ensure!(!client.name.is_empty(), "empty client name");
        color_eyre::eyre::ensure!(
            client
                .capabilities
                .iter()
                .all(|cap| known_caps.contains(cap.as_str())),
            "unknown capability"
        );
        color_eyre::eyre::ensure!(
            client.access == Access::Manage
                || client
                    .capabilities
                    .iter()
                    .all(|capability| !management_caps.contains(capability.as_str())),
            "observation client cannot receive job capabilities"
        );
        color_eyre::eyre::ensure!(
            client.hosts.iter().all(|name| hosts.contains_key(name)),
            "policy references an unknown host"
        );
        color_eyre::eyre::ensure!(
            client
                .repositories
                .iter()
                .all(|name| repositories.contains_key(name)),
            "policy references an unknown repository"
        );
        color_eyre::eyre::ensure!(
            client.repositories.iter().all(|name| {
                repositories
                    .get(name)
                    .is_some_and(|host| client.hosts.contains(host))
            }),
            "repository executor must also be in the client's host scope"
        );
        color_eyre::eyre::ensure!(
            client.deployments.iter().all(|name| {
                deployments.get(name).is_some_and(|deployment| {
                    client.repositories.contains(&deployment.repository)
                        && client.hosts.contains(&deployment.builder_host)
                        && client.hosts.contains(&deployment.target_host)
                })
            }),
            "deployment grant must remain within the client's host and repository scope"
        );
        let token = Token::read(&client.token_file)?;
        color_eyre::eyre::ensure!(
            !clients
                .iter()
                .any(|p| p.name == client.name || p.token.same_as(&token)),
            "duplicate client name or token"
        );
        color_eyre::eyre::ensure!(
            !hosts.values().any(|h| h.token.same_as(&token)),
            "client and agent credentials must differ"
        );
        color_eyre::eyre::ensure!(
            !hosts.values().any(|host| {
                host.execution_token
                    .as_ref()
                    .is_some_and(|execution| execution.same_as(&token))
            }),
            "client and agent execution credentials must differ"
        );
        clients.push(Principal {
            name: client.name,
            token,
            hosts: client.hosts,
            capabilities: client.capabilities,
            access: client.access,
            repositories: client.repositories,
            deployments: client.deployments,
        });
    }
    color_eyre::eyre::ensure!(
        !clients.is_empty(),
        "at least one authenticated client is required"
    );
    let management_enabled = clients.iter().any(|client| client.access == Access::Manage);
    color_eyre::eyre::ensure!(
        !management_enabled || config.state_file.is_some(),
        "management clients require a durable hub state_file"
    );
    let store = match config.state_file.as_deref() {
        Some(path) => Some(Store::open(path).await?),
        None => None,
    };
    let alert_ingress = config
        .alert_ingress
        .map(|ingress| -> color_eyre::eyre::Result<_> {
            transport::validate_url(&ingress.sink_url)?;
            let token = Token::read(&ingress.token_file)?;
            let sink_token = ingress
                .sink_token_file
                .map(|p| Token::read(&p))
                .transpose()?;
            color_eyre::eyre::ensure!(
                !clients.iter().any(|p| p.token.same_as(&token))
                    && !hosts.values().any(|host| {
                        host.token.same_as(&token)
                            || host
                                .execution_token
                                .as_ref()
                                .is_some_and(|execution| execution.same_as(&token))
                    }),
                "alert ingress requires a dedicated token"
            );
            if let Some(sink) = &sink_token {
                color_eyre::eyre::ensure!(
                    !sink.same_as(&token)
                        && !clients
                            .iter()
                            .any(|principal| principal.token.same_as(sink))
                        && !hosts.values().any(|host| host.token.same_as(sink))
                        && !hosts.values().any(|host| {
                            host.execution_token
                                .as_ref()
                                .is_some_and(|execution| execution.same_as(sink))
                        }),
                    "notification sink requires a dedicated token"
                );
            }
            Ok(AlertIngress {
                token,
                sink_url: ingress.sink_url,
                sink_token,
            })
        })
        .transpose()?;
    color_eyre::eyre::ensure!(
        config.event_sinks.is_empty() || store.is_some(),
        "event sinks require durable hub storage"
    );
    color_eyre::eyre::ensure!(
        config.remediation_policy.max_attempts_per_episode > 0,
        "remediation attempt budget must be positive"
    );
    let mut event_sinks = Vec::new();
    for sink in config.event_sinks {
        color_eyre::eyre::ensure!(
            !sink.id.is_empty()
                && sink.id.len() <= 128
                && sink.retry_seconds > 0
                && sink.hosts.iter().all(|host| hosts.contains_key(host)),
            "invalid event sink"
        );
        transport::validate_url(&sink.url)?;
        let token = sink.token_file.as_deref().map(Token::read).transpose()?;
        if let Some(token) = &token {
            color_eyre::eyre::ensure!(
                !clients.iter().any(|client| client.token.same_as(token))
                    && !hosts.values().any(|host| {
                        host.token.same_as(token)
                            || host
                                .execution_token
                                .as_ref()
                                .is_some_and(|execution| execution.same_as(token))
                    })
                    && !event_sinks.iter().any(|other: &EventSink| {
                        other
                            .token
                            .as_ref()
                            .is_some_and(|existing| existing.same_as(token))
                    })
                    && !alert_ingress.as_ref().is_some_and(|ingress| {
                        ingress.token.same_as(token)
                            || ingress
                                .sink_token
                                .as_ref()
                                .is_some_and(|existing| existing.same_as(token))
                    }),
                "event sink requires a dedicated token"
            );
        }
        color_eyre::eyre::ensure!(
            !event_sinks
                .iter()
                .any(|other: &EventSink| other.config.id == sink.id),
            "duplicate event sink"
        );
        if let Some(store) = &store {
            store
                .ensure_subscription(
                    &sink.id,
                    &json!({"hosts":sink.hosts,"kinds":sink.kinds}),
                    &json!({"url":sink.url}),
                    sink.token_file.as_ref().map(|_| sink.id.as_str()),
                )
                .await?;
        }
        event_sinks.push(EventSink {
            config: sink,
            token,
        });
    }
    let app = Arc::new(App {
        hosts,
        clients,
        client: transport::client()?,
        prometheus_url: config.prometheus_url,
        alertmanager_url: config.alertmanager_url,
        alert_ingress,
        event_sinks,
        remediation_policy: config.remediation_policy,
        slots: Semaphore::new(16),
        wait_slots: Semaphore::new(64),
        store,
        repositories,
        deployments,
        started_at: now(),
        agent_heartbeats: Mutex::new(BTreeMap::new()),
        executor_heartbeats: Mutex::new(BTreeMap::new()),
        local_workers: Mutex::new(HashSet::new()),
        requests_total: AtomicU64::new(0),
        events_ingested_total: AtomicU64::new(0),
        delivery_attempts_total: AtomicU64::new(0),
        delivery_failures_total: AtomicU64::new(0),
    });
    if let Some(store) = &app.store {
        for job in store.nonterminal_jobs().await? {
            match job.handle.operation.as_str() {
                "deploy.run" => deployment_workflow::spawn(app.clone(), job),
                "diagnostics.collect" => spawn_diagnostics(app.clone(), job),
                "remediations.begin" => spawn_remediation_begin(app.clone(), job),
                _ => spawn_dispatch(app.clone(), job.handle.job_id),
            }
        }
    }
    if !app.event_sinks.is_empty() {
        spawn_event_delivery(app.clone());
    }
    Ok((config.listen, router(app)))
}

fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/healthz", get(transport::health))
        .route("/readyz", get(readiness))
        .route("/metrics", get(self_metrics))
        .route("/v1/operations", get(catalog))
        .route("/v1/openapi.json", get(openapi))
        .route(
            "/v1/execute",
            post(execute).layer(DefaultBodyLimit::max(maxops_proto::transport::MAX_BODY)),
        )
        .route("/v1/alerts", post(alerts))
        .layer(DefaultBodyLimit::max(256 * 1024))
        .with_state(app)
}

async fn readiness(State(app): State<Arc<App>>) -> (StatusCode, Json<Value>) {
    let storage = match &app.store {
        Some(store) => match store.stats().await {
            Ok(_) => "ready",
            Err(_) => "unavailable",
        },
        None => "disabled",
    };
    let ready = storage != "unavailable";
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "ready": ready,
            "components": {
                "storage": storage,
                "agents": "independent",
            }
        })),
    )
}

async fn self_metrics(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> Result<Response<Body>, ApiError> {
    let principal = authenticate(&app, &headers)?;
    if !principal.capabilities.contains("self:read") {
        return Err(ApiError(StatusCode::FORBIDDEN, "capability not permitted"));
    }
    let stats = match &app.store {
        Some(store) => Some(store.stats().await.map_err(map_store_error)?),
        None => None,
    };
    let mut body = format!(
        concat!(
            "# TYPE maxops_requests_total counter\n",
            "maxops_requests_total {}\n",
            "# TYPE maxops_events_ingested_total counter\n",
            "maxops_events_ingested_total {}\n",
            "# TYPE maxops_event_delivery_attempts_total counter\n",
            "maxops_event_delivery_attempts_total {}\n",
            "# TYPE maxops_event_delivery_failures_total counter\n",
            "maxops_event_delivery_failures_total {}\n",
            "# TYPE maxops_jobs_nonterminal gauge\n",
            "maxops_jobs_nonterminal {}\n",
            "# TYPE maxops_jobs_queued gauge\n",
            "maxops_jobs_queued {}\n",
            "# TYPE maxops_jobs_outcome_unknown gauge\n",
            "maxops_jobs_outcome_unknown {}\n",
            "# TYPE maxops_jobs_completed_total counter\n",
            "maxops_jobs_completed_total {}\n",
            "# TYPE maxops_job_duration_seconds_sum counter\n",
            "maxops_job_duration_seconds_sum {}\n",
            "# TYPE maxops_reconciliations_total counter\n",
            "maxops_reconciliations_total {}\n",
            "# TYPE maxops_event_deliveries_pending gauge\n",
            "maxops_event_deliveries_pending {}\n",
            "# TYPE maxops_remediations_active gauge\n",
            "maxops_remediations_active {}\n",
            "# TYPE maxops_store_database_bytes gauge\n",
            "maxops_store_database_bytes {}\n",
            "# TYPE maxops_store_available_bytes gauge\n",
            "maxops_store_available_bytes {}\n",
        ),
        app.requests_total.load(Ordering::Relaxed),
        app.events_ingested_total.load(Ordering::Relaxed),
        app.delivery_attempts_total.load(Ordering::Relaxed),
        app.delivery_failures_total.load(Ordering::Relaxed),
        stats.as_ref().map_or(0, |stats| stats.jobs_nonterminal),
        stats.as_ref().map_or(0, |stats| stats.jobs_queued),
        stats.as_ref().map_or(0, |stats| stats.jobs_outcome_unknown),
        stats.as_ref().map_or(0, |stats| stats.jobs_completed_total),
        stats
            .as_ref()
            .map_or(0.0, |stats| stats.job_duration_seconds_sum),
        stats
            .as_ref()
            .map_or(0, |stats| stats.reconciliations_total),
        stats.as_ref().map_or(0, |stats| stats.deliveries_pending),
        stats.as_ref().map_or(0, |stats| stats.remediations_active),
        stats.as_ref().map_or(0, |stats| stats.database_size_bytes),
        stats
            .as_ref()
            .map_or(0, |stats| stats.storage_available_bytes),
    );
    body.push_str("# TYPE maxops_agent_heartbeat_age_seconds gauge\n");
    body.push_str("# TYPE maxops_executor_heartbeat_age_seconds gauge\n");
    let agent_heartbeats = app
        .agent_heartbeats
        .lock()
        .expect("agent heartbeat lock poisoned");
    let executor_heartbeats = app
        .executor_heartbeats
        .lock()
        .expect("executor heartbeat lock poisoned");
    for host in &principal.hosts {
        let agent_age = agent_heartbeats
            .get(host)
            .map(|at| now().as_second().saturating_sub(at.as_second()))
            .unwrap_or(-1);
        let executor_age = executor_heartbeats
            .get(host)
            .map(|at| now().as_second().saturating_sub(at.as_second()))
            .unwrap_or(-1);
        body.push_str(&format!(
            "maxops_agent_heartbeat_age_seconds{{host=\"{host}\"}} {agent_age}\n\
             maxops_executor_heartbeat_age_seconds{{host=\"{host}\"}} {executor_age}\n"
        ));
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "response build failed"))
}

fn authenticate<'a>(app: &'a App, headers: &HeaderMap) -> Result<&'a Principal, ApiError> {
    app.clients
        .iter()
        .find(|p| p.token.matches(headers))
        .ok_or(ApiError(StatusCode::UNAUTHORIZED, "unauthorized"))
}

fn host_for<'a>(app: &'a App, principal: &Principal, name: &str) -> Result<&'a Host, ApiError> {
    if !principal.hosts.contains(name) {
        return Err(ApiError(StatusCode::FORBIDDEN, "host not permitted"));
    }
    app.hosts
        .get(name)
        .ok_or(ApiError(StatusCode::FORBIDDEN, "host not permitted"))
}

fn repository_host<'a>(
    app: &'a App,
    principal: &Principal,
    repository: &str,
) -> Result<&'a str, ApiError> {
    if !principal.repositories.contains(repository) {
        return Err(ApiError(StatusCode::FORBIDDEN, "repository not permitted"));
    }
    let host = app
        .repositories
        .get(repository)
        .ok_or(ApiError(StatusCode::FORBIDDEN, "repository not permitted"))?;
    host_for(app, principal, host)?;
    Ok(host)
}

fn deployment_for<'a>(
    app: &'a App,
    principal: &Principal,
    name: &str,
) -> Result<&'a DeploymentConfig, ApiError> {
    if !principal.deployments.contains(name) {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "deployment profile not permitted",
        ));
    }
    let deployment = app.deployments.get(name).ok_or(ApiError(
        StatusCode::FORBIDDEN,
        "deployment profile not permitted",
    ))?;
    repository_host(app, principal, &deployment.repository)?;
    host_for(app, principal, &deployment.target_host)?;
    Ok(deployment)
}

async fn repository_head(
    app: &App,
    principal: &Principal,
    repository: &str,
    reference: &str,
) -> Result<RepositoryHeadResponse, ApiError> {
    let host = repository_host(app, principal, repository)?;
    let response = agent_request(
        app,
        host,
        &ExecutorRequest::RepositoryHead {
            principal: principal.name.clone(),
            request: RepositoryHeadRequest {
                repository: repository.to_owned(),
                reference: reference.to_owned(),
            },
        },
    )
    .await
    .map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "repository executor unavailable",
        )
    })?;
    let ExecutorResponse::RepositoryHead(response) = response else {
        return Err(ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid executor response",
        ));
    };
    if response.repository != repository || response.reference != reference {
        return Err(ApiError(
            StatusCode::BAD_GATEWAY,
            "repository observation identity mismatch",
        ));
    }
    Ok(response)
}

async fn runtime_state(
    app: &App,
    principal: &Principal,
    deployment: &DeploymentConfig,
) -> Result<RuntimeStateResponse, ApiError> {
    host_for(app, principal, &deployment.target_host)?;
    let response = agent_request(
        app,
        &deployment.target_host,
        &ExecutorRequest::RuntimeState {
            principal: principal.name.clone(),
            request: RuntimeStateRequest {
                deployment_profile: deployment.name.clone(),
            },
        },
    )
    .await
    .map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "target executor unavailable",
        )
    })?;
    let ExecutorResponse::RuntimeState(response) = response else {
        return Err(ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid executor response",
        ));
    };
    if response.host != deployment.target_host
        || response.deployment_profile != deployment.name
        || response.observed_at.as_second() > now().as_second() + 30
        || now().as_second() - response.observed_at.as_second() > 90
    {
        return Err(ApiError(
            StatusCode::BAD_GATEWAY,
            "runtime observation identity mismatch",
        ));
    }
    Ok(response)
}

fn unit_allowed(host: &Host, unit: &str) -> Result<(), ApiError> {
    if !valid_observation_unit(unit) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid observation unit name",
        ));
    }
    if !host.config.read_all_units && !host.config.readable_units.contains(unit) {
        return Err(ApiError(StatusCode::FORBIDDEN, "unit not permitted"));
    }
    Ok(())
}

fn unit_manageable(host: &Host, unit: &str) -> Result<(), ApiError> {
    if !host.config.manageable_units.contains(unit) {
        return Err(ApiError(StatusCode::FORBIDDEN, "unit is not manageable"));
    }
    Ok(())
}

#[utoipa::path(get, path = "/v1/operations", responses(
    (status = 200, description = "Operations permitted for the authenticated principal", body = Value),
    (status = 401, description = "Missing or invalid bearer token")
), security(("bearer" = [])))]
async fn catalog(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    query: Result<Query<client_api::CatalogQuery>, axum::extract::rejection::QueryRejection>,
) -> ApiResult<Value> {
    let principal = authenticate(&app, &headers)?;
    let Query(query) =
        query.map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid catalog parameters"))?;
    Ok(Json(client_api::catalog_value(principal, query)?))
}

fn unit_scope(snapshot: &Snapshot) -> Value {
    json!({"coverage": if snapshot.read_all_units {"all_loaded"} else {"allowlist"},
        "observed_count": snapshot.units.len(), "includes_all_installed": false})
}

async fn observe(app: &App, host: &Host) -> color_eyre::eyre::Result<Snapshot> {
    let request = host.token.apply(app.client.get(format!(
        "{}/v1/snapshot",
        host.config.agent_url.trim_end_matches('/')
    )));
    let mut snapshot: Snapshot = transport::read_json(request).await?;
    color_eyre::eyre::ensure!(snapshot.host == host.config.name, "agent identity mismatch");
    color_eyre::eyre::ensure!(
        snapshot.observed_at.as_second() <= now().as_second() + 30
            && now().as_second() - snapshot.observed_at.as_second() <= 90,
        "agent observation is stale or clock is skewed"
    );
    snapshot.units.retain(|unit| {
        valid_observation_unit(&unit.unit)
            && (host.config.read_all_units || host.config.readable_units.contains(&unit.unit))
    });
    snapshot.read_all_units &= host.config.read_all_units;
    app.agent_heartbeats
        .lock()
        .expect("agent heartbeat lock poisoned")
        .insert(host.config.name.clone(), snapshot.observed_at);
    Ok(snapshot)
}

async fn snapshots<'a>(
    app: &'a App,
    hosts: Vec<&'a Host>,
) -> Vec<(&'a Host, color_eyre::eyre::Result<Snapshot>)> {
    let mut results = Vec::new();
    for chunk in hosts.chunks(8) {
        results.extend(
            join_all(
                chunk
                    .iter()
                    .map(|&host| async move { (host, observe(app, host).await) }),
            )
            .await,
        );
    }
    results.sort_by(|a, b| a.0.config.name.cmp(&b.0.config.name));
    results
}

async fn prometheus_vector(
    app: &App,
    url: &str,
    query: &str,
) -> color_eyre::eyre::Result<Vec<Value>> {
    let request = app
        .client
        .get(format!("{}/api/v1/query", url.trim_end_matches('/')))
        .query(&[("query", query), ("timeout", "5s")]);
    let value = transport::read_json::<Value>(request).await?;
    color_eyre::eyre::ensure!(
        value["status"] == "success" && value["data"]["resultType"] == "vector",
        "invalid Prometheus response"
    );
    value["data"]["result"]
        .as_array()
        .cloned()
        .ok_or_else(|| color_eyre::eyre::eyre!("invalid vector"))
}

async fn exporter_samples(app: &App) -> (&'static str, BTreeMap<String, Value>) {
    let Some(url) = &app.prometheus_url else {
        return ("not_configured", BTreeMap::new());
    };
    // Instant-query result timestamps are evaluation times, not scrape times.
    let (values, times) = tokio::join!(
        prometheus_vector(app, url, "up{job=\"node\"}"),
        prometheus_vector(app, url, "timestamp(up{job=\"node\"})")
    );
    let (Ok(mut values), Ok(mut times)) = (values, times) else {
        return ("unavailable", BTreeMap::new());
    };
    // timestamp() drops __name__; match on all remaining series labels.
    for sample in values.iter_mut().chain(times.iter_mut()) {
        if let Some(labels) = sample.get_mut("metric").and_then(Value::as_object_mut) {
            labels.remove("__name__");
        }
    }
    let mut result = BTreeMap::new();
    for sample in values {
        let Some(name) = sample["metric"]["instance"].as_str() else {
            continue;
        };
        let timestamp = times
            .iter()
            .find(|s| s["metric"] == sample["metric"])
            .and_then(|s| s["value"][1].as_str())
            .and_then(|s| s.parse::<f64>().ok());
        let state = match timestamp {
            Some(timestamp)
                if timestamp.is_finite()
                    && timestamp <= now().as_second() as f64 + 30.0
                    && now().as_second() as f64 - timestamp <= 90.0 =>
            {
                match sample["value"][1].as_str() {
                    Some("1") => "up",
                    Some("0") => "down",
                    _ => "unknown",
                }
            }
            Some(_) => "stale",
            None => "unknown",
        };
        let observation = json!({"state": state, "sample_at_unix_seconds": timestamp});
        result
            .entry(name.to_owned())
            .and_modify(|v| *v = json!({"state": "ambiguous", "sample_at_unix_seconds": null}))
            .or_insert(observation);
    }
    ("available", result)
}

async fn fleet_pressure(app: &App, principal: &Principal) -> BTreeMap<String, Value> {
    let names: Vec<_> = principal.hosts.iter().collect();
    let mut results = BTreeMap::new();
    for chunk in names.chunks(8) {
        results.extend(
            join_all(chunk.iter().map(|name| async move {
                ((*name).clone(), metrics::host_metrics(app, name).await)
            }))
            .await,
        );
    }
    results
}

async fn run(app: &App, principal: &Principal, request: Request) -> Result<Value, ApiError> {
    let upstream_error = || ApiError(StatusCode::BAD_GATEWAY, "upstream observation unavailable");
    match request {
        Request::ResourcesList(params) => client_api::resources(app, principal, params).await,
        Request::FleetOverview(_) => {
            let hosts = principal
                .hosts
                .iter()
                .filter_map(|name| app.hosts.get(name))
                .collect();
            let pressure = fleet_pressure(app, principal);
            let (observations, (prometheus, samples), pressure) =
                tokio::join!(snapshots(app, hosts), exporter_samples(app), pressure);
            let rows: Vec<_> = observations.into_iter().map(|(host, result)| {
                let observation = match result {
                    Ok(snapshot) => json!({"state": "reachable", "observed_at": snapshot.observed_at,
                        "unit_scope": unit_scope(&snapshot), "failed_units": snapshot.units.iter().filter(|u| u.active_state == "failed").count()}),
                    Err(_) => json!({"state": "unavailable", "observed_at": null, "failed_units": null}),
                };
                let exporter = samples.get(&host.config.name).cloned().unwrap_or(json!({"state": "unknown", "sample_at_unix_seconds": null}));
                let assessment = match (observation["state"].as_str(), exporter["state"].as_str()) {
                    (Some("reachable"), Some("up")) => "observed_up",
                    (Some("reachable"), Some("down")) => "exporter_unavailable",
                    (Some("reachable"), _) => "agent_reachable_exporter_unknown",
                    (_, Some("up")) => "agent_unavailable",
                    (_, Some("down")) => "unreachable",
                    _ => "unknown",
                };
                let metrics = pressure.get(&host.config.name).cloned().unwrap_or(Value::Null);
                json!({"host": host.config.name, "site": host.config.site, "agent": observation, "exporter": exporter, "assessment": assessment,
                    "pressure": {"state": metrics["state"], "load1": metrics["metrics"]["load1"], "filesystem_available_bytes": metrics["metrics"]["filesystem_available_bytes"], "filesystem_size_bytes": metrics["metrics"]["filesystem_size_bytes"]}})
            }).collect();
            Ok(json!({"observed_at": now(), "prometheus": prometheus, "hosts": rows}))
        }
        Request::UnitsFailed(params) => {
            let hosts = match params.host {
                Some(name) => vec![host_for(app, principal, &name)?],
                None => principal
                    .hosts
                    .iter()
                    .filter_map(|name| app.hosts.get(name))
                    .collect(),
            };
            let rows: Vec<_> = snapshots(app, hosts).await.into_iter().map(|(host, result)| match result {
                Ok(snapshot) => json!({"host": host.config.name, "observed_at": snapshot.observed_at,
                    "state": "available", "unit_scope": unit_scope(&snapshot), "units": snapshot.units.into_iter().filter(|u| u.active_state == "failed").collect::<Vec<_>>()}),
                Err(_) => json!({"host": host.config.name, "observed_at": null, "state": "unavailable", "units": null}),
            }).collect();
            Ok(json!({"observed_at": now(), "hosts": rows}))
        }
        Request::HostFacts(params) => {
            let host = host_for(app, principal, &params.host)?;
            let snapshot = observe(app, host).await.map_err(|_| upstream_error())?;
            Ok(
                json!({"host": snapshot.host, "observed_at": snapshot.observed_at, "facts": snapshot.facts}),
            )
        }
        Request::HostMetrics(params) => {
            let host = host_for(app, principal, &params.host)?;
            let metrics = metrics::host_metrics(app, &host.config.name).await;
            Ok(json!({"host": host.config.name, "observed_at": now(), "observation": metrics}))
        }
        Request::DeployStatus(params) => {
            let hosts = match params.host {
                Some(name) => vec![host_for(app, principal, &name)?],
                None => principal
                    .hosts
                    .iter()
                    .filter_map(|name| app.hosts.get(name))
                    .collect(),
            };
            let rows: Vec<_> = snapshots(app, hosts).await.into_iter().map(|(host, result)| match result {
                Ok(snapshot) => json!({"host": host.config.name, "state": "available", "observed_at": snapshot.observed_at,
                    "running_closure": snapshot.facts.system_closure, "system_profile": snapshot.facts.system_profile,
                    "profile_generation": snapshot.facts.profile_generation, "profile_matches_running": snapshot.facts.profile_matches_running,
                    "activated_at": null}),
                Err(_) => json!({"host": host.config.name, "state": "unavailable", "observed_at": null}),
            }).collect();
            Ok(json!({"observed_at": now(), "hosts": rows}))
        }
        Request::UnitsList(params) => {
            let host = host_for(app, principal, &params.host)?;
            let snapshot = observe(app, host).await.map_err(|_| upstream_error())?;
            let scope = unit_scope(&snapshot);
            let mut units = snapshot.units;
            units.retain(|unit| {
                params
                    .state
                    .as_ref()
                    .is_none_or(|state| state == &unit.active_state)
                    && params
                        .prefix
                        .as_ref()
                        .is_none_or(|prefix| unit.unit.starts_with(prefix))
            });
            units.sort_by(|a, b| a.unit.cmp(&b.unit));
            let values = units
                .into_iter()
                .map(|unit| serde_json::to_value(unit).expect("unit JSON"))
                .collect();
            let mut result =
                client_api::page(values, params.limit, params.cursor.as_deref(), "units")?;
            result["host"] = json!(snapshot.host);
            result["observed_at"] = json!(snapshot.observed_at);
            result["unit_scope"] = scope;
            Ok(result)
        }
        Request::UnitsStatus(params) => {
            let host = host_for(app, principal, &params.host)?;
            unit_allowed(host, &params.unit)?;
            let observation: maxops_proto::UnitObservation = transport::read_json(
                host.token
                    .apply(app.client.post(format!(
                        "{}/v1/unit",
                        host.config.agent_url.trim_end_matches('/')
                    )))
                    .json(&params),
            )
            .await
            .map_err(|_| upstream_error())?;
            if observation.host != params.host
                || observation.unit.unit != params.unit
                || observation.observed_at.as_second() > now().as_second() + 30
                || now().as_second() - observation.observed_at.as_second() > 90
            {
                return Err(upstream_error());
            }
            Ok(
                json!({"host": observation.host, "observed_at": observation.observed_at, "unit": observation.unit}),
            )
        }
        Request::UnitsLogs(params) => {
            params
                .validate()
                .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
            let host = host_for(app, principal, &params.host)?;
            unit_allowed(host, &params.unit)?;
            transport::read_json(
                host.token
                    .apply(app.client.post(format!(
                        "{}/v1/logs",
                        host.config.agent_url.trim_end_matches('/')
                    )))
                    .json(&params),
            )
            .await
            .map_err(|_| upstream_error())
        }
        Request::AlertsActive(_) => {
            let url = app.alertmanager_url.as_ref().ok_or(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Alertmanager not configured",
            ))?;
            let alerts: Vec<Value> = transport::read_json(
                app.client
                    .get(format!("{}/api/v2/alerts", url.trim_end_matches('/')))
                    .query(&[
                        ("active", "true"),
                        ("silenced", "false"),
                        ("inhibited", "false"),
                    ]),
            )
            .await
            .map_err(|_| upstream_error())?;
            Ok(
                json!({"observed_at": now(), "alerts": alerts.into_iter().filter(|alert| {
                alert["labels"]["instance"].as_str().is_some_and(|name| principal.hosts.contains(name))
            }).collect::<Vec<_>>()}),
            )
        }
        Request::EventsGet(params) => {
            let event = durable_store(app)?
                .get_event(&params.event_id)
                .await
                .map_err(map_store_error)?;
            host_for(app, principal, &event.host)?;
            let value = serde_json::to_value(event).expect("stored event JSON");
            let mut fragment =
                client_api::json_fragment(&value, &params.pointer, params.offset, params.limit)?;
            fragment["event_id"] = json!(params.event_id);
            Ok(fragment)
        }
        Request::EventsRecent(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            if let Some(host) = &params.host {
                host_for(app, principal, host)?;
            }
            let events = durable_store(app)?
                .recent_events(&principal.hosts, &params)
                .await
                .map_err(map_store_error)?;
            let next = (events.len() == usize::from(params.limit))
                .then(|| events.last().map(|event| event.sequence))
                .flatten();
            Ok(
                json!({"events": events, "order": "newest_first", "since_seconds": params.since_seconds, "next_before_sequence": next}),
            )
        }
        Request::EventsList(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            if let Some(host) = &params.host {
                host_for(app, principal, host)?;
            }
            let events = durable_store(app)?
                .list_events(&principal.hosts, &params)
                .await
                .map_err(map_store_error)?;
            serde_json::to_value(events)
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "event encoding failed"))
        }
        Request::SelfStatus(_) => {
            let stats = match &app.store {
                Some(store) => Some(store.stats().await.map_err(map_store_error)?),
                None => None,
            };
            let agent_heartbeats = app
                .agent_heartbeats
                .lock()
                .expect("agent heartbeat lock poisoned");
            let executor_heartbeats = app
                .executor_heartbeats
                .lock()
                .expect("executor heartbeat lock poisoned");
            let agents: BTreeMap<_, _> = principal
                .hosts
                .iter()
                .map(|host| {
                    let observed_at = agent_heartbeats.get(host).copied();
                    let state = observed_at
                        .filter(|at| now().as_second() - at.as_second() <= 90)
                        .map_or("unobserved", |_| "observed");
                    (
                        host.clone(),
                        json!({"state":state,"observed_at":observed_at}),
                    )
                })
                .collect();
            let executors: BTreeMap<_, _> = principal
                .hosts
                .iter()
                .map(|host| {
                    let observed_at = executor_heartbeats.get(host).copied();
                    let state = observed_at
                        .filter(|at| now().as_second() - at.as_second() <= 90)
                        .map_or("unobserved", |_| "observed");
                    (
                        host.clone(),
                        json!({"state":state,"observed_at":observed_at}),
                    )
                })
                .collect();
            Ok(json!({
                "started_at": app.started_at,
                "storage": if app.store.is_some() { "ready" } else { "disabled" },
                "agents": agents,
                "executors": executors,
                "counters": {
                    "requests_total": app.requests_total.load(Ordering::Relaxed),
                    "events_ingested_total": app.events_ingested_total.load(Ordering::Relaxed),
                    "event_delivery_attempts_total": app.delivery_attempts_total.load(Ordering::Relaxed),
                    "event_delivery_failures_total": app.delivery_failures_total.load(Ordering::Relaxed),
                    "jobs_nonterminal": stats.as_ref().map_or(0, |value| value.jobs_nonterminal),
                    "jobs_queued": stats.as_ref().map_or(0, |value| value.jobs_queued),
                    "jobs_outcome_unknown": stats.as_ref().map_or(0, |value| value.jobs_outcome_unknown),
                    "jobs_completed_total": stats.as_ref().map_or(0, |value| value.jobs_completed_total),
                    "job_duration_seconds_sum": stats.as_ref().map_or(0.0, |value| value.job_duration_seconds_sum),
                    "reconciliations_total": stats.as_ref().map_or(0, |value| value.reconciliations_total),
                    "events_total": stats.as_ref().map_or(0, |value| value.events_total),
                    "deliveries_pending": stats.as_ref().map_or(0, |value| value.deliveries_pending),
                    "remediations_active": stats.as_ref().map_or(0, |value| value.remediations_active),
                    "database_size_bytes": stats.as_ref().map_or(0, |value| value.database_size_bytes),
                    "storage_available_bytes": stats.as_ref().map_or(0, |value| value.storage_available_bytes),
                }
            }))
        }
        Request::ExecRun(_)
        | Request::UnitsStart(_)
        | Request::UnitsStop(_)
        | Request::UnitsRestart(_)
        | Request::UnitsReload(_)
        | Request::JobsWait(_)
        | Request::JobsEvents(_)
        | Request::JobsResult(_)
        | Request::DeployRun(_)
        | Request::JobsList(_)
        | Request::JobsStatus(_)
        | Request::JobsLogs(_)
        | Request::JobsCancel(_)
        | Request::WorkspaceCreate(_)
        | Request::WorkspaceStatus(_)
        | Request::WorkspaceRead(_)
        | Request::WorkspaceApply(_)
        | Request::WorkspaceDiff(_)
        | Request::WorkspaceCommit(_)
        | Request::WorkspaceCheck(_)
        | Request::WorkspacePublish(_)
        | Request::DeployPrepare(_)
        | Request::DeployBuild(_)
        | Request::DeployActivate(_)
        | Request::DeployVerify(_)
        | Request::DeployRollback(_)
        | Request::ChangesStatus(_)
        | Request::ChangesHistory(_)
        | Request::DiagnosticsCollect(_)
        | Request::RemediationsBegin(_)
        | Request::RemediationsFinish(_) => Err(ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "job operation routed as observation",
        )),
    }
}

fn idempotency_key(headers: &HeaderMap) -> Result<&str, ApiError> {
    if headers.get_all("idempotency-key").iter().count() != 1 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "one Idempotency-Key header is required",
        ));
    }
    let value = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid Idempotency-Key"))?;
    if value.is_empty() || value.len() > 128 || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid Idempotency-Key"));
    }
    Ok(value)
}

fn durable_store(app: &App) -> Result<&Store, ApiError> {
    app.store.as_ref().ok_or(ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "durable job storage is disabled",
    ))
}

fn map_store_error(error: color_eyre::eyre::Report) -> ApiError {
    let message = error.to_string();
    if message.contains("event cursor expired") {
        ApiError(StatusCode::GONE, "event cursor expired")
    } else if message.contains("remediation already active")
        || message.contains("remediation attempt budget exhausted")
        || message.contains("remediation cooldown active")
        || message.contains("remediation is terminal")
    {
        ApiError(StatusCode::CONFLICT, "remediation policy conflict")
    } else if message.contains("idempotency key") {
        ApiError(
            StatusCode::CONFLICT,
            "idempotency key conflicts with another request",
        )
    } else if message.contains("revision changed") {
        ApiError(StatusCode::CONFLICT, "change revision changed")
    } else if message.contains("workflow") {
        ApiError(
            StatusCode::CONFLICT,
            "change is owned by a deployment workflow",
        )
    } else if message.contains("change not found") {
        ApiError(StatusCode::NOT_FOUND, "change not found")
    } else if message.contains("workspace not found") {
        ApiError(StatusCode::NOT_FOUND, "workspace not found")
    } else if message.contains("event not found") {
        ApiError(StatusCode::NOT_FOUND, "event not found")
    } else if message.contains("remediation not found") {
        ApiError(StatusCode::NOT_FOUND, "remediation not found")
    } else if message.contains("not found") {
        ApiError(StatusCode::NOT_FOUND, "job not found")
    } else {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "durable job storage failed",
        )
    }
}

async fn run_job_operation(
    app: &Arc<App>,
    principal: &Principal,
    headers: &HeaderMap,
    request: Request,
) -> Result<(StatusCode, Value), ApiError> {
    let store = durable_store(app)?;
    match request {
        Request::ExecRun(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let host = host_for(app, principal, &params.host)?;
            if host.execution_token.is_none() {
                return Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "host execution disabled",
                ));
            }
            let key = idempotency_key(headers)?;
            let deadline = now()
                .checked_add(std::time::Duration::from_secs(u64::from(
                    params.timeout_seconds.unwrap_or(300),
                )))
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid job deadline"))?;
            let job = NewJob {
                principal: principal.name.clone(),
                host: params.host.clone(),
                operation: "exec.run".into(),
                spec_version: 1,
                spec: serde_json::to_value(params).expect("serializable command request"),
                policy_version: "hub-config-v1".into(),
                deadline: Some(deadline),
            };
            let submitted = store.submit_job(key, &job).await.map_err(map_store_error)?;
            if submitted.created || submitted.job.handle.state == JobState::Queued {
                spawn_dispatch(app.clone(), submitted.job.handle.job_id.clone());
            }
            Ok((
                StatusCode::ACCEPTED,
                serde_json::to_value(submitted.job.handle).expect("serializable job handle"),
            ))
        }
        Request::UnitsStart(params) => {
            submit_unit_action(app, principal, headers, "units.start", params).await
        }
        Request::UnitsStop(params) => {
            submit_unit_action(app, principal, headers, "units.stop", params).await
        }
        Request::UnitsRestart(params) => {
            submit_unit_action(app, principal, headers, "units.restart", params).await
        }
        Request::UnitsReload(params) => {
            submit_unit_action(app, principal, headers, "units.reload", params).await
        }
        Request::WorkspaceCreate(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let repository = params.repository.clone();
            submit_workspace_job(
                app,
                principal,
                headers,
                "workspace.create",
                &repository,
                serde_json::to_value(params).expect("serializable workspace create"),
                300,
            )
            .await
        }
        Request::WorkspaceCheck(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let repository = params.repository.clone();
            submit_workspace_job(
                app,
                principal,
                headers,
                "workspace.check",
                &repository,
                serde_json::to_value(params).expect("serializable workspace check"),
                3600,
            )
            .await
        }
        Request::WorkspacePublish(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let repository = params.repository.clone();
            submit_workspace_job(
                app,
                principal,
                headers,
                "workspace.publish",
                &repository,
                serde_json::to_value(params).expect("serializable workspace publish"),
                300,
            )
            .await
        }
        Request::DeployPrepare(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            prepare_change(app, principal, headers, params).await
        }
        Request::DeployBuild(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            submit_change_stage(app, principal, headers, params, DeploymentAction::Build).await
        }
        Request::DeployActivate(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            submit_change_stage(app, principal, headers, params, DeploymentAction::Activate).await
        }
        Request::DeployVerify(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            submit_change_stage(app, principal, headers, params, DeploymentAction::Verify).await
        }
        Request::DeployRollback(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            submit_change_stage(app, principal, headers, params, DeploymentAction::Rollback).await
        }
        Request::ChangesStatus(params) => {
            let change = refresh_change(app, principal, &params.change_id).await?;
            Ok((
                StatusCode::OK,
                serde_json::to_value(change).expect("serializable change"),
            ))
        }
        Request::ChangesHistory(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            if let Some(host) = &params.host {
                host_for(app, principal, host)?;
            }
            let mut changes = store
                .list_visible_changes(
                    &principal.name,
                    &principal.hosts,
                    &principal.deployments,
                    &params,
                )
                .await
                .map_err(map_store_error)?;
            let more = changes.len() > usize::from(params.limit);
            changes.truncate(usize::from(params.limit));
            let next_cursor = if more {
                changes.last().map(|change| change.plan.change_id.clone())
            } else {
                None
            };
            Ok((
                StatusCode::OK,
                serde_json::to_value(ChangeHistoryResponse {
                    changes,
                    next_cursor,
                })
                .expect("serializable change history"),
            ))
        }
        Request::WorkspaceStatus(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let repository = params.repository.clone();
            let response = forward_workspace(
                app,
                principal,
                &repository,
                WorkspaceTargetRequest::Status(params),
            )
            .await?;
            let WorkspaceTargetResponse::Record(record) = response else {
                return Err(ApiError(
                    StatusCode::BAD_GATEWAY,
                    "invalid executor response",
                ));
            };
            Ok((
                StatusCode::OK,
                serde_json::to_value(record).expect("serializable workspace"),
            ))
        }
        Request::WorkspaceRead(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let repository = params.repository.clone();
            let response = forward_workspace(
                app,
                principal,
                &repository,
                WorkspaceTargetRequest::Read(params),
            )
            .await?;
            let WorkspaceTargetResponse::File(file) = response else {
                return Err(ApiError(
                    StatusCode::BAD_GATEWAY,
                    "invalid executor response",
                ));
            };
            Ok((
                StatusCode::OK,
                serde_json::to_value(file).expect("serializable workspace file"),
            ))
        }
        Request::WorkspaceApply(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let repository = params.repository.clone();
            let response = forward_workspace(
                app,
                principal,
                &repository,
                WorkspaceTargetRequest::Apply(params),
            )
            .await?;
            let WorkspaceTargetResponse::Record(record) = response else {
                return Err(ApiError(
                    StatusCode::BAD_GATEWAY,
                    "invalid executor response",
                ));
            };
            Ok((
                StatusCode::OK,
                serde_json::to_value(record).expect("serializable workspace"),
            ))
        }
        Request::WorkspaceDiff(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let repository = params.repository.clone();
            let response = forward_workspace(
                app,
                principal,
                &repository,
                WorkspaceTargetRequest::Diff(params),
            )
            .await?;
            let WorkspaceTargetResponse::Diff(diff) = response else {
                return Err(ApiError(
                    StatusCode::BAD_GATEWAY,
                    "invalid executor response",
                ));
            };
            Ok((
                StatusCode::OK,
                serde_json::to_value(diff).expect("serializable workspace diff"),
            ))
        }
        Request::WorkspaceCommit(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let repository = params.repository.clone();
            let response = forward_workspace(
                app,
                principal,
                &repository,
                WorkspaceTargetRequest::Commit(params),
            )
            .await?;
            let WorkspaceTargetResponse::Record(record) = response else {
                return Err(ApiError(
                    StatusCode::BAD_GATEWAY,
                    "invalid executor response",
                ));
            };
            Ok((
                StatusCode::OK,
                serde_json::to_value(record).expect("serializable workspace"),
            ))
        }
        Request::DiagnosticsCollect(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let host = host_for(app, principal, &params.host)?;
            if let Some(unit) = &params.unit {
                unit_allowed(host, unit)?;
            }
            if params
                .probes
                .iter()
                .any(|probe| !host.config.diagnostic_probes.contains_key(probe))
            {
                return Err(ApiError(
                    StatusCode::FORBIDDEN,
                    "diagnostic probe not permitted",
                ));
            }
            if !params.probes.is_empty() && host.execution_token.is_none() {
                return Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "host execution disabled",
                ));
            }
            if let Some(event_id) = &params.event_id {
                store
                    .event_for_host(event_id, &params.host)
                    .await
                    .map_err(map_store_error)?;
            }
            let key = idempotency_key(headers)?;
            let deadline = now()
                .checked_add(Duration::from_secs(300))
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid job deadline"))?;
            let job = NewJob {
                principal: principal.name.clone(),
                host: params.host.clone(),
                operation: "diagnostics.collect".into(),
                spec_version: 1,
                spec: serde_json::to_value(params).expect("serializable diagnostic request"),
                policy_version: "hub-config-v1".into(),
                deadline: Some(deadline),
            };
            let submitted = store.submit_job(key, &job).await.map_err(map_store_error)?;
            if submitted.created || submitted.job.handle.state == JobState::Queued {
                spawn_diagnostics(app.clone(), submitted.job.clone());
            }
            Ok((
                StatusCode::ACCEPTED,
                serde_json::to_value(submitted.job.handle).expect("serializable job handle"),
            ))
        }
        Request::RemediationsBegin(params) => {
            let host = host_for(app, principal, &params.host)?;
            if host.execution_token.is_none() {
                return Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "host execution disabled",
                ));
            }
            store
                .event_for_host(&params.event_id, &params.host)
                .await
                .map_err(map_store_error)?;
            let key = idempotency_key(headers)?;
            let deadline = now()
                .checked_add(Duration::from_secs(60))
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid job deadline"))?;
            let job = NewJob {
                principal: principal.name.clone(),
                host: params.host.clone(),
                operation: "remediations.begin".into(),
                spec_version: 1,
                spec: serde_json::to_value(params).expect("serializable remediation claim"),
                policy_version: "hub-config-v1".into(),
                deadline: Some(deadline),
            };
            let submitted = store.submit_job(key, &job).await.map_err(map_store_error)?;
            if submitted.created || submitted.job.handle.state == JobState::Queued {
                spawn_remediation_begin(app.clone(), submitted.job.clone());
            }
            Ok((
                StatusCode::ACCEPTED,
                serde_json::to_value(submitted.job.handle).expect("serializable job handle"),
            ))
        }
        Request::RemediationsFinish(params) => {
            params
                .validate()
                .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
            let remediation = store
                .get_remediation(&params.remediation_id, &principal.name)
                .await
                .map_err(map_store_error)?;
            host_for(app, principal, &remediation.host)?;
            if let Some(job_id) = &params.related_job_id {
                let related = store
                    .get_owned_job(&principal.name, job_id)
                    .await
                    .map_err(map_store_error)?;
                if related.handle.host != remediation.host {
                    return Err(ApiError(StatusCode::FORBIDDEN, "related job not permitted"));
                }
            }
            if let Some(change_id) = &params.related_change_id {
                let related = store
                    .get_owned_change(&principal.name, change_id)
                    .await
                    .map_err(map_store_error)?;
                if related.plan.target_host != remediation.host {
                    return Err(ApiError(
                        StatusCode::FORBIDDEN,
                        "related change not permitted",
                    ));
                }
            }
            let updated = store
                .finish_remediation(
                    &params.remediation_id,
                    &principal.name,
                    RemediationCompletion {
                        expected_revision: params.expected_revision,
                        outcome: params.outcome,
                        related_job_id: params.related_job_id.as_ref(),
                        related_change_id: params.related_change_id.as_ref(),
                        summary: &params.summary,
                    },
                )
                .await
                .map_err(map_store_error)?;
            app.events_ingested_total.fetch_add(1, Ordering::Relaxed);
            Ok((
                StatusCode::OK,
                serde_json::to_value(updated).expect("serializable remediation"),
            ))
        }
        Request::JobsWait(params) => Ok((
            StatusCode::OK,
            client_api::wait(app, principal, params).await?,
        )),
        Request::JobsEvents(params) => Ok((
            StatusCode::OK,
            client_api::events(app, principal, params).await?,
        )),
        Request::JobsResult(params) => Ok((
            StatusCode::OK,
            client_api::result(app, principal, params).await?,
        )),
        Request::DeployRun(params) => {
            deployment_workflow::submit(app, principal, headers, params).await
        }
        Request::JobsList(params) => {
            if let Some(host) = &params.host {
                host_for(app, principal, host)?;
            }
            if !(1..=200).contains(&params.limit) {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "job list limit must be 1..200",
                ));
            }
            let mut jobs = store
                .list_visible_jobs(&principal.name, &principal.hosts, &params)
                .await
                .map_err(map_store_error)?;
            let more = jobs.len() > usize::from(params.limit);
            jobs.truncate(usize::from(params.limit));
            let next_cursor = if more {
                jobs.last().map(|job| job.handle.job_id.clone())
            } else {
                None
            };
            Ok((
                StatusCode::OK,
                serde_json::to_value(JobsListResponse { jobs, next_cursor })
                    .expect("serializable jobs"),
            ))
        }
        Request::JobsStatus(params) => {
            let mut job = store
                .get_owned_job(&principal.name, &params.job_id)
                .await
                .map_err(map_store_error)?;
            host_for(app, principal, &job.handle.host)?;
            job = deployment_workflow::reconcile(app, principal, job).await?;
            if !deployment_workflow::is_local(&job.handle.operation)
                && (!job.handle.state.is_terminal() || job.handle.state == JobState::OutcomeUnknown)
                && let Ok(ExecutorResponse::Job(target)) = agent_request(
                    app,
                    &job.handle.host,
                    &ExecutorRequest::Status(JobIdParams {
                        job_id: params.job_id.clone(),
                    }),
                )
                .await
            {
                job = project_target_job(store, job, target)
                    .await
                    .map_err(map_store_error)?;
            }
            Ok((
                StatusCode::OK,
                serde_json::to_value(job).expect("serializable job"),
            ))
        }
        Request::JobsLogs(params) => {
            let job = store
                .get_owned_job(&principal.name, &params.job_id)
                .await
                .map_err(map_store_error)?;
            host_for(app, principal, &job.handle.host)?;
            let response = agent_request(app, &job.handle.host, &ExecutorRequest::Logs(params))
                .await
                .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "executor unavailable"))?;
            let ExecutorResponse::Logs(logs) = response else {
                return Err(ApiError(
                    StatusCode::BAD_GATEWAY,
                    "invalid executor response",
                ));
            };
            Ok((
                StatusCode::OK,
                serde_json::to_value(logs).expect("serializable logs"),
            ))
        }
        Request::JobsCancel(params) => {
            if params.reason.is_empty() || params.reason.len() > 1024 {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "cancellation reason must contain 1..1024 bytes",
                ));
            }
            let job = store
                .get_owned_job(&principal.name, &params.job_id)
                .await
                .map_err(map_store_error)?;
            host_for(app, principal, &job.handle.host)?;
            if job.handle.revision != params.expected_revision {
                return Err(ApiError(StatusCode::CONFLICT, "job revision changed"));
            }
            if deployment_workflow::is_local(&job.handle.operation) {
                let requested = store
                    .request_cancel(&params.job_id, job.handle.revision, &params.reason)
                    .await
                    .map_err(map_store_error)?;
                return Ok((
                    StatusCode::OK,
                    serde_json::to_value(requested).expect("job"),
                ));
            }
            let target = agent_request(
                app,
                &job.handle.host,
                &ExecutorRequest::Status(JobIdParams {
                    job_id: params.job_id.clone(),
                }),
            )
            .await
            .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "executor unavailable"))?;
            let ExecutorResponse::Job(target) = target else {
                return Err(ApiError(
                    StatusCode::BAD_GATEWAY,
                    "invalid executor response",
                ));
            };
            let response = agent_request(
                app,
                &job.handle.host,
                &ExecutorRequest::Cancel(JobCancelParams {
                    job_id: params.job_id.clone(),
                    expected_revision: target.handle.revision,
                    reason: params.reason.clone(),
                }),
            )
            .await
            .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "executor unavailable"))?;
            let ExecutorResponse::Job(target) = response else {
                return Err(ApiError(
                    StatusCode::BAD_GATEWAY,
                    "invalid executor response",
                ));
            };
            let requested = store
                .request_cancel(&params.job_id, job.handle.revision, &params.reason)
                .await
                .map_err(map_store_error)?;
            let updated = project_target_job(store, requested, target)
                .await
                .map_err(map_store_error)?;
            Ok((
                StatusCode::OK,
                serde_json::to_value(updated).expect("serializable job"),
            ))
        }
        _ => Err(ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "observation routed as job operation",
        )),
    }
}

async fn prepare_change(
    app: &Arc<App>,
    principal: &Principal,
    headers: &HeaderMap,
    params: DeployPrepareParams,
) -> Result<(StatusCode, Value), ApiError> {
    let key = idempotency_key(headers)?;
    let store = durable_store(app)?;
    if let Some(existing) = store
        .get_idempotent_job(&principal.name, key)
        .await
        .map_err(map_store_error)?
    {
        let spec: DeploymentJobSpec =
            serde_json::from_value(existing.spec.clone()).map_err(|_| {
                ApiError(
                    StatusCode::CONFLICT,
                    "idempotency key belongs to another request",
                )
            })?;
        if existing.handle.operation != "deploy.prepare"
            || spec.plan.repository != params.repository
            || spec.plan.workspace_id != params.workspace_id
            || spec.plan.workspace_revision != params.expected_revision
            || spec.plan.target_host != params.target_host
            || spec.plan.deployment_profile != params.profile
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "idempotency key belongs to another request",
            ));
        }
        if existing.handle.state == JobState::Queued {
            spawn_dispatch(app.clone(), existing.handle.job_id.clone());
        }
        return Ok((
            StatusCode::ACCEPTED,
            serde_json::to_value(existing.handle).expect("serializable job handle"),
        ));
    }

    let deployment = deployment_for(app, principal, &params.profile)?;
    if deployment.repository != params.repository || deployment.target_host != params.target_host {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "deployment request does not match profile",
        ));
    }
    let workspace = match forward_workspace(
        app,
        principal,
        &params.repository,
        WorkspaceTargetRequest::Status(WorkspaceStatusParams {
            repository: params.repository.clone(),
            workspace_id: params.workspace_id.clone(),
        }),
    )
    .await?
    {
        WorkspaceTargetResponse::Record(workspace) => workspace,
        _ => {
            return Err(ApiError(
                StatusCode::BAD_GATEWAY,
                "invalid executor response",
            ));
        }
    };
    if workspace.revision != params.expected_revision {
        return Err(ApiError(StatusCode::CONFLICT, "workspace revision changed"));
    }
    let source_commit = workspace
        .commit_hash
        .clone()
        .ok_or(ApiError(StatusCode::CONFLICT, "workspace is not committed"))?;
    let workspace_projection = workspace.clone();
    let source = repository_head(
        app,
        principal,
        &deployment.repository,
        &deployment.source_reference,
    )
    .await?;
    let runtime = runtime_state(app, principal, deployment).await?;
    let created_at = now();
    let expires_at = created_at
        .checked_add(std::time::Duration::from_secs(u64::from(
            deployment.plan_ttl_seconds,
        )))
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid deployment plan lifetime"))?;
    let job_id = JobId::parse(uuid::Uuid::now_v7().to_string()).map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not create change ID",
        )
    })?;
    let change_id = ChangeId::parse(job_id.as_str().to_owned()).map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not create change ID",
        )
    })?;
    let plan = ChangePlan {
        change_id,
        repository: deployment.repository.clone(),
        workspace_id: workspace.workspace_id,
        workspace_revision: workspace.revision,
        tree_hash: workspace.tree_hash,
        source_commit,
        source_reference: deployment.source_reference.clone(),
        source_remote_head: source.commit.clone(),
        target_host: deployment.target_host.clone(),
        deployment_profile: deployment.name.clone(),
        kind: deployment.kind,
        flake_attribute: deployment.flake_attribute.clone(),
        drv_path: None,
        lock_digest: None,
        source_baseline: SourceBaseline {
            repository_id: deployment.repository.clone(),
            reference: deployment.source_reference.clone(),
            commit: Some(source.commit),
            observed_at: source.observed_at,
            evidence: "configured Git remote fetched by repository executor".into(),
        },
        runtime_baseline: RuntimeBaseline {
            host: runtime.host,
            profile: runtime.deployment_profile,
            running_closure: runtime.running_closure,
            persistent_profile: runtime.persistent_profile,
            generation: runtime.generation,
            boot_id: runtime.boot_id,
            source_commit: None,
            observed_at: runtime.observed_at,
            evidence: "target executor read configured running and persistent profile paths".into(),
        },
        policy_version: "hub-config-v1".into(),
        created_at,
        expires_at,
    };
    let job = NewJob {
        principal: principal.name.clone(),
        host: deployment.builder_host.clone(),
        operation: "deploy.prepare".into(),
        spec_version: 1,
        spec: serde_json::to_value(DeploymentJobSpec {
            action: DeploymentAction::Prepare,
            plan: plan.clone(),
            artifact: None,
        })
        .expect("serializable deployment job"),
        policy_version: plan.policy_version.clone(),
        deadline: Some(
            now()
                .checked_add(std::time::Duration::from_secs(900))
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid job deadline"))?,
        ),
    };
    let submitted = store
        .submit_job_with_id(key, &job_id, &job)
        .await
        .map_err(map_store_error)?;
    if submitted.created {
        store
            .record_workspace(&workspace_projection)
            .await
            .map_err(map_store_error)?;
        store
            .create_change(&plan, &principal.name, &job_id)
            .await
            .map_err(map_store_error)?;
    }
    if submitted.created || submitted.job.handle.state == JobState::Queued {
        spawn_dispatch(app.clone(), submitted.job.handle.job_id.clone());
    }
    Ok((
        StatusCode::ACCEPTED,
        serde_json::to_value(submitted.job.handle).expect("serializable job handle"),
    ))
}

async fn submit_change_stage(
    app: &Arc<App>,
    principal: &Principal,
    headers: &HeaderMap,
    params: DeployChangeParams,
    action: DeploymentAction,
) -> Result<(StatusCode, Value), ApiError> {
    submit_change_stage_owned(app, principal, headers, params, action, None).await
}

async fn submit_change_stage_owned(
    app: &Arc<App>,
    principal: &Principal,
    headers: &HeaderMap,
    params: DeployChangeParams,
    action: DeploymentAction,
    workflow: Option<&JobId>,
) -> Result<(StatusCode, Value), ApiError> {
    let key = idempotency_key(headers)?;
    if let Some(existing) = durable_store(app)?
        .get_idempotent_job(&principal.name, key)
        .await
        .map_err(map_store_error)?
    {
        let spec: DeploymentJobSpec =
            serde_json::from_value(existing.spec.clone()).map_err(|_| {
                ApiError(
                    StatusCode::CONFLICT,
                    "idempotency key belongs to another request",
                )
            })?;
        if existing.handle.operation != operation_for_deployment(action)
            || spec.action != action
            || spec.plan.change_id != params.change_id
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "idempotency key belongs to another request",
            ));
        }
        if existing.handle.state == JobState::Queued {
            spawn_dispatch(app.clone(), existing.handle.job_id.clone());
        }
        return Ok((
            StatusCode::ACCEPTED,
            serde_json::to_value(existing.handle).expect("serializable job handle"),
        ));
    }
    let mut change = refresh_change(app, principal, &params.change_id).await?;
    if change.revision != params.expected_revision {
        return Err(ApiError(StatusCode::CONFLICT, "change revision changed"));
    }
    let deployment = deployment_for(app, principal, &change.plan.deployment_profile)?;
    let (required, next, operation, host, timeout) = match action {
        DeploymentAction::Build => (
            &[ChangeState::Prepared][..],
            ChangeState::Building,
            "deploy.build",
            deployment.builder_host.clone(),
            7200,
        ),
        DeploymentAction::Activate => (
            &[ChangeState::Ready][..],
            ChangeState::Activating,
            "deploy.activate",
            deployment.target_host.clone(),
            900,
        ),
        DeploymentAction::Verify => (
            &[ChangeState::Verifying][..],
            ChangeState::Verifying,
            "deploy.verify",
            deployment.target_host.clone(),
            900,
        ),
        DeploymentAction::Rollback => (
            &[ChangeState::Verifying, ChangeState::Succeeded][..],
            ChangeState::Recovering,
            "deploy.rollback",
            deployment.target_host.clone(),
            900,
        ),
        DeploymentAction::Prepare => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "invalid deployment stage",
            ));
        }
    };
    if !required.contains(&change.state) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "change is not ready for this stage",
        ));
    }
    if change.plan.expires_at.as_second() < now().as_second()
        && matches!(action, DeploymentAction::Activate)
    {
        change = transition_change_state(
            durable_store(app)?,
            &change,
            ChangeState::Stale,
            Some("deployment plan expired"),
        )
        .await?;
        let _ = change;
        return Err(ApiError(StatusCode::CONFLICT, "deployment plan expired"));
    }
    if action == DeploymentAction::Activate {
        let source = repository_head(
            app,
            principal,
            &change.plan.repository,
            &change.plan.source_reference,
        )
        .await?;
        let runtime = runtime_state(app, principal, deployment).await?;
        if source.commit != change.plan.source_remote_head
            || !runtime_matches_plan(&runtime, &change.plan)
        {
            transition_change_state(
                durable_store(app)?,
                &change,
                ChangeState::Stale,
                Some("source or runtime baseline changed before activation"),
            )
            .await?;
            return Err(ApiError(
                StatusCode::CONFLICT,
                "deployment baseline changed",
            ));
        }
    }
    let job = NewJob {
        principal: principal.name.clone(),
        host,
        operation: operation.into(),
        spec_version: 1,
        spec: serde_json::to_value(DeploymentJobSpec {
            action,
            plan: change.plan.clone(),
            artifact: change.artifact.clone(),
        })
        .expect("serializable deployment job"),
        policy_version: change.plan.policy_version.clone(),
        deadline: Some(
            now()
                .checked_add(std::time::Duration::from_secs(timeout))
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid job deadline"))?,
        ),
    };
    let id = workflow.map_or_else(
        || JobId::parse(uuid::Uuid::now_v7().to_string()).expect("UUID"),
        |parent| stable_child_job_id(parent, operation),
    );
    let submitted = durable_store(app)?
        .submit_linked_job(
            key,
            &id,
            &job,
            Some(maxops_store::ChangeJobLink {
                change: &change,
                next,
                action: Some(action),
                workflow,
            }),
        )
        .await
        .map_err(map_store_error)?;
    if submitted.created || submitted.job.handle.state == JobState::Queued {
        spawn_dispatch(app.clone(), submitted.job.handle.job_id.clone());
    }
    Ok((
        StatusCode::ACCEPTED,
        serde_json::to_value(submitted.job.handle).expect("serializable job handle"),
    ))
}

async fn refresh_change(
    app: &Arc<App>,
    principal: &Principal,
    id: &ChangeId,
) -> Result<ChangeRecord, ApiError> {
    let store = durable_store(app)?;
    let mut change = store
        .get_owned_change(&principal.name, id)
        .await
        .map_err(map_store_error)?;
    deployment_for(app, principal, &change.plan.deployment_profile)?;
    let stage = match change.state {
        ChangeState::Checking => change.jobs.prepare.clone(),
        ChangeState::Building | ChangeState::Publishing => change.jobs.build.clone(),
        ChangeState::Activating => change.jobs.activate.clone(),
        ChangeState::Verifying => change.jobs.verify.clone(),
        ChangeState::Recovering => change.jobs.rollback.clone(),
        _ => None,
    };
    let Some(job_id) = stage else {
        return Ok(change);
    };
    let mut job = store
        .get_owned_job(&principal.name, &job_id)
        .await
        .map_err(map_store_error)?;
    if (!job.handle.state.is_terminal() || job.handle.state == JobState::OutcomeUnknown)
        && let Ok(ExecutorResponse::Job(target)) = agent_request(
            app,
            &job.handle.host,
            &ExecutorRequest::Status(JobIdParams {
                job_id: job_id.clone(),
            }),
        )
        .await
    {
        job = project_target_job(store, job, target)
            .await
            .map_err(map_store_error)?;
    }
    if !job.handle.state.is_terminal() {
        return Ok(change);
    }
    let report = job
        .result
        .as_ref()
        .and_then(|value| value.get("deployment"))
        .filter(|value| !value.is_null())
        .and_then(|value| serde_json::from_value::<DeploymentReport>(value.clone()).ok());
    let expected_action = match change.state {
        ChangeState::Checking => DeploymentAction::Prepare,
        ChangeState::Building | ChangeState::Publishing => DeploymentAction::Build,
        ChangeState::Activating => DeploymentAction::Activate,
        ChangeState::Verifying => DeploymentAction::Verify,
        ChangeState::Recovering => DeploymentAction::Rollback,
        _ => return Ok(change),
    };
    let Some(report) = report.filter(|report| report.action == expected_action) else {
        let next = if job.handle.state == JobState::OutcomeUnknown {
            ChangeState::OutcomeUnknown
        } else if change.state == ChangeState::Recovering {
            ChangeState::RecoveryFailed
        } else {
            ChangeState::Failed
        };
        return transition_change_state(
            store,
            &change,
            next,
            Some("stage ended without a deployment report"),
        )
        .await;
    };
    let next = match report.status {
        DeploymentReportStatus::Prepared => {
            change.plan.drv_path = report.drv_path.clone();
            change.plan.lock_digest = report.lock_digest.clone();
            if change.plan.drv_path.is_none() || change.plan.lock_digest.is_none() {
                ChangeState::Failed
            } else {
                ChangeState::Prepared
            }
        }
        DeploymentReportStatus::Built => {
            let out_path = report.out_path.clone();
            match (
                out_path,
                change.plan.drv_path.clone(),
                change.plan.lock_digest.clone(),
            ) {
                (Some(out_path), Some(drv_path), Some(lock_digest)) => {
                    change.artifact = Some(DeploymentArtifact {
                        builder_host: job.handle.host.clone(),
                        source_commit: change.plan.source_commit.clone(),
                        tree_hash: change.plan.tree_hash.clone(),
                        lock_digest,
                        drv_path,
                        out_path,
                        built_at: report.completed_at,
                    });
                    ChangeState::Ready
                }
                _ => ChangeState::Failed,
            }
        }
        DeploymentReportStatus::Activated => ChangeState::Verifying,
        DeploymentReportStatus::Verified => ChangeState::Succeeded,
        DeploymentReportStatus::RolledBack => ChangeState::RolledBack,
        DeploymentReportStatus::Stale => ChangeState::Stale,
        DeploymentReportStatus::Superseded => ChangeState::Superseded,
        DeploymentReportStatus::RecoveryFailed => ChangeState::RecoveryFailed,
        DeploymentReportStatus::OutcomeUnknown => ChangeState::OutcomeUnknown,
        DeploymentReportStatus::Failed => {
            if change.state == ChangeState::Recovering {
                ChangeState::RecoveryFailed
            } else {
                ChangeState::Failed
            }
        }
    };
    let recovery = matches!(
        report.status,
        DeploymentReportStatus::RolledBack
            | DeploymentReportStatus::RecoveryFailed
            | DeploymentReportStatus::Superseded
    )
    .then_some(report.detail.as_str());
    store
        .transition_change(
            &change.plan.change_id,
            &principal.name,
            ChangeTransition {
                expected_revision: change.revision,
                next,
                plan: &change.plan,
                artifact: change.artifact.as_ref(),
                jobs: &change.jobs,
                recovery_state: recovery.or(change.recovery_state.as_deref()),
            },
        )
        .await
        .or_else(|error| {
            if error.to_string().contains("revision changed") {
                Ok(change.clone())
            } else {
                Err(error)
            }
        })
        .map_err(map_store_error)
}

async fn transition_change_state(
    store: &Store,
    change: &ChangeRecord,
    next: ChangeState,
    recovery: Option<&str>,
) -> Result<ChangeRecord, ApiError> {
    store
        .transition_change(
            &change.plan.change_id,
            &change.creator,
            ChangeTransition {
                expected_revision: change.revision,
                next,
                plan: &change.plan,
                artifact: change.artifact.as_ref(),
                jobs: &change.jobs,
                recovery_state: recovery.or(change.recovery_state.as_deref()),
            },
        )
        .await
        .map_err(map_store_error)
}

fn runtime_matches_plan(runtime: &RuntimeStateResponse, plan: &ChangePlan) -> bool {
    runtime.host == plan.runtime_baseline.host
        && runtime.deployment_profile == plan.runtime_baseline.profile
        && runtime.running_closure == plan.runtime_baseline.running_closure
        && runtime.persistent_profile == plan.runtime_baseline.persistent_profile
        && runtime.generation == plan.runtime_baseline.generation
        && runtime.boot_id == plan.runtime_baseline.boot_id
}

fn operation_for_deployment(action: DeploymentAction) -> &'static str {
    match action {
        DeploymentAction::Prepare => "deploy.prepare",
        DeploymentAction::Build => "deploy.build",
        DeploymentAction::Activate => "deploy.activate",
        DeploymentAction::Verify => "deploy.verify",
        DeploymentAction::Rollback => "deploy.rollback",
    }
}

async fn submit_unit_action(
    app: &Arc<App>,
    principal: &Principal,
    headers: &HeaderMap,
    operation: &str,
    params: UnitActionParams,
) -> Result<(StatusCode, Value), ApiError> {
    params
        .validate()
        .map_err(|message| ApiError(StatusCode::BAD_REQUEST, message))?;
    let host = host_for(app, principal, &params.host)?;
    unit_manageable(host, &params.unit)?;
    if host.execution_token.is_none() {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "host execution disabled",
        ));
    }
    let key = idempotency_key(headers)?;
    let deadline = now()
        .checked_add(std::time::Duration::from_secs(60))
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid job deadline"))?;
    let job = NewJob {
        principal: principal.name.clone(),
        host: params.host.clone(),
        operation: operation.into(),
        spec_version: 1,
        spec: serde_json::to_value(params).expect("serializable service action"),
        policy_version: "hub-config-v1".into(),
        deadline: Some(deadline),
    };
    let submitted = durable_store(app)?
        .submit_job(key, &job)
        .await
        .map_err(map_store_error)?;
    if submitted.created || submitted.job.handle.state == JobState::Queued {
        spawn_dispatch(app.clone(), submitted.job.handle.job_id.clone());
    }
    Ok((
        StatusCode::ACCEPTED,
        serde_json::to_value(submitted.job.handle).expect("serializable job handle"),
    ))
}

async fn submit_workspace_job(
    app: &Arc<App>,
    principal: &Principal,
    headers: &HeaderMap,
    operation: &str,
    repository: &str,
    spec: Value,
    timeout_seconds: u64,
) -> Result<(StatusCode, Value), ApiError> {
    let host = repository_host(app, principal, repository)?.to_owned();
    let key = idempotency_key(headers)?;
    let deadline = now()
        .checked_add(std::time::Duration::from_secs(timeout_seconds))
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid job deadline"))?;
    let job = NewJob {
        principal: principal.name.clone(),
        host,
        operation: operation.into(),
        spec_version: 1,
        spec,
        policy_version: "hub-config-v1".into(),
        deadline: Some(deadline),
    };
    let submitted = durable_store(app)?
        .submit_job(key, &job)
        .await
        .map_err(map_store_error)?;
    if submitted.created || submitted.job.handle.state == JobState::Queued {
        spawn_dispatch(app.clone(), submitted.job.handle.job_id.clone());
    }
    Ok((
        StatusCode::ACCEPTED,
        serde_json::to_value(submitted.job.handle).expect("serializable job handle"),
    ))
}

async fn forward_workspace(
    app: &App,
    principal: &Principal,
    repository: &str,
    request: WorkspaceTargetRequest,
) -> Result<WorkspaceTargetResponse, ApiError> {
    let host = repository_host(app, principal, repository)?;
    let response = agent_request(
        app,
        host,
        &ExecutorRequest::Workspace {
            principal: principal.name.clone(),
            request,
        },
    )
    .await
    .map_err(map_workspace_upstream_error)?;
    let ExecutorResponse::Workspace(response) = response else {
        return Err(ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid executor response",
        ));
    };
    Ok(response)
}

fn map_workspace_upstream_error(error: color_eyre::Report) -> ApiError {
    match transport::upstream_status(&error) {
        Some(StatusCode::CONFLICT) => ApiError(StatusCode::CONFLICT, "workspace revision changed"),
        Some(StatusCode::NOT_FOUND) => ApiError(StatusCode::NOT_FOUND, "workspace not found"),
        Some(StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY) => ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "workspace request rejected",
        ),
        Some(StatusCode::FORBIDDEN) => ApiError(StatusCode::FORBIDDEN, "workspace not permitted"),
        _ => ApiError(StatusCode::SERVICE_UNAVAILABLE, "executor unavailable"),
    }
}

async fn agent_request(
    app: &App,
    host_name: &str,
    request: &ExecutorRequest,
) -> color_eyre::eyre::Result<ExecutorResponse> {
    let host = app
        .hosts
        .get(host_name)
        .ok_or_else(|| color_eyre::eyre::eyre!("host missing"))?;
    let token = host
        .execution_token
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("execution disabled"))?;
    let response = transport::read_json(
        token
            .apply(app.client.post(format!(
                "{}/v1/manage",
                host.config.agent_url.trim_end_matches('/')
            )))
            .json(request),
    )
    .await?;
    app.executor_heartbeats
        .lock()
        .expect("executor heartbeat lock poisoned")
        .insert(host_name.to_owned(), now());
    Ok(response)
}

async fn project_target_job(
    store: &Store,
    mut current: JobRecord,
    target: JobRecord,
) -> color_eyre::eyre::Result<JobRecord> {
    color_eyre::eyre::ensure!(
        current.handle.job_id == target.handle.job_id
            && current.handle.host == target.handle.host
            && current.spec_hash == target.spec_hash,
        "executor job identity mismatch"
    );
    if current.handle.state == target.handle.state {
        return Ok(current);
    }
    if target.handle.state == JobState::OutcomeUnknown
        && !current
            .handle
            .state
            .can_transition_to(JobState::OutcomeUnknown)
        && current
            .handle
            .state
            .can_transition_to(JobState::Reconciling)
    {
        current = store
            .transition_job(
                &current.handle.job_id,
                current.handle.revision,
                JobState::Reconciling,
                &json!({"source":"executor","target_revision":target.handle.revision}),
                target.result.as_ref(),
            )
            .await?;
    }
    if !current.handle.state.can_transition_to(target.handle.state) {
        return Ok(current);
    }
    store
        .transition_job(
            &current.handle.job_id,
            current.handle.revision,
            target.handle.state,
            &json!({"source":"executor","target_revision":target.handle.revision}),
            target.result.as_ref(),
        )
        .await
}

fn spawn_dispatch(app: Arc<App>, id: JobId) {
    tokio::spawn(async move {
        if let Err(error) = dispatch(app, id.clone()).await {
            tracing::warn!(job_id = %id, %error, "job dispatch or reconciliation stopped");
        }
    });
}

fn spawn_diagnostics(app: Arc<App>, job: JobRecord) {
    let id = job.handle.job_id.clone();
    if !app
        .local_workers
        .lock()
        .expect("local worker lock poisoned")
        .insert(id.clone())
    {
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = run_diagnostics(app.clone(), job).await {
            fail_local_job(&app, &id, "diagnostic_failed").await;
            tracing::warn!(job_id = %id, %error, "diagnostic collection failed");
        }
        app.local_workers
            .lock()
            .expect("local worker lock poisoned")
            .remove(&id);
    });
}

fn spawn_remediation_begin(app: Arc<App>, job: JobRecord) {
    let id = job.handle.job_id.clone();
    if !app
        .local_workers
        .lock()
        .expect("local worker lock poisoned")
        .insert(id.clone())
    {
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = run_remediation_begin(app.clone(), job).await {
            let code = if error.to_string().contains("attempt budget") {
                "remediation_budget_exhausted"
            } else if error.to_string().contains("cooldown") {
                "remediation_cooldown_active"
            } else if error.to_string().contains("already active") {
                "remediation_already_active"
            } else {
                "remediation_claim_failed"
            };
            fail_local_job(&app, &id, code).await;
            tracing::warn!(job_id = %id, %error, "remediation claim failed");
        }
        app.local_workers
            .lock()
            .expect("local worker lock poisoned")
            .remove(&id);
    });
}

async fn enter_local_job(store: &Store, mut job: JobRecord) -> color_eyre::eyre::Result<JobRecord> {
    if job.handle.state == JobState::Queued {
        job = store
            .transition_job(
                &job.handle.job_id,
                job.handle.revision,
                JobState::Dispatching,
                &json!({"executor":"hub"}),
                None,
            )
            .await?;
    }
    if job.handle.state == JobState::Dispatching {
        job = store
            .transition_job(
                &job.handle.job_id,
                job.handle.revision,
                JobState::Running,
                &json!({"executor":"hub"}),
                None,
            )
            .await?;
    }
    color_eyre::eyre::ensure!(
        job.handle.state == JobState::Running,
        "local job is not runnable"
    );
    Ok(job)
}

async fn fail_local_job(app: &App, id: &JobId, code: &'static str) {
    let Some(store) = &app.store else {
        return;
    };
    let Ok(job) = store.get_job(id).await else {
        return;
    };
    if !job.handle.state.is_terminal() && job.handle.state.can_transition_to(JobState::Failed) {
        let _ = store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Failed,
                &json!({"code":code}),
                Some(&json!({"error":code})),
            )
            .await;
    }
}

fn stable_child_job_id(parent: &JobId, key: &str) -> JobId {
    let digest = blake3::hash(format!("{}\0{key}", parent.as_str()).as_bytes())
        .to_hex()
        .to_string();
    JobId::parse(format!(
        "{}-{}-{}-{}-{}",
        &digest[0..8],
        &digest[8..12],
        &digest[12..16],
        &digest[16..20],
        &digest[20..32]
    ))
    .expect("digest produces a valid job ID")
}

async fn diagnostic_probe(
    app: &App,
    parent: &JobRecord,
    probe: &str,
    argv: &[String],
    profile: &str,
) -> DiagnosticEvidence {
    let child_id = stable_child_job_id(&parent.handle.job_id, probe);
    let child = NewJob {
        principal: parent.principal.clone(),
        host: parent.handle.host.clone(),
        operation: "exec.run".into(),
        spec_version: 1,
        spec: serde_json::to_value(ExecRunParams {
            host: parent.handle.host.clone(),
            profile: profile.to_owned(),
            command: CommandSpec::Argv(argv.to_vec()),
            cwd: None,
            env: BTreeMap::new(),
            credential_refs: Vec::new(),
            timeout_seconds: Some(60),
        })
        .expect("serializable diagnostic probe"),
        policy_version: "hub-config-v1:diagnostic".into(),
        deadline: parent.deadline,
    };
    let mut response = agent_request(
        app,
        &parent.handle.host,
        &ExecutorRequest::Submit {
            job_id: child_id.clone(),
            job: child,
        },
    )
    .await;
    for _ in 0..600 {
        if response
            .as_ref()
            .ok()
            .and_then(|response| match response {
                ExecutorResponse::Job(job) => Some(job.handle.state.is_terminal()),
                _ => None,
            })
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        response = agent_request(
            app,
            &parent.handle.host,
            &ExecutorRequest::Status(JobIdParams {
                job_id: child_id.clone(),
            }),
        )
        .await;
    }
    let Ok(ExecutorResponse::Job(job)) = response else {
        return DiagnosticEvidence {
            evidence_id: format!("probe:{probe}"),
            source: "configured_probe".into(),
            assessment: EvidenceAssessment::Missing,
            value: json!({"reason":"executor_unavailable"}),
        };
    };
    if !job.handle.state.is_terminal() {
        return DiagnosticEvidence {
            evidence_id: format!("probe:{probe}"),
            source: "configured_probe".into(),
            assessment: EvidenceAssessment::Missing,
            value: json!({"reason":"probe_timeout","job_id":child_id}),
        };
    }
    let logs = agent_request(
        app,
        &parent.handle.host,
        &ExecutorRequest::Logs(JobLogsParams {
            job_id: child_id.clone(),
            stdout_offset: 0,
            stderr_offset: 0,
            limit: 64 * 1024,
        }),
    )
    .await
    .ok()
    .and_then(|response| match response {
        ExecutorResponse::Logs(logs) => Some(logs),
        _ => None,
    });
    DiagnosticEvidence {
        evidence_id: format!("probe:{probe}"),
        source: "configured_probe".into(),
        assessment: EvidenceAssessment::Fact,
        value: json!({
            "job_id": child_id,
            "state": job.handle.state,
            "result": job.result,
            "output": logs,
        }),
    }
}

async fn run_diagnostics(app: Arc<App>, job: JobRecord) -> color_eyre::eyre::Result<()> {
    color_eyre::eyre::ensure!(dispatch_authorized(&app, &job), "authorization removed");
    let store = app
        .store
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("store disabled"))?;
    let job = enter_local_job(store, job).await?;
    let params: DiagnosticCollectParams = serde_json::from_value(job.spec.clone())?;
    let host = app
        .hosts
        .get(&params.host)
        .ok_or_else(|| color_eyre::eyre::eyre!("host missing"))?;
    color_eyre::eyre::ensure!(
        params
            .probes
            .iter()
            .all(|probe| host.config.diagnostic_probes.contains_key(probe)),
        "diagnostic probe authorization removed"
    );
    let mut evidence = Vec::new();
    let mut selected_unit_state = None;
    match observe(&app, host).await {
        Ok(snapshot) => {
            if let Some(unit) = &params.unit {
                selected_unit_state = snapshot
                    .units
                    .iter()
                    .find(|candidate| &candidate.unit == unit)
                    .map(|candidate| candidate.active_state.clone());
            }
            evidence.push(DiagnosticEvidence {
                evidence_id: "snapshot".into(),
                source: "agent.snapshot".into(),
                assessment: EvidenceAssessment::Fact,
                value: serde_json::to_value(snapshot)?,
            });
        }
        Err(_) => evidence.push(DiagnosticEvidence {
            evidence_id: "snapshot".into(),
            source: "agent.snapshot".into(),
            assessment: EvidenceAssessment::Missing,
            value: json!({"reason":"agent_unavailable"}),
        }),
    }
    if let Some(unit) = &params.unit {
        let logs = transport::read_json::<Value>(
            host.token
                .apply(app.client.post(format!(
                    "{}/v1/logs",
                    host.config.agent_url.trim_end_matches('/')
                )))
                .json(&LogParams {
                    host: params.host.clone(),
                    unit: unit.clone(),
                    lines: params.lines,
                    since_seconds: params.since_seconds,
                }),
        )
        .await;
        evidence.push(match logs {
            Ok(value) => DiagnosticEvidence {
                evidence_id: "unit_logs".into(),
                source: "agent.journal".into(),
                assessment: EvidenceAssessment::Fact,
                value,
            },
            Err(_) => DiagnosticEvidence {
                evidence_id: "unit_logs".into(),
                source: "agent.journal".into(),
                assessment: EvidenceAssessment::Missing,
                value: json!({"reason":"logs_unavailable"}),
            },
        });
    }
    if !params.probes.is_empty() {
        let profile = host
            .config
            .diagnostic_profile
            .as_deref()
            .ok_or_else(|| color_eyre::eyre::eyre!("diagnostic profile missing"))?;
        for probe in &params.probes {
            evidence.push(
                diagnostic_probe(
                    &app,
                    &job,
                    probe,
                    &host.config.diagnostic_probes[probe],
                    profile,
                )
                .await,
            );
        }
    }
    let mut rules = Vec::new();
    if let Some(state) = selected_unit_state {
        rules.push(DiagnosticRuleResult {
            rule_id: "systemd.unit-state".into(),
            confidence: "high".into(),
            evidence_ids: vec!["snapshot".into()],
            conclusion: if state == "failed" {
                "selected unit is failed".into()
            } else {
                format!("selected unit active_state is {state}")
            },
        });
    } else if params.unit.is_some() {
        rules.push(DiagnosticRuleResult {
            rule_id: "systemd.unit-state".into(),
            confidence: "unknown".into(),
            evidence_ids: vec!["snapshot".into()],
            conclusion: "selected unit state is unavailable".into(),
        });
    }
    let bundle = DiagnosticBundle {
        artifact_id: stable_child_job_id(&job.handle.job_id, "diagnostic-artifact").to_string(),
        host: params.host.clone(),
        unit: params.unit.clone(),
        collected_at: now(),
        evidence,
        rules,
    };
    if store
        .event_for_job_kind(&job.handle.job_id, EventKind::DiagnosticCollected)
        .await?
        .is_none()
    {
        let (episode_id, fingerprint) = match &params.event_id {
            Some(event_id) => {
                let event = store.event_for_host(event_id, &params.host).await?;
                (event.episode_id, event.fingerprint)
            }
            None => (
                EpisodeId::parse(uuid::Uuid::now_v7().to_string())
                    .map_err(|message| color_eyre::eyre::eyre!(message))?,
                format!("diagnostic:{}", job.handle.job_id),
            ),
        };
        store
            .append_fleet_event(NewFleetEvent {
                source: "maxops.diagnostics".into(),
                fingerprint,
                episode_id,
                kind: EventKind::DiagnosticCollected,
                host: params.host,
                occurred_at: bundle.collected_at,
                related_job_id: Some(job.handle.job_id.clone()),
                related_change_id: None,
                payload: serde_json::to_value(&bundle)?,
            })
            .await?;
        app.events_ingested_total.fetch_add(1, Ordering::Relaxed);
    }
    store
        .transition_job(
            &job.handle.job_id,
            job.handle.revision,
            JobState::Succeeded,
            &json!({"artifact_id":bundle.artifact_id}),
            Some(&json!({"diagnostic":bundle})),
        )
        .await?;
    Ok(())
}

async fn run_remediation_begin(app: Arc<App>, job: JobRecord) -> color_eyre::eyre::Result<()> {
    color_eyre::eyre::ensure!(dispatch_authorized(&app, &job), "authorization removed");
    let store = app
        .store
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("store disabled"))?;
    let job = enter_local_job(store, job).await?;
    let params: RemediationBeginParams = serde_json::from_value(job.spec.clone())?;
    let remediation = store
        .begin_remediation(
            &params.event_id,
            &params.host,
            &job.principal,
            &job.handle.job_id,
            app.remediation_policy.max_attempts_per_episode,
            app.remediation_policy.cooldown_seconds,
        )
        .await?;
    app.events_ingested_total.fetch_add(1, Ordering::Relaxed);
    store
        .transition_job(
            &job.handle.job_id,
            job.handle.revision,
            JobState::Succeeded,
            &json!({"remediation_id":remediation.remediation_id}),
            Some(&json!({"remediation":remediation})),
        )
        .await?;
    Ok(())
}

async fn dispatch(app: Arc<App>, id: JobId) -> color_eyre::eyre::Result<()> {
    let store = app
        .store
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("store disabled"))?;
    let mut job = store.get_job(&id).await?;
    let authorized = dispatch_authorized(&app, &job);
    if !authorized {
        if job.handle.state == JobState::Queued {
            store
                .transition_job(
                    &id,
                    job.handle.revision,
                    JobState::Cancelled,
                    &json!({"reason":"authorization no longer permits dispatch"}),
                    Some(&json!({"started":false,"cancelled":true})),
                )
                .await?;
        }
        return Ok(());
    }
    if job.handle.state == JobState::Running {
        return monitor_target(app, id).await;
    }
    if job.handle.state == JobState::Queued {
        job = store
            .transition_job(
                &id,
                job.handle.revision,
                JobState::Dispatching,
                &json!({"target":job.handle.host}),
                None,
            )
            .await?;
    }
    let request = submit_request(&job);
    match agent_request(&app, &job.handle.host, &request).await {
        Ok(ExecutorResponse::Job(target)) => {
            job = project_target_job(store, job, target).await?;
        }
        Ok(_) => color_eyre::eyre::bail!("invalid executor response"),
        Err(error) => {
            if job.handle.state != JobState::Reconciling
                && job.handle.state.can_transition_to(JobState::Reconciling)
                && let Ok(updated) = store
                    .transition_job(
                        &id,
                        job.handle.revision,
                        JobState::Reconciling,
                        &json!({"reason":"dispatch acknowledgement unavailable"}),
                        None,
                    )
                    .await
            {
                job = updated;
            }
            tracing::warn!(job_id = %id, %error, "dispatch acknowledgement unavailable");
        }
    }
    if !job.handle.state.is_terminal() {
        monitor_target(app, id).await?;
    }
    Ok(())
}

async fn monitor_target(app: Arc<App>, id: JobId) -> color_eyre::eyre::Result<()> {
    loop {
        let store = app
            .store
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("store disabled"))?;
        let current = store.get_job(&id).await?;
        if current.handle.state.is_terminal()
            && (current.handle.state != JobState::OutcomeUnknown
                || current.handle.operation.starts_with("units."))
        {
            return Ok(());
        }
        match agent_request(
            &app,
            &current.handle.host,
            &ExecutorRequest::Status(JobIdParams { job_id: id.clone() }),
        )
        .await
        {
            Ok(ExecutorResponse::Job(target)) => {
                let updated = project_target_job(store, current, target).await?;
                if updated.handle.state.is_terminal()
                    && (updated.handle.state != JobState::OutcomeUnknown
                        || updated.handle.operation.starts_with("units."))
                {
                    return Ok(());
                }
            }
            Ok(_) => color_eyre::eyre::bail!("invalid executor response"),
            Err(error) => {
                // A lost submit acknowledgement leaves the hub unable to tell whether the
                // target accepted the job. Re-sending the same stable job ID is safe because
                // the executor persists and deduplicates it before launching systemd.
                if matches!(
                    current.handle.state,
                    JobState::Dispatching | JobState::Reconciling
                ) && dispatch_authorized(&app, &current)
                {
                    match agent_request(&app, &current.handle.host, &submit_request(&current)).await
                    {
                        Ok(ExecutorResponse::Job(target)) => {
                            let updated = project_target_job(store, current, target).await?;
                            if updated.handle.state.is_terminal()
                                && (updated.handle.state != JobState::OutcomeUnknown
                                    || updated.handle.operation.starts_with("units."))
                            {
                                return Ok(());
                            }
                        }
                        Ok(_) => color_eyre::eyre::bail!("invalid executor response"),
                        Err(retry_error) => tracing::debug!(
                            job_id = %id,
                            %error,
                            %retry_error,
                            "target status and idempotent submit retry unavailable"
                        ),
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn dispatch_authorized(app: &App, job: &JobRecord) -> bool {
    app.clients.iter().any(|principal| {
        let capability = operations()
            .into_iter()
            .find(|operation| operation.name == job.handle.operation)
            .map(|operation| operation.capability);
        let target_still_allowed = if job.handle.operation.starts_with("units.") {
            serde_json::from_value::<UnitActionParams>(job.spec.clone())
                .ok()
                .zip(app.hosts.get(&job.handle.host))
                .is_some_and(|(params, host)| host.config.manageable_units.contains(&params.unit))
        } else {
            true
        };
        let repository_still_allowed = if job.handle.operation.starts_with("workspace.") {
            job.spec
                .get("repository")
                .and_then(Value::as_str)
                .is_some_and(|repository| {
                    principal.repositories.contains(repository)
                        && app
                            .repositories
                            .get(repository)
                            .is_some_and(|host| host == &job.handle.host)
                })
        } else {
            true
        };
        let deployment_still_allowed = if job.handle.operation.starts_with("deploy.") {
            serde_json::from_value::<DeploymentJobSpec>(job.spec.clone())
                .ok()
                .and_then(|spec| {
                    app.deployments
                        .get(&spec.plan.deployment_profile)
                        .map(|deployment| (spec, deployment))
                })
                .is_some_and(|(spec, deployment)| {
                    let expected_host = match spec.action {
                        DeploymentAction::Prepare | DeploymentAction::Build => {
                            &deployment.builder_host
                        }
                        DeploymentAction::Activate
                        | DeploymentAction::Verify
                        | DeploymentAction::Rollback => &deployment.target_host,
                    };
                    principal.deployments.contains(&deployment.name)
                        && principal.repositories.contains(&deployment.repository)
                        && deployment.repository == spec.plan.repository
                        && deployment.target_host == spec.plan.target_host
                        && deployment.kind == spec.plan.kind
                        && deployment.flake_attribute == spec.plan.flake_attribute
                        && expected_host == &job.handle.host
                })
        } else {
            true
        };
        principal.name == job.principal
            && principal.access == Access::Manage
            && capability.is_some_and(|capability| principal.capabilities.contains(capability))
            && principal.hosts.contains(&job.handle.host)
            && target_still_allowed
            && repository_still_allowed
            && deployment_still_allowed
    })
}

fn submit_request(job: &JobRecord) -> ExecutorRequest {
    ExecutorRequest::Submit {
        job_id: job.handle.job_id.clone(),
        job: NewJob {
            principal: job.principal.clone(),
            host: job.handle.host.clone(),
            operation: job.handle.operation.clone(),
            spec_version: job.spec_version,
            spec: job.spec.clone(),
            policy_version: job.policy_version.clone(),
            deadline: job.deadline,
        },
    }
}

#[utoipa::path(post, path = "/v1/execute", request_body = Request, responses(
    (status = 200, description = "Read-only observation; aggregate responses may include unavailable hosts", body = Value),
    (status = 202, description = "Durable job accepted", body = maxops_proto::JobHandle),
    (status = 401, description = "Missing or invalid bearer token"),
    (status = 403, description = "Capability, host or unit not permitted"),
    (status = 422, description = "Request does not match the operation schema"),
    (status = 502, description = "Upstream observation unavailable")
), security(("bearer" = [])))]
async fn execute(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    representation: Query<client_api::Representation>,
    payload: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    app.requests_total.fetch_add(1, Ordering::Relaxed);
    let principal = authenticate(&app, &headers)?;
    let Json(raw) = payload
        .map_err(|error| ApiError(error.status(), "request does not match operation schema"))?;
    if let Some(name) = raw.get("op").and_then(Value::as_str)
        && !operations().iter().any(|operation| operation.name == name)
    {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported operation",
        ));
    }
    let request: Request = serde_json::from_value(raw).map_err(|_| {
        ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "request does not match operation schema",
        )
    })?;
    if !principal.capabilities.contains(request.capability()) {
        return Err(ApiError(StatusCode::FORBIDDEN, "capability not permitted"));
    }
    let _slot = (if matches!(request, Request::JobsWait(_)) {
        &app.wait_slots
    } else {
        &app.slots
    })
    .try_acquire()
    .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "hub busy"))?;
    representation.0.validate()?;
    let operation = request.name();
    let (kind, idempotency) = operations()
        .into_iter()
        .find(|definition| definition.name == operation)
        .map(|definition| (definition.kind, definition.idempotency))
        .expect("request operation is registered");
    if kind != OperationKind::Observation && principal.access != Access::Manage {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "management access required",
        ));
    }
    if idempotency == IdempotencyRequirement::None && headers.contains_key("idempotency-key") {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "Idempotency-Key is not used by this operation",
        ));
    }
    let result = if kind == OperationKind::Observation {
        run(&app, principal, request)
            .await
            .map(|value| (StatusCode::OK, value))
    } else {
        run_job_operation(&app, principal, &headers, request).await
    };
    tracing::info!(actor = %principal.name, operation, success = result.is_ok(), "query completed");
    result.and_then(|(status, value)| {
        client_api::present(representation, operation, value).map(|value| (status, Json(value)))
    })
}

fn spawn_event_delivery(app: Arc<App>) {
    tokio::spawn(async move {
        loop {
            for sink in &app.event_sinks {
                if let Err(error) = deliver_next_event(&app, sink).await {
                    app.delivery_failures_total.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(subscription = %sink.config.id, %error, "event delivery failed");
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

async fn deliver_next_event(app: &App, sink: &EventSink) -> color_eyre::eyre::Result<()> {
    let store = app
        .store
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("store disabled"))?;
    let cursor = store.subscription_cursor(&sink.config.id).await?;
    let Some(event) = store.next_event(cursor.cursor).await? else {
        return Ok(());
    };
    let host_matches = sink.config.hosts.is_empty() || sink.config.hosts.contains(&event.host);
    let kind_matches = sink.config.kinds.is_empty() || sink.config.kinds.contains(&event.kind);
    if !host_matches || !kind_matches {
        store
            .skip_subscription_event(&sink.config.id, event.sequence)
            .await?;
        return Ok(());
    }
    if store
        .begin_delivery(&sink.config.id, event.sequence, sink.config.retry_seconds)
        .await?
        .is_none()
    {
        return Ok(());
    }
    app.delivery_attempts_total.fetch_add(1, Ordering::Relaxed);
    let mut request = app.client.post(&sink.config.url).json(&event);
    if let Some(token) = &sink.token {
        request = token.apply(request);
    }
    let mut response = request.send().await?;
    let status = response.status();
    color_eyre::eyre::ensure!(status.is_success(), "event sink rejected delivery");
    let mut response_body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        color_eyre::eyre::ensure!(
            response_body.len() + chunk.len() <= 4096,
            "event sink response too large"
        );
        response_body.extend_from_slice(&chunk);
    }
    let explicit_stage = serde_json::from_slice::<Value>(&response_body)
        .ok()
        .and_then(|value| {
            value
                .get("stage")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .and_then(|stage| DeliveryStage::parse(&stage).ok());
    let stage = explicit_stage.unwrap_or_else(|| {
        if status == StatusCode::ACCEPTED {
            DeliveryStage::Accepted
        } else {
            DeliveryStage::Confirmed
        }
    });
    if stage != DeliveryStage::Queued {
        store
            .acknowledge_delivery(
                &sink.config.id,
                event.sequence,
                stage,
                Some(status.as_u16()),
            )
            .await?;
    }
    Ok(())
}

fn alert_time(alert: &Value, firing: bool) -> jiff::Timestamp {
    let field = if firing { "startsAt" } else { "endsAt" };
    alert
        .get(field)
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<jiff::Timestamp>().ok())
        .unwrap_or_else(now)
}

async fn persist_alert_events(app: &App, payload: &Value) -> Result<usize, ApiError> {
    let Some(store) = &app.store else {
        return Ok(0);
    };
    let envelope_firing = payload["status"] == "firing";
    let alerts = payload["alerts"].as_array().ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "expected Alertmanager webhook v4",
    ))?;
    let mut persisted = 0;
    for alert in alerts {
        let Some(host) = alert["labels"]["instance"].as_str() else {
            continue;
        };
        if !app.hosts.contains_key(host) {
            continue;
        }
        let firing = alert["status"]
            .as_str()
            .map_or(envelope_firing, |status| status == "firing");
        let supplied = alert["fingerprint"].as_str().unwrap_or_default();
        let fingerprint = if supplied.is_empty() || supplied.len() > 512 {
            blake3::hash(alert.to_string().as_bytes())
                .to_hex()
                .to_string()
        } else {
            supplied.to_owned()
        };
        store
            .ingest_alert(AlertEventInput {
                source: "alertmanager".into(),
                fingerprint,
                host: host.to_owned(),
                firing,
                occurred_at: alert_time(alert, firing),
                payload: alert.clone(),
            })
            .await
            .map_err(map_store_error)?;
        persisted += 1;
        app.events_ingested_total.fetch_add(1, Ordering::Relaxed);
    }
    Ok(persisted)
}

async fn alerts(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> ApiResult<Value> {
    let ingress = app
        .alert_ingress
        .as_ref()
        .ok_or(ApiError(StatusCode::NOT_FOUND, "alert ingress disabled"))?;
    if !ingress.token.matches(&headers) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "unauthorized"));
    }
    if payload["version"] != "4"
        || !payload["alerts"].is_array()
        || !matches!(payload["status"].as_str(), Some("firing" | "resolved"))
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "expected Alertmanager webhook v4",
        ));
    }
    let _slot = app.slots.try_acquire().map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "hub busy; retry notification",
        )
    })?;
    let persisted = persist_alert_events(&app, &payload).await?;
    let request = app.client.post(&ingress.sink_url).json(&payload);
    let request = match &ingress.sink_token {
        Some(token) => token.apply(request),
        None => request,
    };
    match request.send().await {
        Ok(response) if response.status().is_success() => Ok(Json(json!({
            "accepted": true,
            "stage": "sink_acknowledged",
            "events_persisted": persisted,
        }))),
        _ => Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "notification not acknowledged; retry",
        )),
    }
}

#[cfg(test)]
mod tests;
