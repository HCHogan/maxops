use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use futures::future::join_all;
use maxops_proto::{
    ExecutorRequest, ExecutorResponse, IdempotencyRequirement, JobCancelParams, JobId, JobIdParams,
    JobRecord, JobState, JobsListResponse, NewJob, OperationKind, PROTOCOL_VERSION, Request,
    Snapshot, now, operations,
    transport::{self, ApiError, ApiResult, Token},
    valid_host, valid_unit,
};
use maxops_store::Store;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::Semaphore;
use utoipa::OpenApi;

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
    prometheus_url: Option<String>,
    #[serde(default)]
    alertmanager_url: Option<String>,
    #[serde(default)]
    alert_ingress: Option<AlertIngressConfig>,
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
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientConfig {
    name: String,
    token_file: PathBuf,
    hosts: BTreeSet<String>,
    capabilities: BTreeSet<String>,
    #[serde(default)]
    access: Access,
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
}
struct AlertIngress {
    token: Token,
    sink_url: String,
    sink_token: Option<Token>,
}
struct App {
    hosts: BTreeMap<String, Host>,
    clients: Vec<Principal>,
    client: reqwest::Client,
    prometheus_url: Option<String>,
    alertmanager_url: Option<String>,
    alert_ingress: Option<AlertIngress>,
    slots: Semaphore,
    store: Option<Store>,
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
            host.readable_units.iter().all(|u| valid_unit(u)),
            "invalid readable service name"
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
    let known_caps: BTreeSet<_> = operations().into_iter().map(|op| op.capability).collect();
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
    if management_enabled {
        for principal in clients
            .iter()
            .filter(|client| client.access == Access::Manage)
        {
            color_eyre::eyre::ensure!(
                principal.hosts.iter().all(|name| {
                    hosts
                        .get(name)
                        .and_then(|host| host.execution_token.as_ref())
                        .is_some()
                }),
                "management client references a host without execution credentials"
            );
        }
    }
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
    let app = Arc::new(App {
        hosts,
        clients,
        client: transport::client()?,
        prometheus_url: config.prometheus_url,
        alertmanager_url: config.alertmanager_url,
        alert_ingress,
        slots: Semaphore::new(16),
        store,
    });
    if let Some(store) = &app.store {
        for job in store.nonterminal_jobs().await? {
            spawn_dispatch(app.clone(), job.handle.job_id);
        }
    }
    Ok((config.listen, router(app)))
}

fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/healthz", get(transport::health))
        .route("/v1/operations", get(catalog))
        .route("/v1/openapi.json", get(openapi))
        .route(
            "/v1/execute",
            post(execute).layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route("/v1/alerts", post(alerts))
        .layer(DefaultBodyLimit::max(256 * 1024))
        .with_state(app)
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

fn unit_allowed(host: &Host, unit: &str) -> Result<(), ApiError> {
    if !host.config.readable_units.contains(unit) {
        return Err(ApiError(StatusCode::FORBIDDEN, "unit not permitted"));
    }
    Ok(())
}

#[utoipa::path(get, path = "/v1/operations", responses(
    (status = 200, description = "Operations permitted for the authenticated principal", body = Value),
    (status = 401, description = "Missing or invalid bearer token")
), security(("bearer" = [])))]
async fn catalog(State(app): State<Arc<App>>, headers: HeaderMap) -> ApiResult<Value> {
    let principal = authenticate(&app, &headers)?;
    Ok(Json(
        json!({"version": PROTOCOL_VERSION, "operations": operations().into_iter()
        .filter(|op| principal.capabilities.contains(op.capability)).collect::<Vec<_>>()}),
    ))
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
    snapshot
        .units
        .retain(|unit| host.config.readable_units.contains(&unit.unit));
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
                        "failed_units": snapshot.units.iter().filter(|u| u.active_state == "failed").count()}),
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
                    "state": "available", "units": snapshot.units.into_iter().filter(|u| u.active_state == "failed").collect::<Vec<_>>()}),
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
            Ok(
                json!({"host": snapshot.host, "observed_at": snapshot.observed_at, "units": snapshot.units}),
            )
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
        Request::ExecRun(_)
        | Request::JobsList(_)
        | Request::JobsStatus(_)
        | Request::JobsLogs(_)
        | Request::JobsCancel(_) => Err(ApiError(
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
    if message.contains("idempotency key") || message.contains("revision changed") {
        ApiError(
            StatusCode::CONFLICT,
            "job request conflicts with current state",
        )
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
            let jobs = store
                .list_jobs(
                    Some(&principal.name),
                    params.host.as_deref(),
                    &params.states,
                    params.limit,
                )
                .await
                .map_err(map_store_error)?
                .into_iter()
                .filter(|job| principal.hosts.contains(&job.handle.host))
                .collect();
            Ok((
                StatusCode::OK,
                serde_json::to_value(JobsListResponse { jobs }).expect("serializable jobs"),
            ))
        }
        Request::JobsStatus(params) => {
            let mut job = store
                .get_owned_job(&principal.name, &params.job_id)
                .await
                .map_err(map_store_error)?;
            host_for(app, principal, &job.handle.host)?;
            if (!job.handle.state.is_terminal() || job.handle.state == JobState::OutcomeUnknown)
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
    transport::read_json(
        token
            .apply(app.client.post(format!(
                "{}/v1/manage",
                host.config.agent_url.trim_end_matches('/')
            )))
            .json(request),
    )
    .await
}

async fn project_target_job(
    store: &Store,
    current: JobRecord,
    target: JobRecord,
) -> color_eyre::eyre::Result<JobRecord> {
    color_eyre::eyre::ensure!(
        current.handle.job_id == target.handle.job_id
            && current.handle.host == target.handle.host
            && current.spec_hash == target.spec_hash,
        "executor job identity mismatch"
    );
    if current.handle.state == target.handle.state
        || !current.handle.state.can_transition_to(target.handle.state)
    {
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
        if current.handle.state.is_terminal() && current.handle.state != JobState::OutcomeUnknown {
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
                    && updated.handle.state != JobState::OutcomeUnknown
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
                                && updated.handle.state != JobState::OutcomeUnknown
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
        principal.name == job.principal
            && principal.access == Access::Manage
            && principal.capabilities.contains("exec:run")
            && principal.hosts.contains(&job.handle.host)
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
    Json(request): Json<Request>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let principal = authenticate(&app, &headers)?;
    if !principal.capabilities.contains(request.capability()) {
        return Err(ApiError(StatusCode::FORBIDDEN, "capability not permitted"));
    }
    let _slot = app
        .slots
        .try_acquire()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "hub busy"))?;
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
    result.map(|(status, value)| (status, Json(value)))
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
    let request = app.client.post(&ingress.sink_url).json(&payload);
    let request = match &ingress.sink_token {
        Some(token) => token.apply(request),
        None => request,
    };
    match request.send().await {
        Ok(response) if response.status().is_success() => Ok(Json(
            json!({"accepted": true, "stage": "sink_acknowledged"}),
        )),
        _ => Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "notification not acknowledged; retry",
        )),
    }
}

#[cfg(test)]
mod tests;
