use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use futures::future::join_all;
use maxops_proto::{
    Request, Snapshot, now, operations,
    transport::{self, ApiError, ApiResult, Token},
    valid_host, valid_unit,
};
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
    readable_units: BTreeSet<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientConfig {
    name: String,
    token_file: PathBuf,
    hosts: BTreeSet<String>,
    capabilities: BTreeSet<String>,
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
}
struct Principal {
    name: String,
    token: Token,
    hosts: BTreeSet<String>,
    capabilities: BTreeSet<String>,
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
}

pub fn build(config: Config) -> color_eyre::eyre::Result<(SocketAddr, Router)> {
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
        color_eyre::eyre::ensure!(
            !hosts
                .values()
                .any(|other: &Host| other.token.same_as(&token)),
            "each agent requires a distinct credential"
        );
        color_eyre::eyre::ensure!(
            hosts
                .insert(
                    host.name.clone(),
                    Host {
                        config: host,
                        token
                    }
                )
                .is_none(),
            "duplicate inventory host"
        );
    }
    let known_caps: BTreeSet<_> = operations().into_iter().map(|op| op.capability).collect();
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
        clients.push(Principal {
            name: client.name,
            token,
            hosts: client.hosts,
            capabilities: client.capabilities,
        });
    }
    color_eyre::eyre::ensure!(
        !clients.is_empty(),
        "at least one authenticated client is required"
    );
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
                    && !hosts.values().any(|h| h.token.same_as(&token)),
                "alert ingress requires a dedicated token"
            );
            if let Some(sink) = &sink_token {
                color_eyre::eyre::ensure!(
                    !sink.same_as(&token)
                        && !clients
                            .iter()
                            .any(|principal| principal.token.same_as(sink))
                        && !hosts.values().any(|host| host.token.same_as(sink)),
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
    });
    Ok((config.listen, router(app)))
}

fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/healthz", get(transport::health))
        .route("/v1/operations", get(catalog))
        .route("/v1/openapi.json", get(openapi))
        .route(
            "/v1/execute",
            post(execute).layer(DefaultBodyLimit::max(4096)),
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
        json!({"version": 1, "operations": operations().into_iter()
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
    }
}

#[utoipa::path(post, path = "/v1/execute", request_body = Request, responses(
    (status = 200, description = "Read-only observation; aggregate responses may include unavailable hosts", body = Value),
    (status = 401, description = "Missing or invalid bearer token"),
    (status = 403, description = "Capability, host or unit not permitted"),
    (status = 422, description = "Request does not match the operation schema"),
    (status = 502, description = "Upstream observation unavailable")
), security(("bearer" = [])))]
async fn execute(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> ApiResult<Value> {
    let principal = authenticate(&app, &headers)?;
    if !principal.capabilities.contains(request.capability()) {
        return Err(ApiError(StatusCode::FORBIDDEN, "capability not permitted"));
    }
    let _slot = app
        .slots
        .try_acquire()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "hub busy"))?;
    let operation = request.name();
    let result = run(&app, principal, request).await;
    tracing::info!(actor = %principal.name, operation, success = result.is_ok(), "query completed");
    result.map(Json)
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
