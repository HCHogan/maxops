use super::*;
use axum::{body::Body, http::Request as HttpRequest};
use http_body_util::BodyExt;
use std::sync::{
    Arc as StdArc, Mutex as StdMutex,
    atomic::{AtomicUsize, Ordering},
};
use tower::ServiceExt;

const USER_TOKEN: &str = "user-token-aaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const AGENT_TOKEN: &str = "agent-token-bbbbbbbbbbbbbbbbbbbbbbbbbb";
const ALERT_TOKEN: &str = "alert-token-cccccccccccccccccccccccccc";
const EXECUTION_TOKEN: &str = "execution-token-dddddddddddddddddddddddd";

fn app(agent_url: &str, capabilities: &[&str]) -> App {
    App {
        hosts: BTreeMap::from([
            (
                "alpha".into(),
                Host {
                    config: HostConfig {
                        name: "alpha".into(),
                        site: Some("test".into()),
                        agent_url: agent_url.into(),
                        agent_token_file: PathBuf::new(),
                        execution_token_file: None,
                        readable_units: BTreeSet::from(["demo.service".into()]),
                        manageable_units: BTreeSet::new(),
                        diagnostic_profile: None,
                        diagnostic_probes: BTreeMap::new(),
                    },
                    token: Token::parse(AGENT_TOKEN.into()).unwrap(),
                    execution_token: None,
                },
            ),
            (
                "private".into(),
                Host {
                    config: HostConfig {
                        name: "private".into(),
                        site: None,
                        agent_url: agent_url.into(),
                        agent_token_file: PathBuf::new(),
                        execution_token_file: None,
                        readable_units: BTreeSet::new(),
                        manageable_units: BTreeSet::new(),
                        diagnostic_profile: None,
                        diagnostic_probes: BTreeMap::new(),
                    },
                    token: Token::parse(AGENT_TOKEN.into()).unwrap(),
                    execution_token: None,
                },
            ),
        ]),
        clients: vec![Principal {
            name: "reader".into(),
            token: Token::parse(USER_TOKEN.into()).unwrap(),
            hosts: BTreeSet::from(["alpha".into()]),
            capabilities: capabilities.iter().map(|s| s.to_string()).collect(),
            access: Access::Observe,
            repositories: BTreeSet::new(),
            deployments: BTreeSet::new(),
        }],
        client: transport::client().unwrap(),
        prometheus_url: None,
        alertmanager_url: None,
        alert_ingress: None,
        event_sinks: Vec::new(),
        remediation_policy: RemediationPolicyConfig::default(),
        slots: Semaphore::new(16),
        store: None,
        repositories: BTreeMap::new(),
        deployments: BTreeMap::new(),
        started_at: now(),
        agent_heartbeats: Mutex::new(BTreeMap::new()),
        executor_heartbeats: Mutex::new(BTreeMap::new()),
        local_workers: Mutex::new(HashSet::new()),
        requests_total: AtomicU64::new(0),
        events_ingested_total: AtomicU64::new(0),
        delivery_attempts_total: AtomicU64::new(0),
        delivery_failures_total: AtomicU64::new(0),
    }
}

async fn call(router: Router, path: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
    call_with_idempotency(router, path, token, None, body).await
}

async fn call_with_idempotency(
    router: Router,
    path: &str,
    token: Option<&str>,
    idempotency_key: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut builder = HttpRequest::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(key) = idempotency_key {
        builder = builder.header("idempotency-key", key);
    }
    let response = router
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn successful_executor(
    State(store): State<Store>,
    headers: HeaderMap,
    Json(request): Json<ExecutorRequest>,
) -> Result<Json<ExecutorResponse>, StatusCode> {
    if !Token::parse(EXECUTION_TOKEN.into())
        .unwrap()
        .matches(&headers)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    match request {
        ExecutorRequest::Submit { job_id, job } => {
            let accepted = store.accept_job(&job_id, &job).await.unwrap();
            let job = if accepted.created {
                let dispatching = store
                    .transition_job(&job_id, 1, JobState::Dispatching, &json!({}), None)
                    .await
                    .unwrap();
                let running = store
                    .transition_job(
                        &job_id,
                        dispatching.handle.revision,
                        JobState::Running,
                        &json!({}),
                        None,
                    )
                    .await
                    .unwrap();
                store
                    .transition_job(
                        &job_id,
                        running.handle.revision,
                        JobState::Succeeded,
                        &json!({}),
                        Some(&json!({"exit_code":0})),
                    )
                    .await
                    .unwrap()
            } else {
                accepted.job
            };
            Ok(Json(ExecutorResponse::Job(job)))
        }
        ExecutorRequest::Status(params) => Ok(Json(ExecutorResponse::Job(
            store.get_job(&params.job_id).await.unwrap(),
        ))),
        ExecutorRequest::Logs(params) => Ok(Json(ExecutorResponse::Logs(
            maxops_proto::JobLogsResponse {
                job_id: params.job_id,
                encoding: "base64".into(),
                stdout_base64: String::new(),
                stderr_base64: String::new(),
                next_stdout_offset: 0,
                next_stderr_offset: 0,
                complete: true,
                truncated: false,
            },
        ))),
        ExecutorRequest::Cancel(_) => Err(StatusCode::CONFLICT),
        ExecutorRequest::Workspace { .. }
        | ExecutorRequest::RepositoryHead { .. }
        | ExecutorRequest::RuntimeState { .. } => Err(StatusCode::BAD_REQUEST),
    }
}

#[derive(Clone)]
struct DeploymentExecutor {
    store: Store,
    remote_head: StdArc<StdMutex<String>>,
    runtime: StdArc<StdMutex<String>>,
}

async fn deployment_executor(
    State(state): State<DeploymentExecutor>,
    headers: HeaderMap,
    Json(request): Json<ExecutorRequest>,
) -> Result<Json<ExecutorResponse>, StatusCode> {
    if !Token::parse(EXECUTION_TOKEN.into())
        .unwrap()
        .matches(&headers)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    match request {
        ExecutorRequest::Workspace {
            request: WorkspaceTargetRequest::Status(params),
            ..
        } => Ok(Json(ExecutorResponse::Workspace(
            WorkspaceTargetResponse::Record(maxops_proto::WorkspaceRecord {
                workspace_id: params.workspace_id,
                repository: params.repository,
                executor: "alpha".into(),
                base_commit: "1".repeat(40),
                revision: 4,
                tree_hash: "2".repeat(40),
                commit_hash: Some("3".repeat(40)),
                state: maxops_proto::WorkspaceState::Committed,
                creator: "manager".into(),
                created_at: now(),
                retain_until: None,
            }),
        ))),
        ExecutorRequest::Workspace { .. } => Err(StatusCode::BAD_REQUEST),
        ExecutorRequest::RepositoryHead { request, .. } => {
            let commit = state.remote_head.lock().unwrap().clone();
            Ok(Json(ExecutorResponse::RepositoryHead(
                RepositoryHeadResponse {
                    repository: request.repository,
                    reference: request.reference,
                    commit,
                    observed_at: now(),
                },
            )))
        }
        ExecutorRequest::RuntimeState { request, .. } => {
            let closure = state.runtime.lock().unwrap().clone();
            Ok(Json(ExecutorResponse::RuntimeState(RuntimeStateResponse {
                host: "alpha".into(),
                deployment_profile: request.deployment_profile,
                running_closure: Some(closure.clone()),
                persistent_profile: Some(closure),
                generation: Some(7),
                boot_id: Some("boot-a".into()),
                observed_at: now(),
            })))
        }
        ExecutorRequest::Submit { job_id, job } => {
            let accepted = state.store.accept_job(&job_id, &job).await.unwrap();
            if accepted.created {
                let spec: DeploymentJobSpec = serde_json::from_value(job.spec).unwrap();
                let (status, drv_path, out_path, lock_digest) = match spec.action {
                    DeploymentAction::Prepare => (
                        DeploymentReportStatus::Prepared,
                        Some("/nix/store/prepared.drv".into()),
                        None,
                        Some("4".repeat(64)),
                    ),
                    DeploymentAction::Build => (
                        DeploymentReportStatus::Built,
                        spec.plan.drv_path,
                        Some("/nix/store/built-system".into()),
                        spec.plan.lock_digest,
                    ),
                    DeploymentAction::Activate => {
                        (DeploymentReportStatus::Activated, None, None, None)
                    }
                    DeploymentAction::Verify => {
                        (DeploymentReportStatus::Verified, None, None, None)
                    }
                    DeploymentAction::Rollback => {
                        (DeploymentReportStatus::RolledBack, None, None, None)
                    }
                };
                let report = DeploymentReport {
                    action: spec.action,
                    status,
                    drv_path,
                    out_path,
                    lock_digest,
                    observed_running: None,
                    observed_profile: None,
                    detail: "fixture stage complete".into(),
                    completed_at: now(),
                };
                let dispatching = state
                    .store
                    .transition_job(&job_id, 1, JobState::Dispatching, &json!({}), None)
                    .await
                    .unwrap();
                state
                    .store
                    .transition_job(
                        &job_id,
                        dispatching.handle.revision,
                        JobState::Succeeded,
                        &json!({}),
                        Some(&json!({"deployment":report})),
                    )
                    .await
                    .unwrap();
            }
            Ok(Json(ExecutorResponse::Job(
                state.store.get_job(&job_id).await.unwrap(),
            )))
        }
        ExecutorRequest::Status(params) => state
            .store
            .get_job(&params.job_id)
            .await
            .map(|job| Json(ExecutorResponse::Job(job)))
            .map_err(|_| StatusCode::NOT_FOUND),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

#[derive(Clone)]
struct AckLossExecutor {
    store: Store,
    submissions: StdArc<AtomicUsize>,
}

async fn executor_with_lost_first_ack(
    State(state): State<AckLossExecutor>,
    headers: HeaderMap,
    Json(request): Json<ExecutorRequest>,
) -> Result<Json<ExecutorResponse>, StatusCode> {
    if !Token::parse(EXECUTION_TOKEN.into())
        .unwrap()
        .matches(&headers)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    match request {
        ExecutorRequest::Submit { job_id, job } => {
            let accepted = state.store.accept_job(&job_id, &job).await.unwrap();
            if accepted.created {
                let dispatching = state
                    .store
                    .transition_job(&job_id, 1, JobState::Dispatching, &json!({}), None)
                    .await
                    .unwrap();
                state
                    .store
                    .transition_job(
                        &job_id,
                        dispatching.handle.revision,
                        JobState::Succeeded,
                        &json!({}),
                        Some(&json!({"exit_code":0})),
                    )
                    .await
                    .unwrap();
            }
            let attempt = state.submissions.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            Ok(Json(ExecutorResponse::Job(
                state.store.get_job(&job_id).await.unwrap(),
            )))
        }
        ExecutorRequest::Status(params) if state.submissions.load(Ordering::SeqCst) < 2 => {
            let _ = params;
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
        ExecutorRequest::Status(params) => Ok(Json(ExecutorResponse::Job(
            state.store.get_job(&params.job_id).await.unwrap(),
        ))),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

async fn management_app(agent_url: &str, state_file: &std::path::Path) -> App {
    App {
        hosts: BTreeMap::from([(
            "alpha".into(),
            Host {
                config: HostConfig {
                    name: "alpha".into(),
                    site: Some("test".into()),
                    agent_url: agent_url.into(),
                    agent_token_file: PathBuf::new(),
                    execution_token_file: None,
                    readable_units: BTreeSet::new(),
                    manageable_units: BTreeSet::from(["demo.service".into()]),
                    diagnostic_profile: None,
                    diagnostic_probes: BTreeMap::new(),
                },
                token: Token::parse(AGENT_TOKEN.into()).unwrap(),
                execution_token: Some(Token::parse(EXECUTION_TOKEN.into()).unwrap()),
            },
        )]),
        clients: vec![Principal {
            name: "manager".into(),
            token: Token::parse(USER_TOKEN.into()).unwrap(),
            hosts: BTreeSet::from(["alpha".into()]),
            capabilities: BTreeSet::from([
                "exec:run".into(),
                "units:manage".into(),
                "jobs:read".into(),
                "jobs:cancel".into(),
            ]),
            access: Access::Manage,
            repositories: BTreeSet::new(),
            deployments: BTreeSet::new(),
        }],
        client: transport::client().unwrap(),
        prometheus_url: None,
        alertmanager_url: None,
        alert_ingress: None,
        event_sinks: Vec::new(),
        remediation_policy: RemediationPolicyConfig::default(),
        slots: Semaphore::new(16),
        store: Some(Store::open(state_file).await.unwrap()),
        repositories: BTreeMap::new(),
        deployments: BTreeMap::new(),
        started_at: now(),
        agent_heartbeats: Mutex::new(BTreeMap::new()),
        executor_heartbeats: Mutex::new(BTreeMap::new()),
        local_workers: Mutex::new(HashSet::new()),
        requests_total: AtomicU64::new(0),
        events_ingested_total: AtomicU64::new(0),
        delivery_attempts_total: AtomicU64::new(0),
        delivery_failures_total: AtomicU64::new(0),
    }
}

async fn deployment_management_app(agent_url: &str, state_file: &std::path::Path) -> App {
    let mut app = management_app(agent_url, state_file).await;
    app.clients[0].capabilities.extend([
        "workspace:read".into(),
        "deploy:manage".into(),
        "changes:read".into(),
    ]);
    app.clients[0].repositories.insert("fixture".into());
    app.clients[0].deployments.insert("fixture-system".into());
    app.repositories.insert("fixture".into(), "alpha".into());
    app.deployments.insert(
        "fixture-system".into(),
        DeploymentConfig {
            name: "fixture-system".into(),
            repository: "fixture".into(),
            builder_host: "alpha".into(),
            target_host: "alpha".into(),
            kind: DeploymentKind::System,
            flake_attribute: "nixosConfigurations.alpha.config.system.build.toplevel".into(),
            source_reference: "refs/heads/main".into(),
            plan_ttl_seconds: 3600,
        },
    );
    app
}

async fn get_json(router: Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut builder = HttpRequest::builder().method("GET").uri(path);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let response = router
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn stub(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (
        url,
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }),
    )
}

fn snapshot(host: &str) -> Value {
    json!({"host": host, "observed_at": now(), "facts": {"kernel": "test", "uptime_seconds": 42.0, "system_closure": "/nix/store/test-system"},
    "units": [
        {"unit": "demo.service", "description": "demo", "load_state": "loaded", "active_state": "failed", "sub_state": "failed"},
        {"unit": "secret.service", "description": "secret", "load_state": "loaded", "active_state": "failed", "sub_state": "failed"}
    ]})
}

#[tokio::test]
async fn management_submission_is_durable_idempotent_and_target_confirmed() {
    let target_directory = tempfile::tempdir().unwrap();
    let target_store = Store::open(&target_directory.path().join("target.db"))
        .await
        .unwrap();
    let (url, task) = stub(
        Router::new()
            .route("/v1/manage", post(successful_executor))
            .with_state(target_store),
    )
    .await;
    let hub_directory = tempfile::tempdir().unwrap();
    let router = router(Arc::new(
        management_app(&url, &hub_directory.path().join("hub.db")).await,
    ));
    let body = json!({
        "op":"exec.run",
        "params":{
            "host":"alpha",
            "profile":"diagnostic",
            "command":{"argv":["/bin/true"]},
            "timeout_seconds":30
        }
    });
    assert_eq!(
        call(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            body.clone()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, submitted) = call_with_idempotency(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("durable-submit-1"),
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let job_id = submitted["job_id"].as_str().unwrap().to_owned();
    let (_, repeated) = call_with_idempotency(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("durable-submit-1"),
        body,
    )
    .await;
    assert_eq!(repeated["job_id"], job_id);

    let mut observed = Value::Null;
    for _ in 0..20 {
        let (_, value) = call(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":"jobs.status","params":{"job_id":job_id}}),
        )
        .await;
        observed = value;
        if observed["handle"]["state"] == "succeeded" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(observed["handle"]["state"], "succeeded");
    assert_eq!(observed["result"]["exit_code"], 0);
    task.abort();
}

#[tokio::test]
async fn deployment_reobserves_remote_and_runtime_before_activation() {
    let target_directory = tempfile::tempdir().unwrap();
    let remote_head = StdArc::new(StdMutex::new("a".repeat(40)));
    let runtime = StdArc::new(StdMutex::new("/nix/store/base-system".into()));
    let (url, task) = stub(
        Router::new()
            .route("/v1/manage", post(deployment_executor))
            .with_state(DeploymentExecutor {
                store: Store::open(&target_directory.path().join("target.db"))
                    .await
                    .unwrap(),
                remote_head: remote_head.clone(),
                runtime: runtime.clone(),
            }),
    )
    .await;
    let hub_directory = tempfile::tempdir().unwrap();
    let router = router(Arc::new(
        deployment_management_app(&url, &hub_directory.path().join("hub.db")).await,
    ));
    let workspace_id = "11111111-1111-1111-1111-111111111111";

    let prepare_and_build = |suffix: &'static str, router: Router| async move {
        let (status, prepare) = call_with_idempotency(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            Some(&format!("prepare-{suffix}")),
            json!({
                "op":"deploy.prepare",
                "params":{
                    "repository":"fixture",
                    "workspace_id":workspace_id,
                    "expected_revision":4,
                    "target_host":"alpha",
                    "profile":"fixture-system"
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{prepare}");
        let change_id = prepare["job_id"].as_str().unwrap().to_owned();
        let mut change = Value::Null;
        for _ in 0..20 {
            let (_, value) = call(
                router.clone(),
                "/v1/execute",
                Some(USER_TOKEN),
                json!({"op":"changes.status","params":{"change_id":change_id}}),
            )
            .await;
            change = value;
            if change["state"] == "prepared" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(change["state"], "prepared");
        let revision = change["revision"].as_u64().unwrap();
        let (status, _) = call_with_idempotency(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            Some(&format!("build-{suffix}")),
            json!({
                "op":"deploy.build",
                "params":{"change_id":change_id,"expected_revision":revision}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        for _ in 0..20 {
            let (_, value) = call(
                router.clone(),
                "/v1/execute",
                Some(USER_TOKEN),
                json!({"op":"changes.status","params":{"change_id":change_id}}),
            )
            .await;
            change = value;
            if change["state"] == "ready" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(change["state"], "ready");
        (change_id, change["revision"].as_u64().unwrap())
    };

    let (remote_change, revision) = prepare_and_build("remote", router.clone()).await;
    *remote_head.lock().unwrap() = "b".repeat(40);
    let (status, _) = call_with_idempotency(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("activate-after-remote-change"),
        json!({
            "op":"deploy.activate",
            "params":{"change_id":remote_change,"expected_revision":revision}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (_, stale) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"changes.status","params":{"change_id":remote_change}}),
    )
    .await;
    assert_eq!(stale["state"], "stale");

    let (runtime_change, revision) = prepare_and_build("runtime", router.clone()).await;
    *runtime.lock().unwrap() = "/nix/store/human-rebuild".into();
    let (status, _) = call_with_idempotency(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("activate-after-runtime-change"),
        json!({
            "op":"deploy.activate",
            "params":{"change_id":runtime_change,"expected_revision":revision}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (_, stale) = call(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"changes.status","params":{"change_id":runtime_change}}),
    )
    .await;
    assert_eq!(stale["state"], "stale");
    task.abort();
}

#[tokio::test]
async fn service_submission_requires_capability_and_exact_manageable_unit() {
    let target_directory = tempfile::tempdir().unwrap();
    let target_store = Store::open(&target_directory.path().join("target.db"))
        .await
        .unwrap();
    let (url, task) = stub(
        Router::new()
            .route("/v1/manage", post(successful_executor))
            .with_state(target_store),
    )
    .await;
    let hub_directory = tempfile::tempdir().unwrap();
    let router = router(Arc::new(
        management_app(&url, &hub_directory.path().join("hub.db")).await,
    ));
    let allowed = json!({
        "op":"units.restart",
        "params":{"host":"alpha","unit":"demo.service"}
    });
    let (status, submitted) = call_with_idempotency(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("service-restart"),
        allowed,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let job_id = submitted["job_id"].as_str().unwrap();
    for _ in 0..20 {
        let (_, status) = call(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":"jobs.status","params":{"job_id":job_id}}),
        )
        .await;
        if status["handle"]["state"] == "succeeded" {
            assert_eq!(status["handle"]["operation"], "units.restart");
            task.abort();
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("service job did not complete");
}

#[tokio::test]
async fn service_submission_rejects_unit_outside_manageable_inventory() {
    let target_directory = tempfile::tempdir().unwrap();
    let target_store = Store::open(&target_directory.path().join("target.db"))
        .await
        .unwrap();
    let (url, task) = stub(
        Router::new()
            .route("/v1/manage", post(successful_executor))
            .with_state(target_store),
    )
    .await;
    let hub_directory = tempfile::tempdir().unwrap();
    let router = router(Arc::new(
        management_app(&url, &hub_directory.path().join("hub.db")).await,
    ));
    let (status, _) = call_with_idempotency(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        Some("forbidden-service"),
        json!({
            "op":"units.restart",
            "params":{"host":"alpha","unit":"secret.service"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    task.abort();
}

#[tokio::test]
async fn lost_submit_ack_retries_same_job_without_duplicate_target_execution() {
    let target_directory = tempfile::tempdir().unwrap();
    let target_store = Store::open(&target_directory.path().join("target.db"))
        .await
        .unwrap();
    let submissions = StdArc::new(AtomicUsize::new(0));
    let (url, task) = stub(
        Router::new()
            .route("/v1/manage", post(executor_with_lost_first_ack))
            .with_state(AckLossExecutor {
                store: target_store,
                submissions: submissions.clone(),
            }),
    )
    .await;
    let hub_directory = tempfile::tempdir().unwrap();
    let router = router(Arc::new(
        management_app(&url, &hub_directory.path().join("hub.db")).await,
    ));
    let body = json!({
        "op":"exec.run",
        "params":{
            "host":"alpha",
            "profile":"diagnostic",
            "command":{"argv":["/bin/true"]},
            "timeout_seconds":30
        }
    });
    let (_, submitted) = call_with_idempotency(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("lost-ack"),
        body,
    )
    .await;
    let job_id = submitted["job_id"].as_str().unwrap();
    let mut observed = Value::Null;
    for _ in 0..40 {
        let (_, value) = call(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":"jobs.status","params":{"job_id":job_id}}),
        )
        .await;
        observed = value;
        if observed["handle"]["state"] == "succeeded" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(observed["handle"]["state"], "succeeded");
    assert_eq!(submissions.load(Ordering::SeqCst), 2);
    task.abort();
}

#[tokio::test]
async fn late_target_evidence_resolves_outcome_unknown() {
    let hub_directory = tempfile::tempdir().unwrap();
    let target_directory = tempfile::tempdir().unwrap();
    let hub = Store::open(&hub_directory.path().join("hub.db"))
        .await
        .unwrap();
    let target = Store::open(&target_directory.path().join("target.db"))
        .await
        .unwrap();
    let request = NewJob {
        principal: "manager".into(),
        host: "alpha".into(),
        operation: "exec.run".into(),
        spec_version: 1,
        spec: json!({"host":"alpha","profile":"diagnostic","command":{"argv":["/bin/true"]}}),
        policy_version: "test".into(),
        deadline: None,
    };
    let submitted = hub.submit_job("late-evidence", &request).await.unwrap().job;
    let dispatching = hub
        .transition_job(
            &submitted.handle.job_id,
            submitted.handle.revision,
            JobState::Dispatching,
            &json!({}),
            None,
        )
        .await
        .unwrap();
    let reconciling = hub
        .transition_job(
            &submitted.handle.job_id,
            dispatching.handle.revision,
            JobState::Reconciling,
            &json!({}),
            None,
        )
        .await
        .unwrap();
    let unknown = hub
        .transition_job(
            &submitted.handle.job_id,
            reconciling.handle.revision,
            JobState::OutcomeUnknown,
            &json!({}),
            Some(&json!({"effect":"unknown"})),
        )
        .await
        .unwrap();

    let accepted = target
        .accept_job(&submitted.handle.job_id, &request)
        .await
        .unwrap()
        .job;
    let target_dispatching = target
        .transition_job(
            &accepted.handle.job_id,
            accepted.handle.revision,
            JobState::Dispatching,
            &json!({}),
            None,
        )
        .await
        .unwrap();
    let succeeded = target
        .transition_job(
            &accepted.handle.job_id,
            target_dispatching.handle.revision,
            JobState::Succeeded,
            &json!({}),
            Some(&json!({"exit_code":0})),
        )
        .await
        .unwrap();

    let resolved = project_target_job(&hub, unknown, succeeded).await.unwrap();
    assert_eq!(resolved.handle.state, JobState::Succeeded);
    assert_eq!(resolved.result.unwrap()["exit_code"], 0);
}

#[tokio::test]
async fn catalog_serves_shared_protocol_metadata() {
    let router = router(Arc::new(app(
        "http://127.0.0.1:1",
        &["host:read", "units:read"],
    )));
    let (status, catalog) = get_json(router, "/v1/operations", Some(USER_TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(catalog["version"], PROTOCOL_VERSION);
    for operation in catalog["operations"].as_array().unwrap() {
        assert_eq!(operation["kind"], "observation");
        assert_eq!(operation["read_only"], true);
        assert!(operation["params_schema"].is_object());
        assert!(operation["response_schema"].is_object());
        assert_eq!(operation["minimum_protocol_version"], 1);
        assert_eq!(operation["idempotency"], "none");
    }
}

#[tokio::test]
async fn expanded_operations_preserve_scope_and_unknown_deployment_time() {
    let (url, task) = stub(Router::new()
        .route("/v1/snapshot", get(|| async { Json(snapshot("alpha")) }))
        .route("/v1/unit", post(|headers: HeaderMap, Json(params): Json<maxops_proto::UnitParams>| async move {
            assert!(Token::parse(AGENT_TOKEN.into()).unwrap().matches(&headers));
            assert_eq!(params.host, "alpha");
            assert_eq!(params.unit, "demo.service");
            let mut unit = snapshot("alpha")["units"][0].clone();
            unit["details"] = json!({"main_pid": 12, "memory_current_bytes": null, "restarts": 3, "exec_main_code": 1, "exec_main_status": 2});
            Json(json!({"host": "alpha", "observed_at": now(), "unit": unit}))
        }))).await;
    let router = router(Arc::new(app(
        &url,
        &["units:read", "host:read", "metrics:read"],
    )));
    for operation in ["units.list", "host.metrics", "deploy.status"] {
        assert_eq!(
            call(
                router.clone(),
                "/v1/execute",
                Some(USER_TOKEN),
                json!({"op": operation, "params": {"host": "private"}})
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
    }
    let (status, units) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"units.list", "params":{"host":"alpha"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(units["units"].as_array().unwrap().len(), 1);
    let (status, detail) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"units.status", "params":{"host":"alpha", "unit":"demo.service"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["unit"]["details"]["restarts"], 3);
    let (status, deployment) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"deploy.status", "params":{}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        deployment["hosts"][0]["running_closure"],
        "/nix/store/test-system"
    );
    assert!(deployment["hosts"][0]["activated_at"].is_null());
    assert!(deployment["hosts"][0]["profile_generation"].is_null());
    let (status, metrics) = call(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"host.metrics", "params":{"host":"alpha"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(metrics["observation"]["state"], "not_configured");
    task.abort();
}

#[tokio::test]
async fn service_details_reject_wrong_identity_and_stale_observations() {
    for (host, time) in [
        ("private", now()),
        ("alpha", now() - std::time::Duration::from_secs(100)),
    ] {
        let (url, task) = stub(Router::new().route("/v1/unit", post(move || async move {
            Json(json!({"host": host, "observed_at": time, "unit": snapshot("alpha")["units"][0]}))
        }))).await;
        let router = router(Arc::new(app(&url, &["units:read"])));
        assert_eq!(
            call(
                router,
                "/v1/execute",
                Some(USER_TOKEN),
                json!({"op":"units.status", "params":{"host":"alpha", "unit":"demo.service"}})
            )
            .await
            .0,
            StatusCode::BAD_GATEWAY
        );
        task.abort();
    }
}

#[tokio::test]
async fn metrics_queries_are_fixed_and_host_scoped_over_http() {
    let (url, task) = stub(Router::new().route("/api/v1/query", get(|axum::extract::Query(params): axum::extract::Query<BTreeMap<String, String>>| async move {
        assert_eq!(params["timeout"], "5s");
        assert!(params["query"].contains("instance=\"alpha\""));
        let value = if params["query"].contains("timestamp(") { now().as_second().to_string() } else { "2.5".into() };
        Json(json!({"status":"success", "data":{"resultType":"vector", "result":[{"metric":{"instance":"alpha", "job":"node", "maxops_metric":"load1"}, "value":[0, value]}]}}))
    }))).await;
    let mut state = app("http://127.0.0.1:1", &["metrics:read"]);
    state.prometheus_url = Some(url);
    let router = router(Arc::new(state));
    let (status, result) = call(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"host.metrics", "params":{"host":"alpha"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        result["observation"]["metrics"]["load1"]["samples"][0]["value"],
        2.5
    );
    task.abort();
}

#[tokio::test]
async fn authorization_blocks_identity_spoofing_and_scope_escape() {
    let router = router(Arc::new(app("http://127.0.0.1:1", &["host:read"])));
    let body = json!({"op":"host.facts", "params":{"host":"alpha"}});
    assert_eq!(
        call(router.clone(), "/v1/execute", None, body.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(router.clone(), "/v1/execute", Some(ALERT_TOKEN), body)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":"host.facts","params":{"host":"private"}})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":"units.logs","params":{"host":"alpha","unit":"demo.service"}})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            router,
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":"host.facts","params":{"host":"alpha","uid":"admin"}})
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn failed_units_filter_inventory_and_agent_output() {
    let (url, task) = stub(Router::new().route(
        "/v1/snapshot",
        get(|headers: HeaderMap| async move {
            assert!(Token::parse(AGENT_TOKEN.into()).unwrap().matches(&headers));
            Json(snapshot("alpha"))
        }),
    ))
    .await;
    let router = router(Arc::new(app(&url, &["units:read"])));
    let (status, result) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"units.failed","params":{}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["hosts"].as_array().unwrap().len(), 1);
    assert_eq!(result["hosts"][0]["units"].as_array().unwrap().len(), 1);
    assert_eq!(result["hosts"][0]["units"][0]["unit"], "demo.service");
    assert_eq!(
        call(
            router,
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":"units.status","params":{"host":"alpha","unit":"secret.service"}})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    task.abort();
}

#[tokio::test]
async fn unreachable_and_wrong_agent_are_never_reported_healthy() {
    let (url, task) = stub(Router::new().route(
        "/v1/snapshot",
        get(|| async { Json(snapshot("wrong-host")) }),
    ))
    .await;
    for endpoint in [&url, "http://127.0.0.1:1"] {
        let router = router(Arc::new(app(endpoint, &["fleet:read", "host:read"])));
        let (_, result) = call(
            router.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":"fleet.overview","params":{}}),
        )
        .await;
        assert_eq!(result["hosts"][0]["agent"]["state"], "unavailable");
        assert!(result["hosts"][0]["agent"]["failed_units"].is_null());
        assert_eq!(
            call(
                router,
                "/v1/execute",
                Some(USER_TOKEN),
                json!({"op":"host.facts","params":{"host":"alpha"}})
            )
            .await
            .0,
            StatusCode::BAD_GATEWAY
        );
    }
    task.abort();
}

#[tokio::test]
async fn oversized_or_stale_observations_are_rejected() {
    let (url, task) = stub(Router::new().route(
        "/v1/snapshot",
        get(|| async {
            let mut value = snapshot("alpha");
            value["observed_at"] = json!("1970-01-01T00:00:01Z");
            Json(value)
        }),
    ))
    .await;
    assert!(
        observe(&app(&url, &[]), app(&url, &[]).hosts.get("alpha").unwrap())
            .await
            .is_err()
    );
    task.abort();
    let (url, task) = stub(Router::new().route(
        "/v1/snapshot",
        get(|| async { "x".repeat(transport::MAX_BODY + 1) }),
    ))
    .await;
    assert!(
        observe(&app(&url, &[]), app(&url, &[]).hosts.get("alpha").unwrap())
            .await
            .is_err()
    );
    task.abort();
}

#[tokio::test]
async fn alert_delivery_requires_separate_auth_and_acknowledgement() {
    let (url, task) = stub(Router::new().route(
        "/sink",
        post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
    ))
    .await;
    let mut state = app("http://127.0.0.1:1", &[]);
    state.alert_ingress = Some(AlertIngress {
        token: Token::parse(ALERT_TOKEN.into()).unwrap(),
        sink_url: format!("{url}/sink"),
        sink_token: None,
    });
    let router = router(Arc::new(state));
    let body = json!({"version":"4","status":"firing","alerts":[]});
    assert_eq!(
        call(router.clone(), "/v1/alerts", Some(USER_TOKEN), body.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(router, "/v1/alerts", Some(ALERT_TOKEN), body).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    task.abort();
}

#[tokio::test]
async fn alert_success_forwards_payload_without_ingress_credentials() {
    let body = json!({"version":"4","status":"resolved","alerts":[]});
    let expected = body.clone();
    let (url, task) = stub(Router::new().route(
        "/sink",
        post(move |headers: HeaderMap, Json(value): Json<Value>| {
            let expected = expected.clone();
            async move {
                assert!(!headers.contains_key("authorization"));
                assert_eq!(value, expected);
                StatusCode::OK
            }
        }),
    ))
    .await;
    let mut state = app("http://127.0.0.1:1", &[]);
    state.alert_ingress = Some(AlertIngress {
        token: Token::parse(ALERT_TOKEN.into()).unwrap(),
        sink_url: format!("{url}/sink"),
        sink_token: None,
    });
    assert_eq!(
        call(
            router(Arc::new(state)),
            "/v1/alerts",
            Some(ALERT_TOKEN),
            body
        )
        .await
        .0,
        StatusCode::OK
    );
    task.abort();
}

#[tokio::test]
async fn active_alerts_do_not_leak_other_hosts_or_unscoped_alerts() {
    let (url, task) = stub(Router::new().route("/api/v2/alerts", get(|| async { Json(json!([
        {"labels":{"instance":"alpha"}}, {"labels":{"instance":"private"}}, {"labels":{"alertname":"fleet-wide"}}
    ])) }))).await;
    let mut state = app("http://127.0.0.1:1", &["alerts:read"]);
    state.alertmanager_url = Some(url);
    let (status, body) = call(
        router(Arc::new(state)),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"alerts.active","params":{}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["alerts"].as_array().unwrap().len(), 1);
    task.abort();
}

#[tokio::test]
async fn exporter_freshness_uses_scrape_time_not_query_time() {
    let (url, task) =
        stub(
            Router::new().route(
                "/api/v1/query",
                get(
                    |axum::extract::Query(query): axum::extract::Query<
                        BTreeMap<String, String>,
                    >| async move {
                        let value = if query["query"].starts_with("timestamp(") {
                            "1"
                        } else {
                            "0"
                        };
                        Json(
                            json!({"status":"success", "data":{"resultType":"vector", "result":[
                                {"metric":{"instance":"alpha"},"value":[now().as_second(), value]}
                            ]}}),
                        )
                    },
                ),
            ),
        )
        .await;
    let mut state = app("http://127.0.0.1:1", &[]);
    state.prometheus_url = Some(url);
    let (status, samples) = exporter_samples(&state).await;
    assert_eq!(status, "available");
    assert_eq!(samples["alpha"]["state"], "stale");
    assert_eq!(samples["alpha"]["sample_at_unix_seconds"], 1.0);
    task.abort();
}

#[test]
fn openapi_contains_all_registry_operations() {
    let value = serde_json::to_value(ApiDoc::openapi()).unwrap();
    let schema = value["components"]["schemas"]["Request"].to_string();
    for op in operations() {
        assert!(schema.contains(op.name), "missing {} in {}", op.name, value);
    }
    assert!(value["components"]["securitySchemes"]["bearer"].is_object());
}

#[tokio::test]
async fn alert_events_keep_episode_identity_and_are_replayable() {
    let (sink_url, sink_task) = stub(Router::new().route(
        "/hook",
        post(|| async { (StatusCode::OK, Json(json!({"accepted":true}))) }),
    ))
    .await;
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(&directory.path().join("hub.db")).await.unwrap();
    let mut state = app("http://127.0.0.1:1", &["events:read", "self:read"]);
    state.store = Some(store);
    state.alert_ingress = Some(AlertIngress {
        token: Token::parse(ALERT_TOKEN.into()).unwrap(),
        sink_url: format!("{sink_url}/hook"),
        sink_token: None,
    });
    let router = router(Arc::new(state));
    let mut payload = json!({
        "version":"4",
        "status":"firing",
        "alerts":[{
            "status":"firing",
            "fingerprint":"same-alert",
            "startsAt":now(),
            "labels":{"instance":"alpha","alertname":"FixtureDown"}
        }]
    });
    for _ in 0..2 {
        assert_eq!(
            call(
                router.clone(),
                "/v1/alerts",
                Some(ALERT_TOKEN),
                payload.clone()
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    payload["status"] = json!("resolved");
    payload["alerts"][0]["status"] = json!("resolved");
    payload["alerts"][0]["endsAt"] = json!(now());
    assert_eq!(
        call(
            router.clone(),
            "/v1/alerts",
            Some(ALERT_TOKEN),
            payload.clone()
        )
        .await
        .0,
        StatusCode::OK
    );
    payload["status"] = json!("firing");
    payload["alerts"][0]["status"] = json!("firing");
    assert_eq!(
        call(router.clone(), "/v1/alerts", Some(ALERT_TOKEN), payload)
            .await
            .0,
        StatusCode::OK
    );
    let (status, events) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"events.list","params":{}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events = events["events"].as_array().unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0]["episode_id"], events[1]["episode_id"]);
    assert_eq!(events[1]["episode_id"], events[2]["episode_id"]);
    assert_ne!(events[2]["episode_id"], events[3]["episode_id"]);
    assert_eq!(events[2]["kind"], "alert_resolved");
    assert_eq!(events[3]["kind"], "alert_firing");
    assert_eq!(get_json(router, "/readyz", None).await.0, StatusCode::OK);
    sink_task.abort();
}

#[tokio::test]
async fn event_delivery_retries_and_preserves_http_202_as_accepted() {
    let attempts = StdArc::new(AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    let (sink_url, sink_task) = stub(Router::new().route(
        "/events",
        post(move |Json(_event): Json<Value>| {
            let handler_attempts = handler_attempts.clone();
            async move {
                if handler_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"retry":true})))
                } else {
                    (StatusCode::ACCEPTED, Json(json!({"received":true})))
                }
            }
        }),
    ))
    .await;
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(&directory.path().join("hub.db")).await.unwrap();
    store
        .ensure_subscription("automation", &json!({}), &json!({}), None)
        .await
        .unwrap();
    let event = store
        .ingest_alert(AlertEventInput {
            source: "alertmanager".into(),
            fingerprint: "delivery".into(),
            host: "alpha".into(),
            firing: true,
            occurred_at: now(),
            payload: json!({"status":"firing"}),
        })
        .await
        .unwrap();
    let mut state = app("http://127.0.0.1:1", &[]);
    state.store = Some(store.clone());
    state.event_sinks.push(EventSink {
        config: EventSinkConfig {
            id: "automation".into(),
            url: format!("{sink_url}/events"),
            token_file: None,
            hosts: BTreeSet::new(),
            kinds: BTreeSet::new(),
            retry_seconds: 1,
        },
        token: None,
    });
    let state = Arc::new(state);
    spawn_event_delivery(state.clone());
    for _ in 0..40 {
        if store
            .subscription_cursor("automation")
            .await
            .unwrap()
            .cursor
            == event.sequence
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        store
            .subscription_cursor("automation")
            .await
            .unwrap()
            .cursor,
        event.sequence
    );
    let delivery = store
        .delivery_record("automation", event.sequence)
        .await
        .unwrap();
    assert_eq!(delivery.stage, DeliveryStage::Accepted);
    assert_eq!(delivery.attempts, 2);
    assert_eq!(state.delivery_attempts_total.load(Ordering::Relaxed), 2);
    sink_task.abort();
}

#[tokio::test]
async fn diagnostics_and_remediation_form_a_scoped_budgeted_flow() {
    let target_directory = tempfile::tempdir().unwrap();
    let target_store = Store::open(&target_directory.path().join("target.db"))
        .await
        .unwrap();
    let (url, target_task) = stub(
        Router::new()
            .route("/v1/manage", post(successful_executor))
            .route("/v1/snapshot", get(|| async { Json(snapshot("alpha")) }))
            .route(
                "/v1/logs",
                post(|| async {
                    Json(json!({"host":"alpha","observed_at":now(),"entries":[{"message":"fixture failure"}]}))
                }),
            )
            .with_state(target_store),
    )
    .await;
    let hub_directory = tempfile::tempdir().unwrap();
    let mut state = management_app(&url, &hub_directory.path().join("hub.db")).await;
    state.clients[0].capabilities.extend([
        "diagnostics:collect".into(),
        "remediations:manage".into(),
        "events:read".into(),
        "self:read".into(),
    ]);
    state.hosts.get_mut("alpha").unwrap().config.readable_units =
        BTreeSet::from(["demo.service".into()]);
    state
        .hosts
        .get_mut("alpha")
        .unwrap()
        .config
        .diagnostic_profile = Some("diagnostic".into());
    state
        .hosts
        .get_mut("alpha")
        .unwrap()
        .config
        .diagnostic_probes = BTreeMap::from([("identity".into(), vec!["/bin/true".into()])]);
    state.remediation_policy = RemediationPolicyConfig {
        max_attempts_per_episode: 1,
        cooldown_seconds: 0,
    };
    let store = state.store.as_ref().unwrap().clone();
    let alert = store
        .ingest_alert(AlertEventInput {
            source: "alertmanager".into(),
            fingerprint: "diagnose".into(),
            host: "alpha".into(),
            firing: true,
            occurred_at: now(),
            payload: json!({"status":"firing"}),
        })
        .await
        .unwrap();
    let router = router(Arc::new(state));
    let (status, submitted) = call_with_idempotency(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("diagnostic-flow"),
        json!({"op":"diagnostics.collect","params":{
            "host":"alpha","event_id":alert.event_id,"unit":"demo.service","probes":["identity"]
        }}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let diagnostic_id = JobId::parse(submitted["job_id"].as_str().unwrap()).unwrap();
    let diagnostic = loop {
        let job = store.get_job(&diagnostic_id).await.unwrap();
        if job.handle.state.is_terminal() {
            break job;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(diagnostic.handle.state, JobState::Succeeded);
    assert_eq!(
        diagnostic.result.as_ref().unwrap()["diagnostic"]["host"],
        "alpha"
    );
    assert_eq!(
        diagnostic.result.as_ref().unwrap()["diagnostic"]["rules"][0]["conclusion"],
        "selected unit is failed"
    );
    let (_, events) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"events.list","params":{"kinds":["diagnostic_collected"]}}),
    )
    .await;
    assert_eq!(
        events["events"][0]["episode_id"],
        alert.episode_id.to_string()
    );

    let (status, claim) = call_with_idempotency(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("remediation-1"),
        json!({"op":"remediations.begin","params":{"event_id":alert.event_id,"host":"alpha"}}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let claim_id = JobId::parse(claim["job_id"].as_str().unwrap()).unwrap();
    let claim = loop {
        let job = store.get_job(&claim_id).await.unwrap();
        if job.handle.state.is_terminal() {
            break job;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(claim.handle.state, JobState::Succeeded);
    let remediation = &claim.result.as_ref().unwrap()["remediation"];
    let remediation_id = remediation["remediation_id"].as_str().unwrap();
    let (status, finished) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"remediations.finish","params":{
            "remediation_id":remediation_id,"expected_revision":1,"outcome":"succeeded",
            "related_job_id":diagnostic_id,"summary":"evidence collected and repair verified"
        }}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(finished["state"], "succeeded");

    let (_, second) = call_with_idempotency(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        Some("remediation-2"),
        json!({"op":"remediations.begin","params":{"event_id":alert.event_id,"host":"alpha"}}),
    )
    .await;
    let second_id = JobId::parse(second["job_id"].as_str().unwrap()).unwrap();
    let second = loop {
        let job = store.get_job(&second_id).await.unwrap();
        if job.handle.state.is_terminal() {
            break job;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(second.handle.state, JobState::Failed);
    assert_eq!(
        second.result.unwrap()["error"],
        "remediation_budget_exhausted"
    );
    target_task.abort();
}

#[tokio::test]
async fn exporter_matches_timestamp_labels_without_metric_name() {
    let scraped_at = now().as_second() - 5;
    let (url, task) = stub(Router::new().route(
        "/api/v1/query",
        get(move |axum::extract::Query(query): axum::extract::Query<BTreeMap<String, String>>| async move {
            let result = if query["query"].starts_with("timestamp(") {
                json!([
                    {"metric":{"instance":"alpha","job":"other"},"value":[now().as_second(),"1"]},
                    {"metric":{"instance":"alpha","job":"node"},"value":[now().as_second(),scraped_at.to_string()]}
                ])
            } else {
                json!([
                    {"metric":{"__name__":"up","instance":"alpha","job":"node"},"value":[now().as_second(),"1"]}
                ])
            };
            Json(json!({"status":"success","data":{"resultType":"vector","result":result}}))
        }),
    )).await;
    let mut state = app("http://127.0.0.1:1", &[]);
    state.prometheus_url = Some(url);
    let (status, samples) = exporter_samples(&state).await;
    assert_eq!(status, "available");
    assert_eq!(samples["alpha"]["state"], "up");
    assert_eq!(
        samples["alpha"]["sample_at_unix_seconds"],
        scraped_at as f64
    );
    task.abort();
}
