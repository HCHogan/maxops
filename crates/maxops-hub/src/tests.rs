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

#[tokio::test]
async fn compact_catalog_pages_are_complete_scoped_and_revision_bound() {
    let router = router(Arc::new(app(
        "http://127.0.0.1:1",
        &["host:read", "units:read"],
    )));
    let (_, full) = get_json(router.clone(), "/v1/operations", Some(USER_TOKEN)).await;
    let (_, tools) = get_json(
        router.clone(),
        "/v1/operations?view=tools",
        Some(USER_TOKEN),
    )
    .await;
    assert_eq!(full["total"], tools["total"]);
    assert!(full["operations"][0]["params_schema"]["$schema"].is_string());
    assert!(full["operations"][0]["params_schema"]["title"].is_string());
    for operation in tools["operations"].as_array().unwrap() {
        assert!(operation["params_schema"].is_object());
        assert!(operation["params_schema"].get("$schema").is_none());
        assert!(operation["params_schema"].get("title").is_none());
        assert!(operation.get("response_schema").is_none());
        assert_ne!(operation["name"], "exec.run");
    }
    let (_, first) = get_json(
        router.clone(),
        "/v1/operations?view=summary&limit=1",
        Some(USER_TOKEN),
    )
    .await;
    assert_eq!(first["operations"].as_array().unwrap().len(), 1);
    assert!(first["operations"][0].get("params_schema").is_none());
    let cursor = first["next_cursor"].as_str().unwrap();
    let (status, next) = get_json(
        router.clone(),
        &format!("/v1/operations?view=summary&limit=1&cursor={cursor}"),
        Some(USER_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(
        first["operations"][0]["name"],
        next["operations"][0]["name"]
    );
    let (status, error) = get_json(
        router,
        &format!("/v1/operations?view=tools&cursor={cursor}"),
        Some(USER_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error["code"], "cursor_invalid");
}

#[tokio::test]
async fn resource_discovery_never_returns_private_hosts_or_ungranted_units() {
    let router = router(Arc::new(app(
        "http://127.0.0.1:1",
        &["self:read", "units:read"],
    )));
    let (_, hosts) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"resources.list","params":{}}),
    )
    .await;
    assert_eq!(hosts["resources"].as_array().unwrap().len(), 1);
    assert_eq!(hosts["resources"][0]["host"], "alpha");
    let (_, units) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"resources.list","params":{"kind":"units"}}),
    )
    .await;
    assert_eq!(units["resources"][0]["unit"], "demo.service");
    assert_eq!(units["resources"][0]["manageable"], false);
    let (status, _) = call(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"resources.list","params":{"host":"private"}}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

fn fixture_job(principal: &str, host: &str) -> NewJob {
    NewJob {
        principal: principal.into(),
        host: host.into(),
        operation: "exec.run".into(),
        spec_version: 1,
        spec: json!({"secret_fixture":"never in compact status"}),
        policy_version: "test".into(),
        deadline: None,
    }
}

#[tokio::test]
async fn wait_wakes_on_committed_revision_without_using_execution_slots() {
    let directory = tempfile::tempdir().unwrap();
    let mut state = management_app("http://127.0.0.1:1", &directory.path().join("hub.db")).await;
    state.slots = Semaphore::new(0);
    let store = state.store.as_ref().unwrap().clone();
    let job = store
        .submit_job("wait", &fixture_job("manager", "alpha"))
        .await
        .unwrap()
        .job;
    let id = job.handle.job_id;
    let router = router(Arc::new(state));
    let waiting = tokio::spawn(call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"jobs.wait","params":{"job_id":id,"after_revision":1,"timeout_seconds":10}}),
    ));
    tokio::task::yield_now().await;
    store
        .transition_job(
            &id,
            1,
            JobState::Cancelled,
            &json!({}),
            Some(&json!({"cancelled":true})),
        )
        .await
        .unwrap();
    let (status, result) = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["job"]["handle"]["state"], "cancelled");
    assert!(result["job"].get("spec").is_none());
    let (status, _) = call(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"jobs.status","params":{"job_id":id}}),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn job_pages_events_and_results_keep_scope_and_bounds() {
    let directory = tempfile::tempdir().unwrap();
    let state = management_app("http://127.0.0.1:1", &directory.path().join("hub.db")).await;
    let store = state.store.as_ref().unwrap().clone();
    let mut ids = Vec::new();
    for index in 0..3 {
        let job = store
            .submit_job(&format!("page-{index}"), &fixture_job("manager", "alpha"))
            .await
            .unwrap()
            .job;
        store
            .transition_job(
                &job.handle.job_id,
                1,
                JobState::Cancelled,
                &json!({}),
                Some(&json!({"large":"你好".repeat(20000)})),
            )
            .await
            .unwrap();
        ids.push(job.handle.job_id);
    }
    let foreign = store
        .submit_job("foreign", &fixture_job("another", "alpha"))
        .await
        .unwrap()
        .job;
    let router = router(Arc::new(state));
    let (_, first) = call(
        router.clone(),
        "/v1/execute?view=summary",
        Some(USER_TOKEN),
        json!({"op":"jobs.list","params":{"limit":2}}),
    )
    .await;
    assert_eq!(first["jobs"].as_array().unwrap().len(), 2);
    assert!(first["jobs"][0].get("spec").is_none());
    assert_eq!(first["jobs"][0]["result"], Value::Null);
    let (_, second) = call(
        router.clone(),
        "/v1/execute?view=summary",
        Some(USER_TOKEN),
        json!({"op":"jobs.list","params":{"limit":2,"cursor":first["next_cursor"]}}),
    )
    .await;
    assert_eq!(second["jobs"].as_array().unwrap().len(), 1);
    assert!(second["next_cursor"].is_null());
    let (_, events) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"jobs.events","params":{"job_id":ids[0],"limit":1}}),
    )
    .await;
    assert_eq!(events["events"].as_array().unwrap().len(), 1);
    let (_, later) = call(router.clone(), "/v1/execute", Some(USER_TOKEN), json!({"op":"jobs.events","params":{"job_id":ids[0],"after_sequence":events["next_sequence"]}})).await;
    assert_eq!(later["events"][0]["state"], "cancelled");
    let (_, fragment) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"jobs.result","params":{"job_id":ids[0],"pointer":"/large","limit":10}}),
    )
    .await;
    assert!(fragment["text"].as_str().unwrap().len() <= 10);
    assert_eq!(fragment["complete"], false);
    let (status, _) = call(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"jobs.events","params":{"job_id":foreign.handle.job_id}}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn invalid_representation_rejects_before_submitting_a_job() {
    let directory = tempfile::tempdir().unwrap();
    let state = management_app("http://127.0.0.1:1", &directory.path().join("hub.db")).await;
    let store = state.store.as_ref().unwrap().clone();
    let (status, _) = call_with_idempotency(router(Arc::new(state)), "/v1/execute?view=invalid", Some(USER_TOKEN), Some("invalid-view"),
        json!({"op":"exec.run","params":{"host":"alpha","profile":"diagnostic","command":{"argv":["true"]}}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        store
            .get_idempotent_job("manager", "invalid-view")
            .await
            .unwrap()
            .is_none()
    );
}

async fn prepared_change(app: &Arc<App>, key: &str) -> ChangeRecord {
    let (status, handle) = call_with_idempotency(router(app.clone()), "/v1/execute", Some(USER_TOKEN), Some(key),
        json!({"op":"deploy.prepare","params":{"repository":"fixture","workspace_id":"11111111-1111-1111-1111-111111111111",
          "expected_revision":4,"target_host":"alpha","profile":"fixture-system"}})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{handle}");
    let id: ChangeId = serde_json::from_value(handle["job_id"].clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match refresh_change(app, &app.clients[0], &id).await {
                Ok(change) if change.state == ChangeState::Prepared => return change,
                Ok(_) => {}
                // The dispatch worker may commit the same observed revision
                // first. Re-read this observation; never replay a submission.
                Err(error) if error.0 == StatusCode::CONFLICT => {}
                Err(error) => panic!("prepare observation failed: {error:?}"),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

async fn terminal_job(app: &Arc<App>, id: &JobId) -> JobRecord {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let job = app.store.as_ref().unwrap().get_job(id).await.unwrap();
            if job.handle.state.is_terminal() {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn deployment_workflow_runs_fixed_stages_and_reuses_submission_identity() {
    for (until, expected, stages) in [
        ("built", ChangeState::Ready, 2),
        ("verified", ChangeState::Succeeded, 4),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let target = Store::open(&directory.path().join("target.db"))
            .await
            .unwrap();
        let (url, server) = stub(
            Router::new()
                .route("/v1/manage", post(deployment_executor))
                .with_state(DeploymentExecutor {
                    store: target.clone(),
                    remote_head: StdArc::new(StdMutex::new("a".repeat(40))),
                    runtime: StdArc::new(StdMutex::new("/nix/store/base-system".into())),
                }),
        )
        .await;
        let app = Arc::new(deployment_management_app(&url, &directory.path().join("hub.db")).await);
        let change = prepared_change(&app, "prepare").await;
        let body = json!({"op":"deploy.run","params":{"change_id":change.plan.change_id,"expected_revision":change.revision,"until":until}});
        let (status, first) = call_with_idempotency(
            router(app.clone()),
            "/v1/execute",
            Some(USER_TOKEN),
            Some("workflow"),
            body.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{first}");
        let (_, replay) = call_with_idempotency(
            router(app.clone()),
            "/v1/execute",
            Some(USER_TOKEN),
            Some("workflow"),
            body,
        )
        .await;
        assert_eq!(first["job_id"], replay["job_id"]);
        let id = serde_json::from_value(first["job_id"].clone()).unwrap();
        let terminal = terminal_job(&app, &id).await;
        assert_eq!(
            terminal.handle.state,
            JobState::Succeeded,
            "{:?}",
            terminal.result
        );
        let completed = app
            .store
            .as_ref()
            .unwrap()
            .get_owned_change("manager", &change.plan.change_id)
            .await
            .unwrap();
        assert_eq!(completed.state, expected);
        assert_eq!(
            target
                .list_jobs(Some("manager"), None, &[], 20)
                .await
                .unwrap()
                .len(),
            stages
        );
        server.abort();
    }
}

#[tokio::test]
async fn workflow_ownership_and_stage_link_survive_reopen_without_orphaned_jobs() {
    let directory = tempfile::tempdir().unwrap();
    let target = Store::open(&directory.path().join("target.db"))
        .await
        .unwrap();
    let (url, server) = stub(
        Router::new()
            .route("/v1/manage", post(deployment_executor))
            .with_state(DeploymentExecutor {
                store: target.clone(),
                remote_head: StdArc::new(StdMutex::new("a".repeat(40))),
                runtime: StdArc::new(StdMutex::new("/nix/store/base-system".into())),
            }),
    )
    .await;
    let state_path = directory.path().join("hub.db");
    let app = Arc::new(deployment_management_app(&url, &state_path).await);
    let change = prepared_change(&app, "prepare").await;
    let params = maxops_proto::DeployRunParams {
        change_id: change.plan.change_id.clone(),
        expected_revision: change.revision,
        until: maxops_proto::DeployUntil::Built,
    };
    let id = JobId::parse(uuid::Uuid::now_v7().to_string()).unwrap();
    let mut spec = fixture_job("manager", "alpha");
    spec.operation = "deploy.run".into();
    spec.spec = json!(params);
    let store = app.store.as_ref().unwrap();
    let parent = store
        .submit_linked_job(
            "workflow",
            &id,
            &spec,
            Some(maxops_store::ChangeJobLink {
                change: &change,
                next: ChangeState::Prepared,
                action: None,
                workflow: None,
            }),
        )
        .await
        .unwrap()
        .job;
    let claimed = store
        .get_owned_change("manager", &change.plan.change_id)
        .await
        .unwrap();
    let (status, _) = call_with_idempotency(router(app.clone()), "/v1/execute", Some(USER_TOKEN), Some("competing"),
        json!({"op":"deploy.build","params":{"change_id":change.plan.change_id,"expected_revision":claimed.revision}})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        store
            .get_idempotent_job("manager", "competing")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .list_jobs(Some("manager"), None, &[], 20)
            .await
            .unwrap()
            .len(),
        2
    );
    let running = enter_local_job(store, parent).await.unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        "idempotency-key",
        format!("workflow:{id}:deploy.build").parse().unwrap(),
    );
    submit_change_stage_owned(
        &app,
        &app.clients[0],
        &headers,
        DeployChangeParams {
            change_id: change.plan.change_id.clone(),
            expected_revision: claimed.revision,
        },
        DeploymentAction::Build,
        Some(&id),
    )
    .await
    .unwrap();
    // Reconstruct a fresh Hub owner from the durable database after the stage
    // was linked. The existing child identity must be observed, never replaced.
    let reopened = Arc::new(deployment_management_app(&url, &state_path).await);
    deployment_workflow::spawn(reopened.clone(), running);
    assert_eq!(
        terminal_job(&reopened, &id).await.handle.state,
        JobState::Succeeded
    );
    assert_eq!(
        target
            .list_jobs(Some("manager"), None, &[], 20)
            .await
            .unwrap()
            .len(),
        2
    );
    server.abort();
}

#[tokio::test]
async fn cancelled_workflow_never_starts_a_stage_and_unknown_only_reconciles() {
    for cancelled in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let target = Store::open(&directory.path().join("target.db"))
            .await
            .unwrap();
        let (url, server) = stub(
            Router::new()
                .route("/v1/manage", post(deployment_executor))
                .with_state(DeploymentExecutor {
                    store: target.clone(),
                    remote_head: StdArc::new(StdMutex::new("a".repeat(40))),
                    runtime: StdArc::new(StdMutex::new("/nix/store/base-system".into())),
                }),
        )
        .await;
        let app = Arc::new(deployment_management_app(&url, &directory.path().join("hub.db")).await);
        let change = prepared_change(&app, "prepare").await;
        let store = app.store.as_ref().unwrap();
        let id = JobId::parse(uuid::Uuid::now_v7().to_string()).unwrap();
        let mut spec = fixture_job("manager", "alpha");
        spec.operation = "deploy.run".into();
        spec.spec = json!({"change_id":change.plan.change_id,"expected_revision":change.revision,"until":"verified"});
        let parent = store
            .submit_linked_job(
                "workflow",
                &id,
                &spec,
                Some(maxops_store::ChangeJobLink {
                    change: &change,
                    next: ChangeState::Prepared,
                    action: None,
                    workflow: None,
                }),
            )
            .await
            .unwrap()
            .job;
        if cancelled {
            store
                .request_cancel(&id, parent.handle.revision, "stop before start")
                .await
                .unwrap();
            // Deliberately pass the stale pre-cancel snapshot to the worker.
            deployment_workflow::spawn(app.clone(), parent);
            assert_eq!(
                terminal_job(&app, &id).await.handle.state,
                JobState::Cancelled
            );
        } else {
            let running = enter_local_job(store, parent).await.unwrap();
            let reconciling = store
                .transition_job(
                    &id,
                    running.handle.revision,
                    JobState::Reconciling,
                    &json!({}),
                    None,
                )
                .await
                .unwrap();
            store
                .transition_job(
                    &id,
                    reconciling.handle.revision,
                    JobState::OutcomeUnknown,
                    &json!({}),
                    None,
                )
                .await
                .unwrap();
            let (status, observed) = call(
                router(app.clone()),
                "/v1/execute",
                Some(USER_TOKEN),
                json!({"op":"jobs.status","params":{"job_id":id}}),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(observed["handle"]["state"], "failed");
            assert_eq!(observed["result"]["stages_resumed"], false);
        }
        assert_eq!(
            target
                .list_jobs(Some("manager"), None, &[], 20)
                .await
                .unwrap()
                .len(),
            1
        );
        server.abort();
    }
}

fn app(agent_url: &str, capabilities: &[&str]) -> App {
    App {
        hosts: BTreeMap::from([
            (
                "alpha".into(),
                Host {
                    config: HostConfig {
                        read_all_units: false,
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
                        read_all_units: false,
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
        wait_slots: Semaphore::new(64),
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
        ExecutorRequest::ExecutionProfiles => Ok(Json(ExecutorResponse::ExecutionProfiles(vec![]))),
        ExecutorRequest::Submit { job_id, job } => {
            let command_missing = job.spec.pointer("/command/argv/0").and_then(Value::as_str)
                == Some("/missing/diagnostic-command");
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
                        if command_missing {
                            JobState::Failed
                        } else {
                            JobState::Succeeded
                        },
                        &json!({}),
                        Some(&json!({"exit_code":if command_missing {127} else {0}})),
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
                    read_all_units: false,
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
        wait_slots: Semaphore::new(64),
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
async fn mixed_execution_inventory_keeps_read_only_hosts_available() {
    let (alpha_url, alpha_task) =
        stub(Router::new().route("/v1/snapshot", get(|| async { Json(snapshot("alpha")) }))).await;
    let (beta_url, beta_task) =
        stub(Router::new().route("/v1/snapshot", get(|| async { Json(snapshot("beta")) }))).await;
    let directory = tempfile::tempdir().unwrap();
    let alpha_token = directory.path().join("alpha-token");
    let beta_token = directory.path().join("beta-token");
    let beta_execution_token = directory.path().join("beta-execution-token");
    let client_token = directory.path().join("client-token");
    std::fs::write(&alpha_token, AGENT_TOKEN).unwrap();
    std::fs::write(&beta_token, "agent-token-eeeeeeeeeeeeeeeeeeeeeeeeee").unwrap();
    std::fs::write(&beta_execution_token, EXECUTION_TOKEN).unwrap();
    std::fs::write(&client_token, USER_TOKEN).unwrap();
    let config: Config = serde_json::from_value(json!({
        "listen":"127.0.0.1:0",
        "state_file":directory.path().join("hub.db"),
        "hosts":[
            {
                "name":"alpha",
                "agent_url":alpha_url,
                "agent_token_file":alpha_token,
                "readable_units":["demo.service"],
                "manageable_units":["demo.service"]
            },
            {
                "name":"beta",
                "agent_url":beta_url,
                "agent_token_file":beta_token,
                "execution_token_file":beta_execution_token,
                "readable_units":["demo.service"],
                "manageable_units":["demo.service"]
            }
        ],
        "clients":[{
            "name":"manager",
            "token_file":client_token,
            "hosts":["alpha","beta"],
            "capabilities":["host:read","units:manage","jobs:read"],
            "access":"manage"
        }]
    }))
    .unwrap();
    let (_, router) = build(config).await.unwrap();

    let (status, facts) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"host.facts","params":{"host":"alpha"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(facts["host"], "alpha");

    let (status, error) = call_with_idempotency(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        Some("mixed-version-restart"),
        json!({"op":"units.restart","params":{"host":"alpha","unit":"demo.service"}}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error["error"], "host execution disabled");
    alpha_task.abort();
    beta_task.abort();
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
    assert_eq!(result["hosts"][0]["unit_scope"]["coverage"], "allowlist");
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
async fn all_unit_observation_is_paged_and_does_not_grant_mutations() {
    let (url, server) = stub(Router::new().route(
        "/v1/snapshot",
        get(|| async {
            let mut value = snapshot("alpha");
            value["read_all_units"] = json!(true);
            Json(value)
        }),
    ))
    .await;
    let mut state = app(&url, &["units:read", "self:read"]);
    let host = state.hosts.get_mut("alpha").unwrap();
    host.config.read_all_units = true;
    assert!(unit_allowed(host, "multi-user.target").is_ok());
    assert!(unit_allowed(host, "worker@one.timer").is_ok());
    assert!(unit_allowed(host, "../escape.service").is_err());
    assert!(unit_manageable(host, "secret.service").is_err());
    let router = router(Arc::new(state));
    let (status, first) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"units.list","params":{"host":"alpha","limit":1}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["unit_scope"]["coverage"], "all_loaded");
    assert_eq!(first["total"], 2);
    assert_eq!(first["units"].as_array().unwrap().len(), 1);
    let (_, next) = call(router.clone(), "/v1/execute", Some(USER_TOKEN), json!({"op":"units.list","params":{"host":"alpha","limit":1,"cursor":first["next_cursor"]}})).await;
    assert_eq!(next["units"][0]["unit"], "secret.service");
    let (_, resources) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"resources.list","params":{"kind":"units","host":"alpha"}}),
    )
    .await;
    assert_eq!(resources["total"], 2);
    assert_eq!(resources["resources"][1]["manageable"], false);
    let (_, filtered) = call(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"units.list","params":{"host":"alpha","prefix":"secret","state":"failed"}}),
    )
    .await;
    assert_eq!(filtered["total"], 1);
    server.abort();
}

#[tokio::test]
async fn event_details_are_bounded_and_host_scoped() {
    let directory = tempfile::tempdir().unwrap();
    let mut state = management_app("http://127.0.0.1:1", &directory.path().join("hub.db")).await;
    state.clients[0].capabilities.insert("events:read".into());
    let mut ids = Vec::new();
    for host in ["alpha", "private"] {
        let event = state
            .store
            .as_ref()
            .unwrap()
            .ingest_alert(AlertEventInput {
                source: "test".into(),
                fingerprint: host.into(),
                host: host.into(),
                firing: true,
                occurred_at: now(),
                payload: json!({"evidence":"abcdefghijk"}),
            })
            .await
            .unwrap();
        ids.push(event.event_id);
    }
    let router = router(Arc::new(state));
    let (status, fragment) = call(router.clone(), "/v1/execute?view=summary", Some(USER_TOKEN), json!({"op":"events.get","params":{"event_id":ids[0],"pointer":"/payload/evidence","limit":5}})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fragment["text"].as_str().unwrap().len(), 5);
    assert_eq!(fragment["complete"], false);
    assert_eq!(fragment["next_offset"], 5);
    let (status, error) = call(
        router,
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"events.get","params":{"event_id":ids[1]}}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error["code"], "host_not_permitted");
    assert!(!error.to_string().contains("abcdefghijk"));
}

#[tokio::test]
async fn discovery_explains_host_requirement_and_summaries_bound_event_payloads() {
    let directory = tempfile::tempdir().unwrap();
    let mut state = management_app("http://127.0.0.1:1", &directory.path().join("hub.db")).await;
    state.clients[0].capabilities.insert("self:read".into());
    let (status, error) = call(
        router(Arc::new(state)),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"resources.list","params":{"kind":"execution_profiles"}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "execution_profile_host_required");
    let operation = operations()
        .into_iter()
        .find(|operation| operation.name == "resources.list")
        .unwrap();
    assert_eq!(
        operation.params_schema.to_value()["then"]["required"],
        json!(["host"])
    );
    let query = serde_json::from_value(json!({"view":"summary"})).unwrap();
    let compact = client_api::present(Query(query), &serde_json::from_value(json!({"op":"events.recent","params":{}})).unwrap(), json!({"events":[{"sequence":42,"payload":{"annotations":{"summary":"failure"},"labels":{"name":"health.service"},"large":"x".repeat(100_000)}}]})).unwrap();
    assert!(compact.to_string().len() < 1000);
    assert_eq!(compact["events"][0]["unit"], "health.service");
    assert_eq!(compact["events"][0]["summary"], "failure");
    assert!(compact["events"][0].get("payload").is_none());
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
        .diagnostic_probes = BTreeMap::from([
        ("identity".into(), vec!["/bin/true".into()]),
        ("missing".into(), vec!["/missing/diagnostic-command".into()]),
    ]);
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
            "host":"alpha","event_id":alert.event_id,"unit":"demo.service","probes":["identity","missing"]
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
    let summary = client_api::job_summary(&diagnostic);
    assert_eq!(summary["evidence_status"], "partial");
    assert_eq!(summary["missing_evidence"], json!(["probe:missing"]));
    let evidence = &diagnostic.result.as_ref().unwrap()["diagnostic"]["evidence"];
    let failed = evidence
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["evidence_id"] == "probe:missing")
        .unwrap();
    assert_eq!(failed["assessment"], "missing");
    assert_eq!(failed["value"]["result"]["exit_code"], 127);
    let (status, probes) = call(
        router.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"resources.list","params":{"kind":"diagnostic_probes","host":"alpha"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(probes["total"], 2);

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

#[tokio::test]
async fn submission_receipt_reads_survive_reopen_and_preserve_identity_scope() {
    let directory = tempfile::tempdir().unwrap();
    let target = Store::open(&directory.path().join("target.db"))
        .await
        .unwrap();
    let (url, task) = stub(
        Router::new()
            .route("/v1/manage", post(successful_executor))
            .with_state(target),
    )
    .await;
    let path = directory.path().join("hub.db");
    let state = Arc::new(management_app(&url, &path).await);
    let route = router(state.clone());
    let submission = json!({"op":"exec.run","params":{"host":"alpha","profile":"diagnostic","command":{"argv":["/bin/true"]}}});
    let (status, handle) = call_with_idempotency(
        route.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("receipt-123"),
        submission,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, observed) = call(
        route.clone(),
        "/v1/execute",
        Some(USER_TOKEN),
        json!({"op":"jobs.status","params":{"idempotency_key":"receipt-123"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(observed["handle"]["job_id"], handle["job_id"]);
    let id: JobId = serde_json::from_value(handle["job_id"].clone()).unwrap();
    terminal_job(&state, &id).await;
    drop(route);
    drop(state);
    let mut reopened = management_app(&url, &path).await;
    let store = reopened.store.as_ref().unwrap().clone();
    store
        .submit_job("private-key", &fixture_job("other", "alpha"))
        .await
        .unwrap();
    store
        .submit_job("revoked-host", &fixture_job("manager", "private"))
        .await
        .unwrap();
    reopened.clients.push(Principal {
        name: "other".into(),
        token: Token::parse(ALERT_TOKEN.into()).unwrap(),
        hosts: BTreeSet::from(["alpha".into()]),
        capabilities: BTreeSet::from(["jobs:read".into()]),
        access: Access::Manage,
        repositories: BTreeSet::new(),
        deployments: BTreeSet::new(),
    });
    let route = router(Arc::new(reopened));
    for operation in ["jobs.status", "jobs.wait", "jobs.logs", "jobs.result"] {
        let extra = if operation == "jobs.wait" {
            json!({"timeout_seconds":0})
        } else {
            json!({})
        };
        let mut keyed = extra.clone();
        keyed["idempotency_key"] = json!("receipt-123");
        let (status, by_key) = call(
            route.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":operation,"params":keyed}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{operation}: {by_key}");
        let mut by_id = extra.clone();
        by_id["job_id"] = handle["job_id"].clone();
        let (_, by_id) = call(
            route.clone(),
            "/v1/execute",
            Some(USER_TOKEN),
            json!({"op":operation,"params":by_id}),
        )
        .await;
        assert_eq!(by_key, by_id, "{operation}");
        for (token, key, expected) in [
            (USER_TOKEN, "private-key", StatusCode::NOT_FOUND),
            (ALERT_TOKEN, "receipt-123", StatusCode::NOT_FOUND),
            (USER_TOKEN, "missing", StatusCode::NOT_FOUND),
            (USER_TOKEN, "revoked-host", StatusCode::FORBIDDEN),
        ] {
            let mut params = extra.clone();
            params["idempotency_key"] = json!(key);
            let (status, error) = call(
                route.clone(),
                "/v1/execute",
                Some(token),
                json!({"op":operation,"params":params}),
            )
            .await;
            assert_eq!(status, expected, "{operation}: {error}");
        }
        for mut params in [
            json!({}),
            json!({"idempotency_key":""}),
            json!({"job_id":id,"idempotency_key":"receipt-123"}),
        ] {
            params
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let (status, _) = call(
                route.clone(),
                "/v1/execute",
                Some(USER_TOKEN),
                json!({"op":operation,"params":params}),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
    }
    task.abort();
}

#[tokio::test]
async fn wrong_mutation_kind_has_specific_error_and_creates_no_durable_job() {
    let directory = tempfile::tempdir().unwrap();
    let state =
        Arc::new(management_app("http://127.0.0.1:1", &directory.path().join("hub.db")).await);
    let (status, error) = call_with_idempotency(
        router(state.clone()),
        "/v1/execute",
        Some(USER_TOKEN),
        Some("reject-target"),
        json!({"op":"units.stop","params":{"host":"alpha","unit":"multi-user.target"}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "unit_kind_not_manageable");
    assert_eq!(error["retry"], "never");
    assert!(
        state
            .store
            .as_ref()
            .unwrap()
            .get_idempotent_job("manager", "reject-target")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn nine_host_overview_and_metric_views_are_bounded_over_http() {
    let (url, task) = stub(Router::new()
        .route("/{host}/v1/snapshot", get(|axum::extract::Path(host): axum::extract::Path<String>| async move {Json(snapshot(&host))}))
        .route("/api/v1/query", get(|axum::extract::Query(params): axum::extract::Query<BTreeMap<String, String>>| async move {
            let query = &params["query"];
            let host = query.split("instance=\"").nth(1).and_then(|s| s.split('"').next());
            let mut rows = Vec::new();
            if let Some(host) = host {
                for (name, count, value) in [("load1",1,2.0),("load5",1,1.5),("load15",1,1.0),("memory_available_bytes",1,25.0),("memory_total_bytes",1,100.0),("filesystem_available_bytes",9,20.0),("filesystem_size_bytes",9,100.0),("cpu_idle_seconds_per_second",9,0.5),("network_receive_bytes_per_second",19,50.0),("network_transmit_bytes_per_second",19,100.0)] {
                    for index in 0..count {
                        let mut labels = json!({"job":"node","instance":host,"maxops_metric":name});
                        if name.starts_with("filesystem") {labels["mountpoint"]=json!(format!("/data/{index}")); labels["device"]=json!(format!("/dev/device{index}"));}
                        if name.starts_with("cpu") {labels["cpu"]=json!(index.to_string());}
                        if name.starts_with("network") {labels["device"]=json!(format!("eth{index}"));}
                        rows.push(json!({"metric":labels,"value":[0,if query.contains("timestamp(") {now().as_second().to_string()} else {value.to_string()}]}));
                    }
                }
            }
            Json(json!({"status":"success","data":{"resultType":"vector","result":rows}}))
        }))).await;
    let mut state = app("http://127.0.0.1:1", &["fleet:read", "metrics:read"]);
    state.hosts.clear();
    state.clients[0].hosts.clear();
    for index in 0..9 {
        let name = format!("node-{index:02}");
        let mut config = app("http://127.0.0.1:1", &[])
            .hosts
            .remove("alpha")
            .unwrap()
            .config;
        config.name = name.clone();
        config.agent_url = format!("{url}/{name}");
        state.hosts.insert(
            name.clone(),
            Host {
                config,
                token: Token::parse(AGENT_TOKEN.into()).unwrap(),
                execution_token: None,
            },
        );
        state.clients[0].hosts.insert(name);
    }
    state.prometheus_url = Some(url);
    let route = router(Arc::new(state));
    let (status, summary) = call(
        route.clone(),
        "/v1/execute?view=summary",
        Some(USER_TOKEN),
        json!({"op":"fleet.overview","params":{}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let bytes = serde_json::to_vec(&summary).unwrap().len();
    eprintln!("nine-host overview summary: {bytes} bytes");
    assert!(bytes < 6000, "nine-host overview summary: {bytes} bytes");
    assert_eq!(summary["hosts"].as_array().unwrap().len(), 9);
    for host in summary["hosts"].as_array().unwrap() {
        assert_eq!(host["agent_state"], "reachable");
        assert_eq!(host["pressure"]["load1"], 2.0);
        assert_eq!(host["pressure"]["memory_used_fraction"], 0.75);
        assert_eq!(host["pressure"]["filesystem_state"], "available");
    }
    assert!(!summary.to_string().contains("samples"));
    let (_, full) = call(
        route.clone(),
        "/v1/execute?view=full",
        Some(USER_TOKEN),
        json!({"op":"fleet.overview","params":{}}),
    )
    .await;
    assert!(full["hosts"][0]["pressure"]["filesystem_available_bytes"]["samples"].is_array());
    for (path, params, aggregated) in [
        ("/v1/execute?view=full", json!({"host":"node-00"}), false),
        ("/v1/execute?view=summary", json!({"host":"node-00"}), true),
        (
            "/v1/execute?view=full",
            json!({"host":"node-00","aggregation":"stats"}),
            true,
        ),
    ] {
        let (status, result) = call(
            route.clone(),
            path,
            Some(USER_TOKEN),
            json!({"op":"host.metrics","params":params}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let cpu = &result["observation"]["metrics"]["cpu_idle_seconds_per_second"];
        if aggregated {
            assert_eq!(cpu["series_count"], 9);
            assert!((cpu["mean"].as_f64().unwrap() - 0.5).abs() < 1e-12);
            assert!(cpu.get("samples").is_none());
        } else {
            assert_eq!(cpu["samples"].as_array().unwrap().len(), 9);
        }
    }
    task.abort();
}

#[tokio::test]
async fn alert_views_filter_before_paging_and_keep_full_objects() {
    let capture: Value =
        serde_json::from_str(include_str!("../tests/fixtures/active-alerts-20.json")).unwrap();
    let source = capture.clone();
    let (url, task) = stub(Router::new().route(
        "/api/v2/alerts",
        get(move || {
            let data = source.clone();
            async move { Json(data) }
        }),
    ))
    .await;
    let mut state = app("http://127.0.0.1:1", &["alerts:read"]);
    state.alertmanager_url = Some(url);
    state.clients[0].hosts = capture
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["labels"]["instance"].as_str().unwrap().to_owned())
        .collect();
    for name in state.clients[0].hosts.clone() {
        let mut config = app("http://127.0.0.1:1", &[])
            .hosts
            .remove("alpha")
            .unwrap()
            .config;
        config.name = name.clone();
        state.hosts.insert(
            name,
            Host {
                config,
                token: Token::parse(AGENT_TOKEN.into()).unwrap(),
                execution_token: None,
            },
        );
    }
    let route = router(Arc::new(state));
    let (_, full) = call(
        route.clone(),
        "/v1/execute?view=full",
        Some(USER_TOKEN),
        json!({"op":"alerts.active","params":{}}),
    )
    .await;
    assert_eq!(
        serde_json::to_vec(&full["alerts"]).unwrap(),
        serde_json::to_vec(&capture).unwrap()
    );
    let (status, summary) = call(
        route.clone(),
        "/v1/execute?view=summary",
        Some(USER_TOKEN),
        json!({"op":"alerts.active","params":{}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(serde_json::to_vec(&summary).unwrap().len() <= 4000);
    let selected = capture[0]["labels"]["instance"].as_str().unwrap();
    let expected = capture
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["labels"]["instance"] == selected)
        .count();
    for view in ["summary", "full"] {
        let (status, filtered) = call(
            route.clone(),
            &format!("/v1/execute?view={view}"),
            Some(USER_TOKEN),
            json!({"op":"alerts.active","params":{"host":selected,"limit":1}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(filtered["total"], expected);
        assert_eq!(filtered["alerts"].as_array().unwrap().len(), 1);
        let (status, _) = call(
            route.clone(),
            &format!("/v1/execute?view={view}"),
            Some(USER_TOKEN),
            json!({"op":"alerts.active","params":{"host":"private"}}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    task.abort();
}

#[tokio::test]
async fn tools_mutation_unit_enums_enforce_current_scope_before_consumer_admission() {
    let directory = tempfile::tempdir().unwrap();
    let mut state = management_app("http://127.0.0.1:1", &directory.path().join("hub.db")).await;
    let mut private = app("http://127.0.0.1:1", &[])
        .hosts
        .remove("private")
        .unwrap();
    private
        .config
        .manageable_units
        .insert("private.service".into());
    state.hosts.insert("private".into(), private);
    let state = Arc::new(state);
    let route = router(state.clone());
    let (_, tools) = get_json(route.clone(), "/v1/operations?view=tools", Some(USER_TOKEN)).await;
    let (_, full) = get_json(route, "/v1/operations?view=full", Some(USER_TOKEN)).await;
    for operation in tools["operations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|op| op["capability"] == "units:manage")
    {
        let unit = &operation["params_schema"]["properties"]["unit"];
        assert_eq!(unit["enum"], json!(["demo.service"]));
        // Max.Tool.Catalog.validatePresent rejects strings outside enum before
        // invoking its task admission handler; it does not yet check pattern.
        assert!(
            !unit["enum"]
                .as_array()
                .unwrap()
                .contains(&json!("multi-user.target"))
        );
        assert!(unit["pattern"].is_string());
        let original = full["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|op| op["name"] == operation["name"])
            .unwrap();
        assert!(
            original["params_schema"]["properties"]["unit"]
                .get("enum")
                .is_none()
        );
    }
    let mut state = Arc::try_unwrap(state).ok().unwrap();
    state
        .hosts
        .get_mut("alpha")
        .unwrap()
        .config
        .manageable_units
        .clear();
    let (_, empty) = get_json(
        router(Arc::new(state)),
        "/v1/operations?view=tools",
        Some(USER_TOKEN),
    )
    .await;
    assert!(
        empty["operations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|op| op["capability"] != "units:manage")
    );
}
