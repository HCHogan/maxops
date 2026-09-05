use super::*;
use axum::{body::Body, http::Request as HttpRequest};
use http_body_util::BodyExt;
use tower::ServiceExt;

const USER_TOKEN: &str = "user-token-aaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const AGENT_TOKEN: &str = "agent-token-bbbbbbbbbbbbbbbbbbbbbbbbbb";
const ALERT_TOKEN: &str = "alert-token-cccccccccccccccccccccccccc";

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
                        readable_units: BTreeSet::from(["demo.service".into()]),
                    },
                    token: Token::parse(AGENT_TOKEN.into()).unwrap(),
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
                        readable_units: BTreeSet::new(),
                    },
                    token: Token::parse(AGENT_TOKEN.into()).unwrap(),
                },
            ),
        ]),
        clients: vec![Principal {
            name: "reader".into(),
            token: Token::parse(USER_TOKEN.into()).unwrap(),
            hosts: BTreeSet::from(["alpha".into()]),
            capabilities: capabilities.iter().map(|s| s.to_string()).collect(),
        }],
        client: transport::client().unwrap(),
        prometheus_url: None,
        alertmanager_url: None,
        alert_ingress: None,
        slots: Semaphore::new(16),
    }
}

async fn call(router: Router, path: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut builder = HttpRequest::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
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
