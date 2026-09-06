use criterion::{Criterion, criterion_group, criterion_main};
use maxops_proto::{EpisodeId, EventId, EventKind, EventRecord, Request, now, transport::Token};
use serde_json::json;
use std::hint::black_box;

fn protocol(c: &mut Criterion) {
    let query = r#"{"op":"units.logs","params":{"host":"router","unit":"nginx.service","lines":100,"since_seconds":3600}}"#;
    c.bench_function("decode_and_validate_log_request", |b| {
        b.iter(|| {
            let request: Request = serde_json::from_str(black_box(query)).unwrap();
            if let Request::UnitsLogs(params) = request {
                black_box(params.validate()).unwrap();
            }
        })
    });
    let token = Token::parse("benchmark-only-token-aaaaaaaaaaaaaaaa".into()).unwrap();
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "authorization",
        "Bearer benchmark-only-token-aaaaaaaaaaaaaaaa"
            .parse()
            .unwrap(),
    );
    c.bench_function("bearer_auth", |b| {
        b.iter(|| black_box(token.matches(black_box(&headers))))
    });
    let event = EventRecord {
        sequence: 42,
        event_id: EventId::parse("00000000-0000-0000-0000-000000000001").unwrap(),
        source: "alertmanager".into(),
        fingerprint: "fixture".into(),
        episode_id: EpisodeId::parse("00000000-0000-0000-0000-000000000002").unwrap(),
        kind: EventKind::AlertFiring,
        host: "host-a".into(),
        occurred_at: now(),
        received_at: now(),
        related_job_id: None,
        related_change_id: None,
        payload: json!({"labels":{"alertname":"FixtureDown","instance":"host-a"}}),
    };
    c.bench_function("serialize_fleet_event", |b| {
        b.iter(|| black_box(serde_json::to_vec(black_box(&event))).unwrap())
    });
}
criterion_group!(benches, protocol);
criterion_main!(benches);
