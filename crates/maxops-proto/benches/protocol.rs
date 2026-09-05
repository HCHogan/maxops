use criterion::{Criterion, criterion_group, criterion_main};
use maxops_proto::{Request, transport::Token};
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
}
criterion_group!(benches, protocol);
criterion_main!(benches);
