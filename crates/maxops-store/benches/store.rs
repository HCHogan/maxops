use criterion::{Criterion, criterion_group, criterion_main};
use maxops_proto::NewJob;
use maxops_store::job_spec_hash;
use serde_json::json;
use std::hint::black_box;

fn store(c: &mut Criterion) {
    let job = NewJob {
        principal: "benchmark-client".into(),
        host: "host-a".into(),
        operation: "exec.run".into(),
        spec_version: 1,
        spec: json!({
            "profile": "diagnostic",
            "command": {"argv": ["systemctl", "show", "example.service"]},
            "env": {"B": "2", "A": "1"}
        }),
        policy_version: "benchmark-policy".into(),
        deadline: None,
    };
    c.bench_function("canonical_job_spec_hash", |bench| {
        bench.iter(|| black_box(job_spec_hash(black_box(&job))).unwrap())
    });
}

criterion_group!(benches, store);
criterion_main!(benches);
