use super::*;
use serde_json::{Value, json};

fn check_properties(schema: &Value, path: &str) {
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, property) in properties {
            assert!(
                property["description"]
                    .as_str()
                    .is_some_and(|s| !s.trim().is_empty()),
                "missing parameter description: {path}.{name}"
            );
        }
    }
    match schema {
        Value::Object(fields) => {
            for (name, child) in fields {
                check_properties(child, &format!("{path}/{name}"));
            }
        }
        Value::Array(items) => {
            for child in items {
                check_properties(child, path);
            }
        }
        _ => {}
    }
}

#[test]
fn every_parameter_including_nested_properties_has_semantics() {
    for operation in operations() {
        let schema = serde_json::to_value(operation.params_schema).unwrap();
        check_properties(&schema, operation.name);
        jsonschema::validator_for(&schema).unwrap_or_else(|e| panic!("{}: {e}", operation.name));
    }
}

#[test]
fn client_schema_rejects_non_service_mutations_before_admission() {
    for operation in operations()
        .into_iter()
        .filter(|op| op.capability == "units:manage")
    {
        let schema = serde_json::to_value(operation.params_schema).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let mut names = vec![
            "multi-user.target".into(),
            "demo.timer".into(),
            "worker@one.service".into(),
            "a.service".into(),
            ".service".into(),
            "../a.service".into(),
            "a.service\n".into(),
            "a/b.service".into(),
            "a*.service".into(),
            "é.service".into(),
            r"a\x20b.service".into(),
        ];
        names.extend((0..=260).map(|n| format!("{}.service", "a".repeat(n))));
        for unit in names {
            assert_eq!(
                validator.is_valid(&json!({"host":"alpha", "unit":unit})),
                valid_unit(&unit),
                "{}: {unit:?}",
                operation.name
            );
        }
    }
}

#[test]
fn job_read_schemas_accept_exactly_one_stable_identifier() {
    for operation in operations()
        .into_iter()
        .filter(|op| ["jobs.status", "jobs.wait", "jobs.logs", "jobs.result"].contains(&op.name))
    {
        let schema = serde_json::to_value(operation.params_schema).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let id = "00000000-0000-0000-0000-000000000000";
        for input in [
            json!({"job_id":id}),
            json!({"idempotency_key":"submission-123"}),
        ] {
            assert!(validator.is_valid(&input), "{}: {input}", operation.name);
            assert!(
                serde_json::from_value::<Request>(json!({"op":operation.name,"params":input}))
                    .is_ok()
            );
        }
        for input in [
            json!({}),
            json!({"job_id":id,"idempotency_key":"submission-123"}),
            json!({"job_id":"133"}),
            json!({"idempotency_key":""}),
            json!({"idempotency_key":"with space"}),
            json!({"idempotency_key":"x".repeat(129)}),
            json!({"idempotency_key":null}),
            json!({"job_id":null}),
        ] {
            assert!(!validator.is_valid(&input), "{}: {input}", operation.name);
        }
    }
}
