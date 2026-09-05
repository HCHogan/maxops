use super::*;

const METRICS: &[(&str, &str, bool)] = &[
    ("load1", "node_load1", false),
    ("load5", "node_load5", false),
    ("load15", "node_load15", false),
    (
        "memory_available_bytes",
        "node_memory_MemAvailable_bytes",
        false,
    ),
    ("memory_total_bytes", "node_memory_MemTotal_bytes", false),
    (
        "filesystem_available_bytes",
        "node_filesystem_avail_bytes",
        false,
    ),
    ("filesystem_size_bytes", "node_filesystem_size_bytes", false),
    (
        "cpu_idle_seconds_per_second",
        "node_cpu_seconds_total",
        true,
    ),
    (
        "network_receive_bytes_per_second",
        "node_network_receive_bytes_total",
        true,
    ),
    (
        "network_transmit_bytes_per_second",
        "node_network_transmit_bytes_total",
        true,
    ),
];

fn expressions(host: &str) -> (String, String) {
    let mut values = Vec::new();
    let mut times = Vec::new();
    for (name, metric, rate) in METRICS {
        let extra = if *metric == "node_cpu_seconds_total" {
            ",mode=\"idle\""
        } else {
            ""
        };
        let selector = format!("{metric}{{job=\"node\",instance=\"{host}\"{extra}}}");
        let expression = if *rate {
            format!("rate({selector}[5m])")
        } else {
            selector.clone()
        };
        values.push(format!(
            "label_replace({expression},\"maxops_metric\",\"{name}\",\"\",\"\")"
        ));
        times.push(format!(
            "label_replace(timestamp({selector}),\"maxops_metric\",\"{name}\",\"\",\"\")"
        ));
    }
    (values.join(" or "), times.join(" or "))
}

fn number(sample: &Value) -> Option<f64> {
    sample["value"][1]
        .as_str()?
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn labels(sample: &Value) -> Option<serde_json::Map<String, Value>> {
    let mut labels = sample["metric"].as_object()?.clone();
    labels.remove("__name__");
    Some(labels)
}

fn series_key(sample: &Value, public_only: bool) -> Option<String> {
    let mut labels = labels(sample)?;
    if public_only {
        labels.retain(|key, _| {
            [
                "instance",
                "job",
                "maxops_metric",
                "cpu",
                "device",
                "mountpoint",
                "fstype",
                "mode",
            ]
            .contains(&key.as_str())
        });
    }
    serde_json::to_string(&labels).ok()
}

fn observations(host: &str, values: Vec<Value>, times: Vec<Value>, current: i64) -> Value {
    let mut time_index: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for sample in &times {
        if let Some(key) = series_key(sample, false) {
            time_index.entry(key).or_default().push(sample);
        }
    }
    let mut counts = BTreeMap::new();
    for sample in &values {
        if let Some(key) = series_key(sample, true) {
            *counts.entry(key).or_insert(0) += 1;
        }
    }
    let mut result = serde_json::Map::new();
    for (name, _, _) in METRICS {
        let mut entries = Vec::new();
        for sample in &values {
            if sample["metric"]["instance"] != host
                || sample["metric"]["job"] != "node"
                || sample["metric"]["maxops_metric"] != *name
            {
                continue;
            }
            let Some(sample_labels) = labels(sample) else {
                continue;
            };
            let matching_times = series_key(sample, false)
                .and_then(|key| time_index.get(&key))
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let timestamp = match matching_times {
                [entry] => number(entry),
                _ => None,
            };
            let value = number(sample);
            let duplicates = series_key(sample, true)
                .and_then(|key| counts.get(&key))
                .copied()
                .unwrap_or(0);
            let state = if duplicates != 1 || matching_times.len() > 1 {
                "ambiguous"
            } else if timestamp.is_none() || value.is_none() {
                "unknown"
            } else if timestamp
                .is_some_and(|time| time > current as f64 + 30.0 || current as f64 - time > 90.0)
            {
                "stale"
            } else {
                "available"
            };
            let selected: serde_json::Map<_, _> = sample_labels
                .into_iter()
                .filter(|(key, _)| {
                    ["cpu", "device", "mountpoint", "fstype", "mode"].contains(&key.as_str())
                })
                .collect();
            entries.push(json!({"labels": selected, "state": state, "value": if state == "available" { value } else { None }, "sample_at_unix_seconds": timestamp}));
        }
        result.insert((*name).into(), json!({"state": if entries.is_empty() { "unknown" } else if entries.iter().all(|entry| entry["state"] == "available") { "available" } else { "partial" }, "samples": entries}));
    }
    Value::Object(result)
}

pub(super) async fn host_metrics(app: &App, host: &str) -> Value {
    let Some(url) = &app.prometheus_url else {
        return json!({"state": "not_configured", "metrics": null});
    };
    let (query, timestamps) = expressions(host);
    let (values, times) = tokio::join!(
        prometheus_vector(app, url, &query),
        prometheus_vector(app, url, &timestamps)
    );
    match (values, times) {
        (Ok(values), Ok(times)) if values.len() <= 4096 && times.len() <= 4096 => {
            json!({"state": "available", "metrics": observations(host, values, times, now().as_second())})
        }
        _ => json!({"state": "unavailable", "metrics": null}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(host: &str, name: &str, value: &str) -> Value {
        json!({"metric": {"job": "node", "instance": host, "maxops_metric": name}, "value": [1000, value]})
    }

    #[test]
    fn metrics_keep_scope_and_source_freshness() {
        let values = vec![
            sample("alpha", "load1", "1.5"),
            sample("private", "load1", "99"),
            sample("alpha", "load5", "NaN"),
        ];
        let times = vec![
            sample("alpha", "load1", "980"),
            sample("alpha", "load5", "980"),
        ];
        let result = observations("alpha", values.clone(), times.clone(), 1000);
        assert_eq!(result["load1"]["samples"].as_array().unwrap().len(), 1);
        assert_eq!(result["load1"]["samples"][0]["value"], 1.5);
        assert_eq!(result["load5"]["samples"][0]["state"], "unknown");
        assert_eq!(result["load15"]["state"], "unknown");
        assert_eq!(
            observations("alpha", values, times, 2000)["load1"]["samples"][0]["state"],
            "stale"
        );
    }

    #[test]
    fn ambiguous_and_future_samples_are_not_healthy() {
        let value = sample("alpha", "load1", "0");
        let time = sample("alpha", "load1", "1100");
        assert_eq!(
            observations("alpha", vec![value.clone()], vec![time.clone()], 1000)["load1"]["samples"]
                [0]["state"],
            "stale"
        );
        assert_eq!(
            observations("alpha", vec![value.clone(), value], vec![time], 1000)["load1"]["samples"]
                [0]["state"],
            "ambiguous"
        );
    }

    #[test]
    fn every_selector_is_host_scoped() {
        let (values, times) = expressions("alpha");
        for expression in [values, times] {
            assert_eq!(
                expression.matches("instance=\"alpha\"").count(),
                METRICS.len()
            );
            assert_eq!(expression.matches("job=\"node\"").count(), METRICS.len());
        }
    }

    #[test]
    fn hidden_duplicate_labels_are_ambiguous_not_two_healthy_samples() {
        let mut first = sample("alpha", "load1", "0");
        first["metric"]["replica"] = json!("private-a");
        let mut second = first.clone();
        second["metric"]["replica"] = json!("private-b");
        let mut first_time = first.clone();
        first_time["value"][1] = json!("1000");
        let mut second_time = second.clone();
        second_time["value"][1] = json!("1000");
        let result = observations(
            "alpha",
            vec![first, second],
            vec![first_time, second_time],
            1000,
        );
        assert_eq!(result["load1"]["samples"][0]["state"], "ambiguous");
        assert!(!result.to_string().contains("private-"));
    }
}
