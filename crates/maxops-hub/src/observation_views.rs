//! Bounded observation projections. Full observations remain available for audit.
use super::*;

fn bounded(value: &Value, max_chars: usize) -> Value {
    value.as_str().map_or(Value::Null, |s| {
        json!(s.chars().take(max_chars).collect::<String>())
    })
}

pub(super) fn alerts(
    mut value: Value,
    params: &maxops_proto::AlertsParams,
    summary: bool,
) -> Result<Value, ApiError> {
    if !summary && params.limit.is_none() && params.cursor.is_none() {
        return Ok(value);
    }
    let mut alerts = value["alerts"].as_array().cloned().unwrap_or_default();
    if summary {
        alerts = alerts.into_iter().map(|alert| {
            let labels = &alert["labels"];
            let summary = alert["annotations"]["summary"].as_str()
                .or_else(|| alert["annotations"]["description"].as_str()).unwrap_or("");
            let mut projected = json!({
                "alertname": bounded(&labels["alertname"], 128),
                "host": labels["instance"],
                "unit": bounded(labels.get("unit").or_else(|| labels.get("name")).unwrap_or(&Value::Null), 255),
                "severity": bounded(&labels["severity"], 32),
                "startsAt": bounded(&alert["startsAt"], 64),
                "fingerprint": bounded(&alert["fingerprint"], 128),
                "summary": summary.chars().take(200).collect::<String>()
            });
            // Preserve peer dimensions while omitting exporter and routing labels.
            let peers: serde_json::Map<String, Value> = labels.as_object().into_iter().flat_map(|labels| labels.iter())
                .filter(|(name, _)| name.starts_with("peer"))
                .take(8).map(|(name, value)| (name.clone(), bounded(value, 128))).collect();
            if peers.len() == 1 && peers.contains_key("peer") { projected["peer"] = peers["peer"].clone(); }
            else if !peers.is_empty() { projected["peer"] = json!(peers); }
            projected
        }).collect();
    }
    // Ordering must not depend on Alertmanager's response order. The page revision
    // binds the filtered projection, excluding volatile omitted metadata.
    alerts.sort_by_cached_key(|alert| serde_json::to_string(alert).expect("alert JSON"));
    let mut result = client_api::page(
        alerts,
        params.limit.unwrap_or(if summary { 20 } else { 50 }),
        params.cursor.as_deref(),
        "alerts",
    )?;
    if summary {
        let page = result["alerts"].as_array().expect("page").clone();
        let mut groups: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for alert in page {
            groups
                .entry(alert["alertname"].as_str().unwrap_or("unknown").to_owned())
                .or_default()
                .push(alert);
        }
        result["alerts"] = json!(
            groups
                .into_iter()
                .map(|(name, mut peers)| {
                    let mut group = json!({"alertname":name, "count":peers.len()});
                    let templates: Option<Vec<String>> =
                        peers.iter().map(summary_template).collect();
                    if let Some(templates) = templates
                        && templates[0].chars().count() <= 200
                        && templates.iter().all(|template| template == &templates[0])
                        && peers
                            .iter()
                            .any(|peer| peer["summary"] != peers[0]["summary"])
                    {
                        group["summary_template"] = json!(templates[0]);
                        for peer in &mut peers {
                            peer.as_object_mut().unwrap().remove("summary");
                        }
                    }
                    // Hoist only identical fields. Each peer inherits these fields; any
                    // differing host, unit, severity, start time or summary stays per peer.
                    for field in ["host", "unit", "severity", "startsAt", "summary"] {
                        if peers[0].get(field).is_some()
                            && peers.iter().all(|peer| peer[field] == peers[0][field])
                        {
                            group[field] = peers[0][field].clone();
                            for peer in &mut peers {
                                peer.as_object_mut().unwrap().remove(field);
                            }
                        }
                    }
                    for peer in &mut peers {
                        peer.as_object_mut().unwrap().remove("alertname");
                    }
                    group["peers"] = json!(peers);
                    group
                })
                .collect::<Vec<_>>()
        );
    }
    result["observed_at"] = value["observed_at"].take();
    Ok(result)
}

// A shared template is used only when substitution is lossless for every peer.
// Scan the original string once, so host names cannot alter inserted placeholders.
fn summary_template(alert: &Value) -> Option<String> {
    let text = alert["summary"].as_str()?;
    if text.contains("{host}") || text.contains("{peer}") {
        return None;
    }
    let mut replacements: Vec<_> = [
        (alert["host"].as_str()?, "{host}"),
        (alert["peer"].as_str()?, "{peer}"),
    ]
    .into_iter()
    .filter(|(value, _)| !value.is_empty())
    .collect();
    replacements.sort_by_key(|(value, _)| std::cmp::Reverse(value.len()));
    let mut remaining = text;
    let mut template = String::new();
    while !remaining.is_empty() {
        if let Some((value, token)) = replacements
            .iter()
            .find(|(value, _)| remaining.starts_with(value))
        {
            template.push_str(token);
            remaining = &remaining[value.len()..];
        } else {
            let character = remaining.chars().next()?;
            template.push(character);
            remaining = &remaining[character.len_utf8()..];
        }
    }
    Some(template)
}

pub(super) fn aggregate(observation: &mut Value) {
    let Some(metrics) = observation["metrics"].as_object_mut() else {
        return;
    };
    for metric in metrics.values_mut() {
        let Some(samples) = metric.get("samples").and_then(Value::as_array) else {
            continue;
        };
        let values: Vec<f64> = samples
            .iter()
            .filter(|s| s["state"] == "available")
            .filter_map(|s| s["value"].as_f64())
            .filter(|v| v.is_finite())
            .collect();
        let mut states = BTreeMap::new();
        for sample in samples {
            *states
                .entry(sample["state"].as_str().unwrap_or("unknown"))
                .or_insert(0_u64) += 1;
        }
        let count = values.len();
        let mean = (count > 0).then(|| values.iter().map(|v| v / count as f64).sum::<f64>());
        *metric = json!({"state":metric["state"], "series_count":samples.len(), "available_count":count,
            "states":states, "min":values.iter().copied().reduce(f64::min),
            "max":values.iter().copied().reduce(f64::max), "mean":mean});
    }
}

fn samples<'a>(metrics: &'a Value, name: &str) -> &'a [Value] {
    metrics[name]["samples"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn scalar(metrics: &Value, name: &str) -> Option<f64> {
    match samples(metrics, name) {
        [sample] if sample["state"] == "available" => sample["value"]
            .as_f64()
            .filter(|v| v.is_finite() && *v >= 0.0),
        _ => None,
    }
}

pub(super) fn pressure(observation: &Value) -> Value {
    let metrics = &observation["metrics"];
    let load = scalar(metrics, "load1");
    let memory = scalar(metrics, "memory_available_bytes")
        .zip(scalar(metrics, "memory_total_bytes"))
        .filter(|(available, total)| *total > 0.0 && available <= total)
        .map(|(available, total)| 1.0 - available / total);
    let available = samples(metrics, "filesystem_available_bytes");
    let sizes = samples(metrics, "filesystem_size_bytes");
    let mut valid = 0;
    let mut worst: Option<(f64, &Value)> = None;
    for sample in available {
        let matches: Vec<_> = sizes
            .iter()
            .filter(|size| size["labels"] == sample["labels"])
            .collect();
        let [size] = matches.as_slice() else {
            continue;
        };
        if sample["state"] != "available" || size["state"] != "available" {
            continue;
        }
        let Some((free, total)) = sample["value"].as_f64().zip(size["value"].as_f64()) else {
            continue;
        };
        if !free.is_finite() || !total.is_finite() || total <= 0.0 || free < 0.0 || free > total {
            continue;
        }
        valid += 1;
        let fraction = free / total;
        if worst.is_none_or(|(previous, _)| fraction < previous) {
            worst = Some((fraction, sample));
        }
    }
    let filesystem_complete = valid > 0 && valid == available.len() && valid == sizes.len();
    let complete = load.is_some() && memory.is_some() && filesystem_complete;
    json!({"state":if observation["state"] != "available" {observation["state"].clone()} else {json!(if complete {"available"} else {"partial"})},
        "load1":load, "memory_used_fraction":memory,
        "filesystem_state":if filesystem_complete {"available"} else if valid > 0 {"partial"} else {"unknown"},
        "worst_filesystem":worst.map(|(fraction, sample)| json!({"mountpoint":bounded(&sample["labels"]["mountpoint"],128), "device":bounded(&sample["labels"]["device"],128), "available_fraction":fraction}))})
}

pub(super) fn fleet(value: &mut Value) {
    if let Some(hosts) = value["hosts"].as_array_mut() {
        for host in hosts {
            *host = json!({"host":host["host"], "site":bounded(&host["site"],64), "assessment":host["assessment"],
                "agent_state":host["agent"]["state"], "exporter_state":host["exporter"]["state"],
                "failed_units":host["agent"]["failed_units"], "unit_scope":host["agent"]["unit_scope"],
                "pressure":host["pressure"]});
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture() -> Value {
        json!({"observed_at":"2026-09-12T09:00:00Z", "alerts":serde_json::from_str::<Value>(include_str!("../tests/fixtures/active-alerts-20.json")).unwrap()})
    }

    fn params(limit: Option<u16>, cursor: Option<String>) -> maxops_proto::AlertsParams {
        maxops_proto::AlertsParams {
            host: None,
            limit,
            cursor,
        }
    }

    #[test]
    fn twenty_alert_capture_is_compact_lossless_and_full_is_byte_identical() {
        let original = capture();
        let full = alerts(original.clone(), &params(None, None), false).unwrap();
        assert_eq!(
            serde_json::to_vec(&full).unwrap(),
            serde_json::to_vec(&original).unwrap()
        );
        let compact = alerts(original.clone(), &params(None, None), true).unwrap();
        let bytes = serde_json::to_vec(&compact).unwrap().len();
        eprintln!("20-alert summary: {bytes} bytes");
        assert!(bytes <= 4000, "20-alert summary: {bytes} bytes");
        assert_eq!(compact["total"], 20);
        assert_eq!(compact["alerts"].as_array().unwrap().len(), 2);
        assert!(compact["next_cursor"].is_null());
        for group in compact["alerts"].as_array().unwrap() {
            assert_eq!(
                group["count"].as_u64().unwrap() as usize,
                group["peers"].as_array().unwrap().len()
            );
            for peer in group["peers"].as_array().unwrap() {
                let source = original["alerts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|alert| alert["fingerprint"] == peer["fingerprint"])
                    .unwrap();
                let field = |name: &str| peer.get(name).unwrap_or(&group[name]).clone();
                assert_eq!(field("host"), source["labels"]["instance"]);
                assert_eq!(field("severity"), source["labels"]["severity"]);
                assert_eq!(field("startsAt"), source["startsAt"]);
                assert_eq!(
                    field("unit"),
                    source["labels"]
                        .get("unit")
                        .or_else(|| source["labels"].get("name"))
                        .cloned()
                        .unwrap_or(Value::Null)
                );
                let summary = group
                    .get("summary_template")
                    .and_then(Value::as_str)
                    .map(|template| {
                        template
                            .replace("{host}", field("host").as_str().unwrap())
                            .replace("{peer}", field("peer").as_str().unwrap())
                    })
                    .unwrap_or_else(|| field("summary").as_str().unwrap().to_owned());
                assert_eq!(summary, source["annotations"]["summary"].as_str().unwrap());
            }
        }
        let serialized = compact.to_string();
        for excluded in [
            "generatorURL",
            "receivers",
            "updatedAt",
            "status",
            "description",
        ] {
            assert!(!serialized.contains(excluded));
        }
    }

    #[test]
    fn alert_pages_bound_peers_and_keep_revision_and_unicode_contracts() {
        let mut original = capture();
        original["alerts"][0]["annotations"]["summary"] = json!("诊断".repeat(300));
        let first = alerts(original.clone(), &params(Some(3), None), true).unwrap();
        let mut fingerprints = BTreeSet::new();
        let mut page = first.clone();
        loop {
            let mut count = 0;
            for group in page["alerts"].as_array().unwrap() {
                for peer in group["peers"].as_array().unwrap() {
                    assert!(fingerprints.insert(peer["fingerprint"].as_str().unwrap().to_owned()));
                    count += 1;
                    for summary in [peer.get("summary"), group.get("summary")]
                        .into_iter()
                        .flatten()
                    {
                        assert!(summary.as_str().unwrap().chars().count() <= 200);
                    }
                }
            }
            assert!(count <= 3);
            let Some(cursor) = page["next_cursor"].as_str() else {
                break;
            };
            // Reversing upstream order does not invalidate the cursor.
            original["alerts"].as_array_mut().unwrap().reverse();
            page = alerts(
                original.clone(),
                &params(Some(3), Some(cursor.to_owned())),
                true,
            )
            .unwrap();
        }
        assert_eq!(fingerprints.len(), 20);
        original["alerts"].as_array_mut().unwrap().pop();
        assert_eq!(
            alerts(
                original,
                &params(Some(3), first["next_cursor"].as_str().map(str::to_owned)),
                true
            )
            .unwrap_err()
            .0,
            StatusCode::CONFLICT
        );
        for limit in [0, 201] {
            assert!(alerts(capture(), &params(Some(limit), None), true).is_err());
        }
    }

    fn metric(samples: &[(&str, f64, Value)]) -> Value {
        json!({"state":if samples.iter().all(|(state,_,_)| *state == "available") {"available"} else {"partial"},
            "samples": samples.iter().map(|(state,value,labels)| json!({"state":state, "value":value, "labels":labels})).collect::<Vec<_>>()})
    }

    #[test]
    fn aggregates_do_not_hide_stale_ambiguous_or_missing_series() {
        let mut observation = json!({"state":"available", "metrics":{
            "cpu":metric(&[("available",0.2,json!({"cpu":"0"})),("available",0.8,json!({"cpu":"1"})),("stale",99.0,json!({"cpu":"2"})),("ambiguous",99.0,json!({"cpu":"3"}))]),
            "missing":{"state":"unknown","samples":[]}
        }});
        aggregate(&mut observation);
        let cpu = &observation["metrics"]["cpu"];
        assert_eq!(cpu["state"], "partial");
        assert_eq!(cpu["series_count"], 4);
        assert_eq!(cpu["available_count"], 2);
        assert_eq!(cpu["min"], 0.2);
        assert_eq!(cpu["max"], 0.8);
        assert_eq!(cpu["mean"], 0.5);
        assert_eq!(cpu["states"]["stale"], 1);
        assert!(observation["metrics"]["missing"]["mean"].is_null());
        assert!(cpu.get("samples").is_none());
        let once = observation.clone();
        aggregate(&mut observation);
        assert_eq!(observation, once);
        let mut unavailable = json!({"state":"unavailable","metrics":null});
        aggregate(&mut unavailable);
        assert!(unavailable["metrics"].is_null());
    }

    #[test]
    fn pressure_pairs_filesystems_and_retains_partial_coverage() {
        let root = json!({"mountpoint":"/", "device":"/dev/root"});
        let data = json!({"mountpoint":"/data", "device":"/dev/data"});
        let mut observation = json!({"state":"available","metrics":{
            "load1":metric(&[("available",2.5,json!({}))]),
            "memory_available_bytes":metric(&[("available",25.0,json!({}))]),
            "memory_total_bytes":metric(&[("available",100.0,json!({}))]),
            "filesystem_available_bytes":metric(&[("available",50.0,root.clone()),("available",20.0,data.clone())]),
            "filesystem_size_bytes":metric(&[("available",200.0,data),("available",100.0,root)])
        }});
        let projected = pressure(&observation);
        assert_eq!(projected["state"], "available");
        assert_eq!(projected["memory_used_fraction"], 0.75);
        assert_eq!(projected["worst_filesystem"]["mountpoint"], "/data");
        assert_eq!(projected["worst_filesystem"]["available_fraction"], 0.1);
        observation["metrics"]["filesystem_size_bytes"]["samples"][0]["state"] = json!("stale");
        let projected = pressure(&observation);
        assert_eq!(projected["state"], "partial");
        assert_eq!(projected["filesystem_state"], "partial");
        assert_eq!(projected["worst_filesystem"]["mountpoint"], "/");
        assert!(
            pressure(&json!({"state":"unavailable","metrics":null}))["memory_used_fraction"]
                .is_null()
        );
    }
}
