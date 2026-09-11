//! Client presentation and observation. Execution state transitions stay in
//! the deployment/job coordinators, not in catalog or response formatting.
use super::*;
use axum::extract::Query;
use maxops_proto::{
    DiscoveryResourceKind, JobEventsParams, JobResultParams, JobWaitParams, ResourcesParams,
};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogQuery {
    view: Option<String>,
    name: Option<String>,
    category: Option<String>,
    query: Option<String>,
    limit: Option<u16>,
    cursor: Option<String>,
}

pub(super) fn catalog_value(principal: &Principal, query: CatalogQuery) -> Result<Value, ApiError> {
    let view = query.view.as_deref().unwrap_or("full");
    if !matches!(view, "summary" | "tools" | "full") {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid catalog view"));
    }
    let mut values = Vec::new();
    for operation in operations()
        .into_iter()
        .filter(|op| principal.capabilities.contains(op.capability))
    {
        if query
            .name
            .as_ref()
            .is_some_and(|name| name != operation.name)
            || query
                .category
                .as_ref()
                .is_some_and(|category| operation.name.split('.').next() != Some(category.as_str()))
            || query.query.as_ref().is_some_and(|q| {
                !format!("{} {}", operation.name, operation.summary)
                    .to_lowercase()
                    .contains(&q.to_lowercase())
            })
        {
            continue;
        }
        let mut value = serde_json::to_value(operation).expect("serializable operation");
        let fields = value.as_object_mut().expect("operation object");
        if view != "full" {
            fields.remove("response_schema");
        }
        if view == "summary" {
            fields.remove("params_schema");
        }
        values.push(value);
    }
    let mut result = page(
        values,
        query.limit.unwrap_or(200),
        query.cursor.as_deref(),
        "operations",
    )?;
    result["version"] = json!(PROTOCOL_VERSION);
    result["view"] = json!(view);
    Ok(result)
}

// Cursor binds the complete authorized, filtered representation. It cannot be
// moved between principals with different scope, filters, or catalog versions.
pub(super) fn page(
    values: Vec<Value>,
    limit: u16,
    cursor: Option<&str>,
    key: &str,
) -> Result<Value, ApiError> {
    if !(1..=200).contains(&limit) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "list limit must be 1..200",
        ));
    }
    let revision = blake3::hash(&serde_json::to_vec(&values).expect("JSON values"))
        .to_hex()
        .to_string();
    let offset = match cursor {
        None => 0,
        Some(cursor) => {
            let (version, offset) = cursor
                .split_once(':')
                .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid cursor"))?;
            if version != revision {
                return Err(ApiError(StatusCode::CONFLICT, "catalog revision changed"));
            }
            offset
                .parse::<usize>()
                .ok()
                .filter(|offset| *offset <= values.len())
                .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid cursor"))?
        }
    };
    let end = (offset + usize::from(limit)).min(values.len());
    Ok(
        json!({(key): values[offset..end], "revision": revision, "total": values.len(),
        "next_cursor": (end < values.len()).then(|| format!("{revision}:{end}"))}),
    )
}

pub(super) async fn resources(
    app: &App,
    principal: &Principal,
    params: ResourcesParams,
) -> Result<Value, ApiError> {
    if let Some(host) = &params.host {
        host_for(app, principal, host)?;
    }
    let hosts: Vec<_> = app
        .hosts
        .values()
        .filter(|host| {
            principal.hosts.contains(&host.config.name)
                && params
                    .host
                    .as_ref()
                    .is_none_or(|name| name == &host.config.name)
        })
        .collect();
    let mut entries = Vec::new();
    match params.kind {
        DiscoveryResourceKind::Hosts => {
            for host in hosts {
                entries.push(json!({"host": host.config.name, "site": host.config.site, "read_all_units": host.config.read_all_units}));
            }
        }
        DiscoveryResourceKind::Units => {
            for host in hosts {
                let readable = principal.capabilities.contains("units:read");
                let manageable = principal.capabilities.contains("units:manage");
                let mut names = host
                    .config
                    .readable_units
                    .union(&host.config.manageable_units)
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if readable && host.config.read_all_units {
                    let snapshot = observe(app, host).await.map_err(|_| {
                        ApiError(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "unit discovery unavailable",
                        )
                    })?;
                    names.extend(snapshot.units.into_iter().map(|unit| unit.unit));
                }
                for unit in &names {
                    let read = readable
                        && (host.config.read_all_units
                            || host.config.readable_units.contains(unit));
                    let manage = manageable && host.config.manageable_units.contains(unit);
                    if read || manage {
                        entries.push(json!({"host": host.config.name, "unit": unit, "readable": read, "manageable": manage}));
                    }
                }
            }
        }
        DiscoveryResourceKind::Repositories => {
            if principal.capabilities.contains("workspace:read")
                || principal.capabilities.contains("workspace:write")
            {
                for (name, host) in &app.repositories {
                    if principal.repositories.contains(name)
                        && principal.hosts.contains(host)
                        && params.host.as_ref().is_none_or(|selected| selected == host)
                    {
                        entries.push(json!({"repository": name, "host": host}));
                    }
                }
            }
        }
        DiscoveryResourceKind::Deployments => {
            if principal.capabilities.contains("deploy:manage")
                || principal.capabilities.contains("changes:read")
            {
                for deployment in app.deployments.values() {
                    if deployment_for(app, principal, &deployment.name).is_ok()
                        && params.host.as_ref().is_none_or(|host| {
                            host == &deployment.target_host || host == &deployment.builder_host
                        })
                    {
                        entries.push(json!({"profile": deployment.name, "repository": deployment.repository,
                        "builder_host": deployment.builder_host, "target_host": deployment.target_host, "kind": deployment.kind}));
                    }
                }
            }
        }
        DiscoveryResourceKind::DiagnosticProbes => {
            if !principal.capabilities.contains("diagnostics:collect") {
                return Err(ApiError(StatusCode::FORBIDDEN, "capability not permitted"));
            }
            for host in app.hosts.values().filter(|host| {
                principal.hosts.contains(&host.config.name)
                    && params
                        .host
                        .as_ref()
                        .is_none_or(|selected| selected == &host.config.name)
            }) {
                for (name, argv) in &host.config.diagnostic_probes {
                    entries.push(json!({"host":host.config.name,"probe":name,"argv":argv,"profile":host.config.diagnostic_profile}));
                }
            }
        }
        DiscoveryResourceKind::ExecutionProfiles => {
            if !principal.capabilities.contains("exec:run") {
                return Err(ApiError(StatusCode::FORBIDDEN, "capability not permitted"));
            }
            // Target discovery is explicit and bounded to one host. No catalog
            // request fans out into fleet HTTP traffic.
            let host = params.host.as_deref().ok_or(ApiError(
                StatusCode::BAD_REQUEST,
                "execution profiles require a host",
            ))?;
            match agent_request(app, host, &ExecutorRequest::ExecutionProfiles).await {
                Ok(ExecutorResponse::ExecutionProfiles(profiles)) => {
                    for profile in profiles {
                        entries.push(json!({"host": host, "profile": profile}));
                    }
                }
                _ => {
                    return Err(ApiError(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "executor unavailable",
                    ));
                }
            }
        }
    }
    page(entries, params.limit, params.cursor.as_deref(), "resources")
}

pub(super) fn job_summary(job: &JobRecord) -> Value {
    json!({"handle": job.handle, "created_at": job.created_at, "updated_at": job.updated_at,
        "deadline": job.deadline, "cancel_requested": job.cancel_requested,
        "result_available": job.result.is_some(),
        "evidence_status": job.result.as_ref().and_then(|value| value.pointer("/diagnostic/collection_status")),
        "missing_evidence": job.result.as_ref().and_then(|value| value.pointer("/diagnostic/missing_evidence")),
        "result": job.result.as_ref().filter(|value| serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= 8192))})
}

pub(super) async fn wait(
    app: &App,
    principal: &Principal,
    params: JobWaitParams,
) -> Result<Value, ApiError> {
    if params.timeout_seconds > 10 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "wait timeout must be 0..10 seconds",
        ));
    }
    let store = durable_store(app)?;
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(u64::from(params.timeout_seconds));
    let mut revision = params.after_revision;
    loop {
        // Subscribe before reading to avoid a lost notification between the
        // SELECT and await. A bounded refresh also covers another store owner.
        let notified = store.job_changed();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let job = store
            .get_owned_job(&principal.name, &params.job_id)
            .await
            .map_err(map_store_error)?;
        host_for(app, principal, &job.handle.host)?;
        let previous = *revision.get_or_insert(job.handle.revision);
        let timed_out = tokio::time::Instant::now() >= deadline;
        if job.handle.state.is_terminal() || job.handle.revision != previous || timed_out {
            return Ok(
                json!({"job": job_summary(&job), "timed_out": timed_out && !job.handle.state.is_terminal() && job.handle.revision == previous}),
            );
        }
        tokio::select! {
            _ = &mut notified => {},
            _ = tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + Duration::from_secs(1))) => {},
        }
    }
}

pub(super) async fn events(
    app: &App,
    principal: &Principal,
    params: JobEventsParams,
) -> Result<Value, ApiError> {
    if !(1..=200).contains(&params.limit) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "event limit must be 1..200",
        ));
    }
    let store = durable_store(app)?;
    let job = store
        .get_owned_job(&principal.name, &params.job_id)
        .await
        .map_err(map_store_error)?;
    host_for(app, principal, &job.handle.host)?;
    let events = store
        .job_events_page(&params.job_id, params.after_sequence, params.limit)
        .await
        .map_err(map_store_error)?;
    Ok(
        json!({"next_sequence": events.last().map_or(params.after_sequence, |event| event.sequence), "events": events, "terminal": job.handle.state.is_terminal()}),
    )
}

pub(super) async fn result(
    app: &App,
    principal: &Principal,
    params: JobResultParams,
) -> Result<Value, ApiError> {
    if !(1..=32768).contains(&params.limit) || params.pointer.len() > 1024 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid result bounds"));
    }
    let job = durable_store(app)?
        .get_owned_job(&principal.name, &params.job_id)
        .await
        .map_err(map_store_error)?;
    host_for(app, principal, &job.handle.host)?;
    let Some(value) = &job.result else {
        return Ok(json!({"available": false, "handle": job.handle}));
    };
    let mut fragment = json_fragment(value, &params.pointer, params.offset, params.limit)?;
    fragment["job_id"] = json!(params.job_id);
    Ok(fragment)
}

pub(super) fn json_fragment(
    value: &Value,
    pointer: &str,
    offset: u64,
    limit: u32,
) -> Result<Value, ApiError> {
    if !(1..=32768).contains(&limit) || pointer.len() > 1024 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid result bounds"));
    }
    let selected = value
        .pointer(pointer)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "result pointer not found"))?;
    let text = serde_json::to_string(selected).expect("stored JSON");
    let start = usize::try_from(offset)
        .ok()
        .filter(|offset| *offset <= text.len() && text.is_char_boundary(*offset))
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid result offset"))?;
    let mut end = (start + limit as usize).min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    if end == start && end < text.len() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "result limit is smaller than a UTF-8 character",
        ));
    }
    Ok(json!({"available": true, "pointer": pointer,
        "encoding": "json_utf8", "text": &text[start..end], "next_offset": end,
        "total_bytes": text.len(), "complete": end == text.len()}))
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Representation {
    view: Option<String>,
    encoding: Option<String>,
}

impl Representation {
    pub(super) fn validate(&self) -> Result<(), ApiError> {
        let query = self;
        if query
            .view
            .as_deref()
            .is_some_and(|view| !matches!(view, "full" | "summary"))
            || query
                .encoding
                .as_deref()
                .is_some_and(|encoding| !matches!(encoding, "base64" | "text"))
        {
            return Err(ApiError(StatusCode::BAD_REQUEST, "invalid representation"));
        }
        Ok(())
    }
}

pub(super) fn present(
    Query(query): Query<Representation>,
    operation: &str,
    mut value: Value,
) -> Result<Value, ApiError> {
    query.validate()?;
    if query.view.as_deref() == Some("summary") {
        match operation {
            "jobs.status" | "jobs.cancel" => {
                if let Ok(job) = serde_json::from_value::<JobRecord>(value.clone()) {
                    value = job_summary(&job);
                }
            }
            "jobs.list" => {
                if let Some(jobs) = value.get_mut("jobs").and_then(Value::as_array_mut) {
                    for job in jobs {
                        if let Ok(record) = serde_json::from_value::<JobRecord>(job.clone()) {
                            *job = job_summary(&record);
                        }
                    }
                }
            }
            "events.list" | "events.recent" => {
                if let Some(events) = value.get_mut("events").and_then(Value::as_array_mut) {
                    for event in events {
                        let payload = event
                            .as_object_mut()
                            .and_then(|fields| fields.remove("payload"))
                            .unwrap_or(Value::Null);
                        event["summary"] = json!(
                            payload["annotations"]["summary"]
                                .as_str()
                                .or_else(|| payload["summary"].as_str())
                                .unwrap_or("")
                                .chars()
                                .take(500)
                                .collect::<String>()
                        );
                        event["unit"] = payload["labels"]["unit"]
                            .as_str()
                            .or_else(|| payload["labels"]["name"].as_str())
                            .or_else(|| payload["unit"].as_str())
                            .map_or(Value::Null, |unit| json!(unit));
                        event["payload_available"] = json!(!payload.is_null());
                    }
                }
            }
            "changes.status" => compact_change(&mut value),
            "changes.history" => {
                if let Some(changes) = value.get_mut("changes").and_then(Value::as_array_mut) {
                    for change in changes {
                        compact_change(change);
                    }
                }
            }
            _ => {}
        }
    }
    if query.encoding.as_deref() == Some("text") && operation == "jobs.logs" {
        use base64::Engine;
        let fields = value
            .as_object_mut()
            .ok_or(ApiError(StatusCode::BAD_GATEWAY, "invalid logs"))?;
        for (binary, text) in [
            ("stdout_base64", "stdout_text"),
            ("stderr_base64", "stderr_text"),
        ] {
            let bytes = fields
                .remove(binary)
                .and_then(|value| value.as_str().map(str::to_owned))
                .and_then(|encoded| {
                    base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .ok()
                })
                .ok_or(ApiError(StatusCode::BAD_GATEWAY, "invalid logs"))?;
            fields.insert(text.into(), json!(String::from_utf8_lossy(&bytes)));
        }
        fields.insert("encoding".into(), json!("utf8_with_replacement"));
    }
    Ok(value)
}

fn compact_change(value: &mut Value) {
    if let Some(fields) = value.as_object_mut() {
        if let Some(plan) = fields.remove("plan") {
            for name in [
                "change_id",
                "target_host",
                "deployment_profile",
                "source_commit",
                "expires_at",
            ] {
                if let Some(value) = plan.get(name) {
                    fields.insert(name.into(), value.clone());
                }
            }
        }
        fields.remove("artifact");
    }
}
