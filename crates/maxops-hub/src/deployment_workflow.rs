//! A fixed durable workflow over existing deployment primitives. The store
//! atomically links each child before dispatch, including on recovery.
use super::*;
use maxops_proto::{DeployRunParams, DeployUntil};

pub(super) fn is_local(operation: &str) -> bool {
    matches!(
        operation,
        "deploy.run" | "diagnostics.collect" | "remediations.begin"
    )
}

pub(super) async fn submit(
    app: &Arc<App>,
    principal: &Principal,
    headers: &HeaderMap,
    params: DeployRunParams,
) -> Result<(StatusCode, Value), ApiError> {
    let store = durable_store(app)?;
    let key = idempotency_key(headers)?;
    if let Some(job) = store
        .get_idempotent_job(&principal.name, key)
        .await
        .map_err(map_store_error)?
    {
        if job.handle.operation != "deploy.run"
            || job.spec != serde_json::to_value(&params).expect("parameters")
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "idempotency key conflicts with another request",
            ));
        }
        host_for(app, principal, &job.handle.host)?;
        let handle = job.handle.clone();
        if !handle.state.is_terminal() {
            spawn(app.clone(), job);
        }
        return Ok((StatusCode::ACCEPTED, json!(handle)));
    }
    let change = refresh_change(app, principal, &params.change_id).await?;
    if change.revision != params.expected_revision {
        return Err(ApiError(StatusCode::CONFLICT, "change revision changed"));
    }
    if !matches!(change.state, ChangeState::Prepared | ChangeState::Ready) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "change is not ready for this stage",
        ));
    }
    let deployment = deployment_for(app, principal, &change.plan.deployment_profile)?;
    let id = JobId::parse(uuid::Uuid::now_v7().to_string()).expect("UUID");
    let job = NewJob {
        principal: principal.name.clone(),
        host: deployment.target_host.clone(),
        operation: "deploy.run".into(),
        spec_version: 1,
        spec: json!(params),
        policy_version: change.plan.policy_version.clone(),
        deadline: Some(
            now()
                .checked_add(Duration::from_secs(12_000))
                .expect("bounded deadline"),
        ),
    };
    let submitted = store
        .submit_linked_job(
            key,
            &id,
            &job,
            Some(maxops_store::ChangeJobLink {
                change: &change,
                next: change.state,
                action: None,
                workflow: None,
            }),
        )
        .await
        .map_err(map_store_error)?;
    let handle = submitted.job.handle.clone();
    if !handle.state.is_terminal() {
        spawn(app.clone(), submitted.job);
    }
    Ok((StatusCode::ACCEPTED, json!(handle)))
}

pub(super) fn spawn(app: Arc<App>, job: JobRecord) {
    let id = job.handle.job_id.clone();
    if !app
        .local_workers
        .lock()
        .expect("local worker lock")
        .insert(id.clone())
    {
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = run(app.clone(), job).await {
            // Unexpected local failure cannot prove the outcome of an already
            // dispatched child. Keep ownership fenced until reconciliation.
            finish(
                &app,
                &id,
                JobState::OutcomeUnknown,
                json!({"error":"workflow_observation_failed"}),
            )
            .await
            .ok();
            tracing::warn!(job_id = %id, %error, "deployment workflow stopped");
        }
        app.local_workers
            .lock()
            .expect("local worker lock")
            .remove(&id);
    });
}

fn active_child(change: &ChangeRecord) -> Option<&JobId> {
    match change.state {
        ChangeState::Building | ChangeState::Publishing => change.jobs.build.as_ref(),
        ChangeState::Activating => change.jobs.activate.as_ref(),
        ChangeState::Verifying => change.jobs.verify.as_ref(),
        ChangeState::Recovering => change.jobs.rollback.as_ref(),
        _ => None,
    }
}

async fn run(app: Arc<App>, job: JobRecord) -> color_eyre::eyre::Result<()> {
    use color_eyre::eyre::{ensure, eyre};
    let store = app.store.as_ref().ok_or_else(|| eyre!("store disabled"))?;
    let params: DeployRunParams = serde_json::from_value(job.spec.clone())?;
    let principal = app
        .clients
        .iter()
        .find(|principal| principal.name == job.principal)
        .ok_or_else(|| eyre!("workflow principal revoked"))?;
    ensure!(
        principal.access == Access::Manage && principal.capabilities.contains("deploy:manage"),
        "workflow capability revoked"
    );
    let id = job.handle.job_id.clone();
    // Persist wall-clock deadlines for restart, then use one monotonic timer
    // for this worker. A clock adjustment cannot extend its execution window.
    let remaining = job.deadline.map_or(12_000, |deadline| {
        deadline
            .as_second()
            .saturating_sub(now().as_second())
            .clamp(0, 12_000) as u64
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(remaining);
    // Re-read before each local transition: jobs.cancel may advance the
    // revision between submission and this worker starting.
    loop {
        let current = store.get_job(&id).await?;
        if current.handle.state.is_terminal() {
            return Ok(());
        }
        if matches!(
            current.handle.state,
            JobState::Queued | JobState::Dispatching
        ) && (current.cancel_requested || tokio::time::Instant::now() >= deadline)
        {
            return finish(
                &app,
                &id,
                if current.cancel_requested {
                    JobState::Cancelled
                } else {
                    JobState::TimedOut
                },
                json!({"change_id":params.change_id,"reversed":false}),
            )
            .await;
        }
        match enter_local_job(store, current).await {
            Ok(_) => break,
            Err(error) if error.to_string() == "job revision changed" => continue,
            Err(error) => return Err(error),
        }
    }
    loop {
        let job = store.get_job(&id).await?;
        if job.handle.state.is_terminal() {
            return Ok(());
        }
        let expired = tokio::time::Instant::now() >= deadline;
        let change = match refresh_change(&app, principal, &params.change_id).await {
            Ok(change) => change,
            Err(error)
                if !expired && (error.0.is_server_error() || error.0 == StatusCode::CONFLICT) =>
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(_) => return Err(eyre!("cannot observe owned change")),
        };
        if job.cancel_requested || expired {
            if let Some(child_id) = active_child(&change) {
                let child = store.get_owned_job(&principal.name, child_id).await?;
                if !child.handle.state.is_terminal() {
                    if let Ok(ExecutorResponse::Job(target)) = agent_request(
                        &app,
                        &child.handle.host,
                        &ExecutorRequest::Status(JobIdParams {
                            job_id: child_id.clone(),
                        }),
                    )
                    .await
                    {
                        if !target.handle.state.is_terminal() {
                            if let Ok(ExecutorResponse::Job(target)) = agent_request(
                                &app,
                                &child.handle.host,
                                &ExecutorRequest::Cancel(JobCancelParams {
                                    job_id: child_id.clone(),
                                    expected_revision: target.handle.revision,
                                    reason: "deployment workflow stopped".into(),
                                }),
                            )
                            .await
                            {
                                project_target_job(store, child.clone(), target).await?;
                            }
                        } else {
                            project_target_job(store, child.clone(), target).await?;
                        }
                    }
                    let child = store.get_job(child_id).await?;
                    if !child.handle.state.is_terminal() {
                        if expired {
                            return finish(&app, &id, JobState::OutcomeUnknown, json!({"change_id": params.change_id, "child_job_id": child_id, "error":"child_cancellation_unconfirmed"})).await;
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    if child.handle.state == JobState::OutcomeUnknown {
                        return finish(
                            &app,
                            &id,
                            JobState::OutcomeUnknown,
                            json!({"change_id": params.change_id, "child_job_id": child_id}),
                        )
                        .await;
                    }
                }
            }
            return finish(&app, &id, if expired { JobState::TimedOut } else { JobState::Cancelled }, json!({"change_id": params.change_id, "change_state": change.state, "reversed": false})).await;
        }
        if (params.until == DeployUntil::Built && change.state == ChangeState::Ready)
            || change.state == ChangeState::Succeeded
        {
            return finish(&app, &id, JobState::Succeeded, json!({"change_id": params.change_id, "change_revision": change.revision, "change_state": change.state, "until": params.until, "jobs": change.jobs})).await;
        }
        if change.state.is_terminal() {
            return finish(&app, &id, if change.state == ChangeState::OutcomeUnknown { JobState::OutcomeUnknown } else { JobState::Failed },
                json!({"change_id": params.change_id, "change_state": change.state, "recovery_state": change.recovery_state, "jobs": change.jobs})).await;
        }
        let action = match change.state {
            ChangeState::Prepared => Some(DeploymentAction::Build),
            ChangeState::Ready => Some(DeploymentAction::Activate),
            ChangeState::Verifying if change.jobs.verify.is_none() => {
                Some(DeploymentAction::Verify)
            }
            _ => None,
        };
        if let Some(action) = action {
            let mut headers = HeaderMap::new();
            headers.insert(
                "idempotency-key",
                format!("workflow:{id}:{}", operation_for_deployment(action))
                    .parse()
                    .expect("bounded key"),
            );
            match submit_change_stage_owned(
                &app,
                principal,
                &headers,
                DeployChangeParams {
                    change_id: params.change_id.clone(),
                    expected_revision: change.revision,
                },
                action,
                Some(&id),
            )
            .await
            {
                Ok(_) => {}
                Err(error) if error.0 == StatusCode::CONFLICT || error.0.is_server_error() => {}
                Err(_) => return Err(eyre!("workflow stage rejected")),
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn finish(
    app: &App,
    id: &JobId,
    state: JobState,
    result: Value,
) -> color_eyre::eyre::Result<()> {
    let store = app.store.as_ref().expect("workflow store");
    loop {
        let job = store.get_job(id).await?;
        if job.handle.state.is_terminal() {
            return Ok(());
        }
        let state = if state == JobState::OutcomeUnknown && job.handle.state == JobState::Queued {
            JobState::Failed
        } else {
            state
        };
        let next = if state == JobState::OutcomeUnknown && job.handle.state != JobState::Reconciling
        {
            JobState::Reconciling
        } else {
            state
        };
        match store
            .transition_job(
                id,
                job.handle.revision,
                next,
                &json!({"executor":"hub"}),
                (next == state).then_some(&result),
            )
            .await
        {
            Ok(_) if next == state => return Ok(()),
            Ok(_) => {}
            Err(error) if error.to_string() == "job revision changed" => {}
            Err(error) => return Err(error),
        }
    }
}

/// Reconcile evidence only. An unknown workflow must never resume stage
/// submission just because a previously unreachable executor became available.
pub(super) async fn reconcile(
    app: &Arc<App>,
    principal: &Principal,
    mut job: JobRecord,
) -> Result<JobRecord, ApiError> {
    if job.handle.state != JobState::OutcomeUnknown || job.handle.operation != "deploy.run" {
        return Ok(job);
    }
    let params: DeployRunParams = serde_json::from_value(job.spec.clone())
        .map_err(|_| ApiError(StatusCode::CONFLICT, "invalid workflow record"))?;
    let change = refresh_change(app, principal, &params.change_id).await?;
    let store = durable_store(app)?;
    let mut evidence = Vec::new();
    for id in [
        &change.jobs.build,
        &change.jobs.activate,
        &change.jobs.verify,
        &change.jobs.rollback,
    ]
    .into_iter()
    .flatten()
    {
        let mut child = store
            .get_owned_job(&principal.name, id)
            .await
            .map_err(map_store_error)?;
        host_for(app, principal, &child.handle.host)?;
        if (!child.handle.state.is_terminal() || child.handle.state == JobState::OutcomeUnknown)
            && let Ok(ExecutorResponse::Job(target)) = agent_request(
                app,
                &child.handle.host,
                &ExecutorRequest::Status(JobIdParams { job_id: id.clone() }),
            )
            .await
        {
            child = project_target_job(store, child, target)
                .await
                .map_err(map_store_error)?;
        }
        if !child.handle.state.is_terminal() || child.handle.state == JobState::OutcomeUnknown {
            return Ok(job);
        }
        evidence.push(json!({"handle": child.handle, "result_available": child.result.is_some()}));
    }
    let attained = change.state == ChangeState::Succeeded
        || (params.until == DeployUntil::Built && change.state == ChangeState::Ready);
    let result = json!({"change_id": params.change_id, "change_state":change.state, "jobs":evidence,
        "stages_resumed":false, "detail":"workflow stopped; remote effects reconciled"});
    match store
        .transition_job(
            &job.handle.job_id,
            job.handle.revision,
            if attained {
                JobState::Succeeded
            } else if job.cancel_requested {
                JobState::Cancelled
            } else {
                JobState::Failed
            },
            &json!({"executor":"hub","observation_only":true}),
            Some(&result),
        )
        .await
    {
        Ok(updated) => job = updated,
        Err(error) if error.to_string() == "job revision changed" => {
            job = store
                .get_job(&job.handle.job_id)
                .await
                .map_err(map_store_error)?
        }
        Err(error) => return Err(map_store_error(error)),
    }
    Ok(job)
}
