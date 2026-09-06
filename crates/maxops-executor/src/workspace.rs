use super::{App, RepositoryConfig};
use color_eyre::eyre::{Context, Result, ensure, eyre};
use maxops_proto::{
    JobId, JobRecord, JobState, NewJob, WorkspaceApplyParams, WorkspaceCheckParams,
    WorkspaceCommitParams, WorkspaceCreateParams, WorkspaceDiff, WorkspaceFile, WorkspaceId,
    WorkspacePublishParams, WorkspaceReadParams, WorkspaceRecord, WorkspaceRevisionParams,
    WorkspaceState, WorkspaceTargetRequest, WorkspaceTargetResponse,
};
use serde_json::json;
use std::{
    collections::HashSet,
    ffi::OsString,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use tokio::{io::AsyncWriteExt, process::Command};

const MAX_GIT_OUTPUT: usize = 1024 * 1024;

pub(super) fn is_workspace_job(operation: &str) -> bool {
    matches!(
        operation,
        "workspace.create" | "workspace.check" | "workspace.publish"
    )
}

pub(super) async fn validate_job(app: &App, job: &NewJob) -> Result<()> {
    match job.operation.as_str() {
        "workspace.create" => {
            let params: WorkspaceCreateParams = serde_json::from_value(job.spec.clone())?;
            params.validate().map_err(|message| eyre!(message))?;
            repository(app, &params.repository)?;
        }
        "workspace.check" => {
            let params: WorkspaceCheckParams = serde_json::from_value(job.spec.clone())?;
            params.validate().map_err(|message| eyre!(message))?;
            let repository = repository(app, &params.repository)?;
            ensure!(
                repository.checks.contains_key(&params.check),
                "workspace check is not configured"
            );
            let workspace = app
                .store
                .get_owned_workspace(&job.principal, &params.repository, &params.workspace_id)
                .await?;
            ensure!(
                workspace.revision == params.expected_revision,
                "workspace revision changed"
            );
        }
        "workspace.publish" => {
            let params: WorkspacePublishParams = serde_json::from_value(job.spec.clone())?;
            params.validate().map_err(|message| eyre!(message))?;
            let repository = repository(app, &params.repository)?;
            ensure!(
                repository.publish_refs.contains(&params.reference),
                "publish ref is not configured"
            );
            let workspace = app
                .store
                .get_owned_workspace(&job.principal, &params.repository, &params.workspace_id)
                .await?;
            ensure!(
                workspace.revision == params.expected_revision,
                "workspace revision changed"
            );
            ensure!(
                workspace.commit_hash.is_some(),
                "workspace has no commit to publish"
            );
        }
        _ => return Err(eyre!("unsupported workspace job")),
    }
    Ok(())
}

pub(super) fn spawn(app: Arc<App>, id: JobId) {
    let inserted = app
        .workspace_workers
        .lock()
        .expect("workspace worker lock poisoned")
        .insert(id.clone());
    if !inserted {
        return;
    }
    tokio::spawn(async move {
        if let Err(error) = run_job(&app, &id).await {
            tracing::warn!(job_id = %id, %error, "workspace job stopped");
            if let Err(record_error) = record_worker_error(&app, &id).await {
                tracing::error!(job_id = %id, %record_error, "could not record workspace job failure");
            }
        }
        app.workspace_workers
            .lock()
            .expect("workspace worker lock poisoned")
            .remove(&id);
    });
}

async fn run_job(app: &App, id: &JobId) -> Result<()> {
    let job = app.store.get_job(id).await?;
    if job.handle.state.is_terminal() {
        release_job_resource(app, &job).await?;
        return Ok(());
    }
    match job.handle.operation.as_str() {
        "workspace.create" => create_job(app, id, job).await,
        "workspace.publish" => publish_job(app, id, job).await,
        // workspace.check runs through the hardened transient command runner.
        "workspace.check" => Ok(()),
        _ => Err(eyre!("unsupported workspace job")),
    }
}

async fn create_job(app: &App, id: &JobId, mut job: JobRecord) -> Result<()> {
    let params: WorkspaceCreateParams = serde_json::from_value(job.spec.clone())?;
    let repository = repository(app, &params.repository)?;
    while !app
        .store
        .try_acquire_resource("repository_fetch", &params.repository, id)
        .await?
    {
        job = app.store.get_job(id).await?;
        if job.handle.state.is_terminal() {
            return Ok(());
        }
        if job.cancel_requested {
            app.store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::Cancelled,
                    &json!({"phase":"repository_queue"}),
                    Some(&json!({"effect":"not_started"})),
                )
                .await?;
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    job = mark_workspace_job_running(app, id, job, "fetching_configured_remote").await?;
    let workspace_id =
        WorkspaceId::parse(id.as_str().to_owned()).map_err(|message| eyre!(message))?;
    if let Ok(existing) = app.store.get_workspace(&workspace_id).await {
        ensure!(
            existing.creator == job.principal && existing.repository == params.repository,
            "workspace identity conflict"
        );
        if !job.handle.state.is_terminal() {
            app.store
                .transition_job(
                    id,
                    job.handle.revision,
                    JobState::Succeeded,
                    &json!({"phase":"workspace_already_materialized"}),
                    Some(&json!({"workspace":existing,"recovered":true})),
                )
                .await?;
        }
        app.store
            .release_resource("repository_fetch", &params.repository, id)
            .await?;
        return Ok(());
    }
    ensure!(
        job.handle.state == JobState::Running,
        "invalid workspace create state"
    );
    let _serial = app.workspace_serial.lock().await;
    let mirror = ensure_mirror(app, &params.repository, repository).await?;
    fetch(app, &mirror, repository).await?;
    let remote_head = resolve_remote_head(app, &mirror, &repository.default_ref).await?;
    if params
        .expected_remote_head
        .as_deref()
        .is_some_and(|expected| !expected.eq_ignore_ascii_case(&remote_head))
    {
        app.store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Failed,
                &json!({"phase":"remote_baseline","reason":"baseline_changed"}),
                Some(&json!({
                    "error":"baseline_changed",
                    "expected_remote_head":params.expected_remote_head,
                    "observed_remote_head":remote_head,
                    "reference":repository.default_ref,
                    "observed_at":maxops_proto::now(),
                })),
            )
            .await?;
        app.store
            .release_resource("repository_fetch", &params.repository, id)
            .await?;
        return Ok(());
    }
    let tree_hash = git_text(
        app,
        Some(&mirror),
        None,
        ["rev-parse", &format!("{remote_head}^{{tree}}")],
    )
    .await?;
    materialize_revision(app, &mirror, &workspace_id, 1, &remote_head).await?;
    let workspace = app
        .store
        .create_workspace(
            &workspace_id,
            &params.repository,
            &app.config.host,
            &remote_head,
            &tree_hash,
            &job.principal,
        )
        .await?;
    app.store
        .transition_job(
            id,
            job.handle.revision,
            JobState::Succeeded,
            &json!({"phase":"workspace_materialized"}),
            Some(&json!({
                "workspace":workspace,
                "remote":{"reference":repository.default_ref,"commit":remote_head,"observed_at":maxops_proto::now()},
            })),
        )
        .await?;
    app.store
        .release_resource("repository_fetch", &params.repository, id)
        .await?;
    Ok(())
}

async fn publish_job(app: &App, id: &JobId, mut job: JobRecord) -> Result<()> {
    let params: WorkspacePublishParams = serde_json::from_value(job.spec.clone())?;
    let repository = repository(app, &params.repository)?;
    ensure!(
        repository.publish_refs.contains(&params.reference),
        "publish ref is no longer configured"
    );
    let resource = format!("{}:{}", params.repository, params.reference);
    while !app
        .store
        .try_acquire_resource("repository_publish", &resource, id)
        .await?
    {
        job = app.store.get_job(id).await?;
        if job.handle.state.is_terminal() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let workspace = app
        .store
        .get_owned_workspace(&job.principal, &params.repository, &params.workspace_id)
        .await?;
    let published_revision = params.expected_revision.checked_add(1);
    ensure!(
        workspace.revision == params.expected_revision
            || (published_revision == Some(workspace.revision)
                && workspace.state == WorkspaceState::Published),
        "workspace revision changed"
    );
    let commit = workspace
        .commit_hash
        .clone()
        .ok_or_else(|| eyre!("workspace has no commit to publish"))?;
    job = mark_workspace_job_running(app, id, job, "checking_remote_baseline").await?;
    let _serial = app.workspace_serial.lock().await;
    let mirror = ensure_mirror(app, &params.repository, repository).await?;
    fetch(app, &mirror, repository).await?;
    let observed = resolve_remote_head(app, &mirror, &params.reference).await?;
    if observed.eq_ignore_ascii_case(&commit) {
        let published = mark_published_if_needed(app, &workspace, &commit).await?;
        finish_publish_success(app, id, job, &params, published, observed, true).await?;
        return Ok(());
    }
    if !observed.eq_ignore_ascii_case(&params.expected_remote_head) {
        app.store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Failed,
                &json!({"phase":"remote_baseline","reason":"baseline_changed"}),
                Some(&json!({
                    "error":"baseline_changed",
                    "expected_remote_head":params.expected_remote_head,
                    "observed_remote_head":observed,
                    "reference":params.reference,
                    "observed_at":maxops_proto::now(),
                })),
            )
            .await?;
        app.store
            .release_resource("repository_publish", &resource, id)
            .await?;
        return Ok(());
    }
    ensure!(
        git_success(
            app,
            Some(&mirror),
            None,
            [
                "merge-base",
                "--is-ancestor",
                &params.expected_remote_head,
                &commit
            ],
        )
        .await?,
        "workspace commit is not a fast-forward of the expected remote head"
    );
    if job.handle.state == JobState::Running {
        job = app
            .store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Reconciling,
                &json!({"phase":"push_may_start"}),
                Some(&json!({
                    "commit":commit,
                    "expected_remote_head":params.expected_remote_head,
                    "reference":params.reference,
                })),
            )
            .await?;
    }
    let refspec = format!("{commit}:{}", params.reference);
    let pushed = git_success(app, Some(&mirror), None, ["push", "origin", &refspec]).await?;
    fetch(app, &mirror, repository).await?;
    let after = resolve_remote_head(app, &mirror, &params.reference).await?;
    if after.eq_ignore_ascii_case(&commit) {
        let latest = app
            .store
            .get_owned_workspace(&job.principal, &params.repository, &params.workspace_id)
            .await?;
        let published = mark_published_if_needed(app, &latest, &commit).await?;
        finish_publish_success(app, id, job, &params, published, after, false).await?;
    } else {
        let state = if pushed {
            JobState::OutcomeUnknown
        } else {
            JobState::Failed
        };
        app.store
            .transition_job(
                id,
                job.handle.revision,
                state,
                &json!({"phase":"publish_verification"}),
                Some(&json!({
                    "error":if pushed {"publish_superseded_or_unconfirmed"} else {"baseline_changed"},
                    "expected_remote_head":params.expected_remote_head,
                    "intended_commit":commit,
                    "observed_remote_head":after,
                    "effect":if pushed {"unknown"} else {"not_applied"},
                })),
            )
            .await?;
        app.store
            .release_resource("repository_publish", &resource, id)
            .await?;
    }
    Ok(())
}

async fn mark_workspace_job_running(
    app: &App,
    id: &JobId,
    mut job: JobRecord,
    phase: &str,
) -> Result<JobRecord> {
    if job.handle.state == JobState::Queued {
        job = app
            .store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Dispatching,
                &json!({"launcher":"workspace_worker"}),
                None,
            )
            .await?;
    }
    if job.handle.state == JobState::Dispatching {
        job = app
            .store
            .transition_job(
                id,
                job.handle.revision,
                JobState::Running,
                &json!({"phase":phase}),
                None,
            )
            .await?;
    }
    Ok(job)
}

async fn mark_published_if_needed(
    app: &App,
    workspace: &WorkspaceRecord,
    commit: &str,
) -> Result<WorkspaceRecord> {
    if workspace.state == WorkspaceState::Published {
        return Ok(workspace.clone());
    }
    app.store
        .transition_workspace(
            &workspace.workspace_id,
            &workspace.creator,
            workspace.revision,
            &workspace.tree_hash,
            Some(commit),
            WorkspaceState::Published,
        )
        .await
}

async fn finish_publish_success(
    app: &App,
    id: &JobId,
    job: JobRecord,
    params: &WorkspacePublishParams,
    workspace: WorkspaceRecord,
    observed: String,
    recovered: bool,
) -> Result<()> {
    app.store
        .transition_job(
            id,
            job.handle.revision,
            JobState::Succeeded,
            &json!({"phase":"publish_verified"}),
            Some(&json!({
                "workspace":workspace,
                "reference":params.reference,
                "observed_remote_head":observed,
                "observed_at":maxops_proto::now(),
                "recovered":recovered,
            })),
        )
        .await?;
    let resource = format!("{}:{}", params.repository, params.reference);
    app.store
        .release_resource("repository_publish", &resource, id)
        .await?;
    Ok(())
}

async fn record_worker_error(app: &App, id: &JobId) -> Result<()> {
    let job = app.store.get_job(id).await?;
    if job.handle.state.is_terminal() {
        release_job_resource(app, &job).await?;
        return Ok(());
    }
    let (state, effect) = if job.handle.state == JobState::Reconciling {
        (JobState::OutcomeUnknown, "unknown")
    } else {
        (JobState::Failed, "not_applied")
    };
    app.store
        .transition_job(
            id,
            job.handle.revision,
            state,
            &json!({"phase":"workspace_worker_error"}),
            Some(&json!({"error":"workspace_worker_failed","effect":effect})),
        )
        .await?;
    release_job_resource(app, &job).await?;
    Ok(())
}

pub(super) async fn cancel(
    app: &App,
    params: maxops_proto::JobCancelParams,
) -> Result<maxops_proto::ExecutorResponse> {
    let requested = app
        .store
        .request_cancel(&params.job_id, params.expected_revision, &params.reason)
        .await?;
    if matches!(
        requested.handle.state,
        JobState::Queued | JobState::Dispatching
    ) {
        let cancelled = app
            .store
            .transition_job(
                &params.job_id,
                requested.handle.revision,
                JobState::Cancelled,
                &json!({"phase":"before_workspace_side_effect"}),
                Some(&json!({"effect":"not_started"})),
            )
            .await?;
        release_job_resource(app, &cancelled).await?;
        return Ok(maxops_proto::ExecutorResponse::Job(cancelled));
    }
    Ok(maxops_proto::ExecutorResponse::Job(requested))
}

async fn release_job_resource(app: &App, job: &JobRecord) -> Result<()> {
    match job.handle.operation.as_str() {
        "workspace.create" => {
            if let Ok(params) = serde_json::from_value::<WorkspaceCreateParams>(job.spec.clone()) {
                let _ = app
                    .store
                    .release_resource("repository_fetch", &params.repository, &job.handle.job_id)
                    .await?;
            }
        }
        "workspace.publish" => {
            if let Ok(params) = serde_json::from_value::<WorkspacePublishParams>(job.spec.clone()) {
                let resource = format!("{}:{}", params.repository, params.reference);
                let _ = app
                    .store
                    .release_resource("repository_publish", &resource, &job.handle.job_id)
                    .await?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) async fn handle(
    app: &App,
    principal: &str,
    request: WorkspaceTargetRequest,
) -> Result<WorkspaceTargetResponse> {
    let _serial = app.workspace_serial.lock().await;
    match request {
        WorkspaceTargetRequest::Status(params) => {
            params.validate().map_err(|message| eyre!(message))?;
            repository(app, &params.repository)?;
            let workspace = app
                .store
                .get_owned_workspace(principal, &params.repository, &params.workspace_id)
                .await?;
            Ok(WorkspaceTargetResponse::Record(workspace))
        }
        WorkspaceTargetRequest::Read(params) => {
            params.validate().map_err(|message| eyre!(message))?;
            let workspace = exact_workspace(app, principal, &params).await?;
            let root = revision_tree(app, &params.workspace_id, params.expected_revision);
            let path = safe_existing_file(&root, &params.path)?;
            let metadata = std::fs::metadata(&path)?;
            ensure!(
                metadata.len() <= u64::from(params.max_bytes),
                "workspace file exceeds requested limit"
            );
            let bytes = tokio::fs::read(path).await?;
            let content =
                String::from_utf8(bytes.clone()).wrap_err("workspace file is not UTF-8")?;
            Ok(WorkspaceTargetResponse::File(WorkspaceFile {
                workspace_id: workspace.workspace_id,
                revision: workspace.revision,
                path: params.path,
                encoding: "utf-8".into(),
                content,
                digest: blake3::hash(&bytes).to_hex().to_string(),
            }))
        }
        WorkspaceTargetRequest::Apply(params) => {
            params.validate().map_err(|message| eyre!(message))?;
            let workspace = exact_workspace(app, principal, &params).await?;
            let next = workspace
                .revision
                .checked_add(1)
                .ok_or_else(|| eyre!("workspace revision is exhausted"))?;
            copy_revision(app, &workspace.workspace_id, workspace.revision, next)
                .wrap_err("copy workspace revision before apply")?;
            let next_root = revision_tree(app, &workspace.workspace_id, next);
            let applied = apply_edits(&next_root, &params);
            let result = match applied {
                Ok(()) => {
                    let mirror = mirror_path(app, &params.repository);
                    git_status_with_revision(
                        app,
                        &mirror,
                        &params.workspace_id,
                        next,
                        ["add", "-A", "--", "."],
                    )
                    .await
                    .wrap_err("Git add failed for workspace revision")?;
                    let tree_hash = git_text_with_revision(
                        app,
                        &mirror,
                        &params.workspace_id,
                        next,
                        ["write-tree"],
                    )
                    .await
                    .wrap_err("Git write-tree failed for workspace revision")?;
                    app.store
                        .transition_workspace(
                            &workspace.workspace_id,
                            principal,
                            workspace.revision,
                            &tree_hash,
                            workspace.commit_hash.as_deref(),
                            WorkspaceState::Dirty,
                        )
                        .await
                }
                Err(error) => Err(error),
            };
            if result.is_err() {
                let _ =
                    std::fs::remove_dir_all(revision_directory(app, &workspace.workspace_id, next));
            }
            Ok(WorkspaceTargetResponse::Record(result?))
        }
        WorkspaceTargetRequest::Diff(params) => {
            params.validate().map_err(|message| eyre!(message))?;
            let workspace = exact_workspace(app, principal, &params).await?;
            let against = workspace
                .commit_hash
                .clone()
                .unwrap_or_else(|| workspace.base_commit.clone());
            let mirror = mirror_path(app, &params.repository);
            let output = git_output_with_revision(
                app,
                &mirror,
                &params.workspace_id,
                params.expected_revision,
                [
                    "diff",
                    "--cached",
                    "--no-ext-diff",
                    "--binary",
                    &against,
                    "--",
                ],
            )
            .await?;
            ensure!(
                output.len() <= MAX_GIT_OUTPUT,
                "workspace diff exceeds 1 MiB"
            );
            Ok(WorkspaceTargetResponse::Diff(WorkspaceDiff {
                workspace_id: workspace.workspace_id,
                revision: workspace.revision,
                against_commit: against,
                patch: String::from_utf8(output).wrap_err("Git diff is not UTF-8")?,
            }))
        }
        WorkspaceTargetRequest::Commit(params) => {
            params.validate().map_err(|message| eyre!(message))?;
            let workspace = exact_workspace(app, principal, &params).await?;
            let repository = repository(app, &params.repository)?;
            let parent = workspace
                .commit_hash
                .as_deref()
                .unwrap_or(&workspace.base_commit);
            let mirror = mirror_path(app, &params.repository);
            let commit = git_commit_tree(
                app,
                &mirror,
                &workspace.tree_hash,
                parent,
                &params.message,
                repository,
            )
            .await?;
            let next = workspace
                .revision
                .checked_add(1)
                .ok_or_else(|| eyre!("workspace revision is exhausted"))?;
            copy_revision(app, &workspace.workspace_id, workspace.revision, next)
                .wrap_err("copy workspace revision before commit")?;
            let result = app
                .store
                .transition_workspace(
                    &workspace.workspace_id,
                    principal,
                    workspace.revision,
                    &workspace.tree_hash,
                    Some(&commit),
                    WorkspaceState::Committed,
                )
                .await;
            if result.is_err() {
                let _ =
                    std::fs::remove_dir_all(revision_directory(app, &workspace.workspace_id, next));
            }
            Ok(WorkspaceTargetResponse::Record(result?))
        }
    }
}

pub(super) async fn check_exec_params(
    app: &App,
    principal: &str,
    spec: &serde_json::Value,
) -> Result<maxops_proto::ExecRunParams> {
    let params: WorkspaceCheckParams = serde_json::from_value(spec.clone())?;
    params.validate().map_err(|message| eyre!(message))?;
    let repository = repository(app, &params.repository)?;
    let workspace = app
        .store
        .get_owned_workspace(principal, &params.repository, &params.workspace_id)
        .await?;
    ensure!(
        workspace.revision == params.expected_revision,
        "workspace revision changed"
    );
    let argv = repository
        .checks
        .get(&params.check)
        .cloned()
        .ok_or_else(|| eyre!("workspace check is not configured"))?;
    let cwd = revision_tree(app, &params.workspace_id, params.expected_revision);
    let canonical = cwd.canonicalize()?;
    ensure!(
        canonical.starts_with(app.config.workspace_root.canonicalize()?),
        "workspace check path escaped workspace root"
    );
    Ok(maxops_proto::ExecRunParams {
        host: app.config.host.clone(),
        profile: repository.check_profile.clone(),
        command: maxops_proto::CommandSpec::Argv(argv),
        cwd: Some(canonical.to_string_lossy().into_owned()),
        env: Default::default(),
        credential_refs: Vec::new(),
        timeout_seconds: None,
    })
}

async fn exact_workspace<P: WorkspaceRevision>(
    app: &App,
    principal: &str,
    params: &P,
) -> Result<WorkspaceRecord> {
    repository(app, params.repository())?;
    let workspace = app
        .store
        .get_owned_workspace(principal, params.repository(), params.workspace_id())
        .await?;
    ensure!(
        workspace.revision == params.expected_revision(),
        "workspace revision changed"
    );
    ensure!(
        revision_directory(app, params.workspace_id(), params.expected_revision()).is_dir(),
        "workspace revision data is unavailable"
    );
    Ok(workspace)
}

trait WorkspaceRevision {
    fn repository(&self) -> &str;
    fn workspace_id(&self) -> &WorkspaceId;
    fn expected_revision(&self) -> u64;
}

macro_rules! workspace_revision {
    ($($type:ty),* $(,)?) => {$(
        impl WorkspaceRevision for $type {
            fn repository(&self) -> &str { &self.repository }
            fn workspace_id(&self) -> &WorkspaceId { &self.workspace_id }
            fn expected_revision(&self) -> u64 { self.expected_revision }
        }
    )*};
}

workspace_revision!(
    WorkspaceReadParams,
    WorkspaceApplyParams,
    WorkspaceRevisionParams,
    WorkspaceCommitParams,
);

fn repository<'a>(app: &'a App, name: &str) -> Result<&'a RepositoryConfig> {
    app.config
        .repositories
        .get(name)
        .ok_or_else(|| eyre!("repository is not configured"))
}

fn mirror_path(app: &App, repository: &str) -> PathBuf {
    app.config.repository_root.join(format!("{repository}.git"))
}

fn workspace_directory(app: &App, workspace: &WorkspaceId) -> PathBuf {
    app.config.workspace_root.join(workspace.as_str())
}

fn revision_directory(app: &App, workspace: &WorkspaceId, revision: u64) -> PathBuf {
    workspace_directory(app, workspace)
        .join("revisions")
        .join(revision.to_string())
}

fn revision_tree(app: &App, workspace: &WorkspaceId, revision: u64) -> PathBuf {
    revision_directory(app, workspace, revision).join("tree")
}

fn revision_index(app: &App, workspace: &WorkspaceId, revision: u64) -> PathBuf {
    revision_directory(app, workspace, revision).join("index")
}

async fn ensure_mirror(app: &App, name: &str, repository: &RepositoryConfig) -> Result<PathBuf> {
    let mirror = mirror_path(app, name);
    if !mirror.exists() {
        let temporary = app.config.repository_root.join(format!(".{name}.tmp"));
        let _ = std::fs::remove_dir_all(&temporary);
        git_status(
            app,
            None,
            None,
            [
                "init",
                "--bare",
                temporary
                    .to_str()
                    .ok_or_else(|| eyre!("repository path is not UTF-8"))?,
            ],
        )
        .await?;
        git_status(
            app,
            Some(&temporary),
            None,
            ["remote", "add", "origin", &repository.url],
        )
        .await?;
        std::fs::rename(temporary, &mirror)?;
    }
    git_status(
        app,
        Some(&mirror),
        None,
        ["remote", "set-url", "origin", &repository.url],
    )
    .await?;
    Ok(mirror)
}

async fn fetch(app: &App, mirror: &Path, repository: &RepositoryConfig) -> Result<()> {
    let refspec = "+refs/heads/*:refs/remotes/origin/*";
    git_status(
        app,
        Some(mirror),
        None,
        ["fetch", "--no-tags", "--prune", "origin", refspec],
    )
    .await?;
    ensure!(
        repository.default_ref.starts_with("refs/heads/"),
        "default ref must be a branch"
    );
    Ok(())
}

async fn resolve_remote_head(app: &App, mirror: &Path, reference: &str) -> Result<String> {
    let branch = reference
        .strip_prefix("refs/heads/")
        .ok_or_else(|| eyre!("configured ref is not a branch"))?;
    let tracking = format!("refs/remotes/origin/{branch}^{{commit}}");
    git_text(
        app,
        Some(mirror),
        None,
        ["rev-parse", "--verify", &tracking],
    )
    .await
}

async fn materialize_revision(
    app: &App,
    mirror: &Path,
    workspace: &WorkspaceId,
    revision: u64,
    commit: &str,
) -> Result<()> {
    let directory = revision_directory(app, workspace, revision);
    if directory.is_dir() {
        return Ok(());
    }
    let parent = directory
        .parent()
        .ok_or_else(|| eyre!("workspace revision has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".{revision}.tmp"));
    let _ = std::fs::remove_dir_all(&temporary);
    std::fs::create_dir(&temporary)?;
    let tree = temporary.join("tree");
    std::fs::create_dir(&tree)?;
    let index = temporary.join("index");
    git_status(app, Some(mirror), Some(&index), ["read-tree", commit])
        .await
        .wrap_err("Git read-tree failed while materializing workspace")?;
    let mut prefix = tree.clone().into_os_string();
    prefix.push("/");
    git_status_os(
        app,
        Some(mirror),
        Some(&index),
        vec![
            OsString::from("--work-tree"),
            tree.into_os_string(),
            OsString::from("checkout-index"),
            OsString::from("--all"),
            OsString::from("--force"),
            OsString::from("--prefix"),
            prefix,
        ],
    )
    .await
    .wrap_err("Git checkout-index failed while materializing workspace")?;
    std::fs::rename(temporary, directory)?;
    Ok(())
}

fn copy_revision(app: &App, workspace: &WorkspaceId, from: u64, to: u64) -> Result<()> {
    let source = revision_directory(app, workspace, from);
    let destination = revision_directory(app, workspace, to);
    ensure!(source.is_dir(), "workspace source revision is unavailable");
    // The database revision is the commit point. A directory at `to` while the
    // database still names `from` is an orphan left by an interrupted apply or
    // commit, so retrying may replace only that unpublished directory.
    if destination.exists() {
        std::fs::remove_dir_all(&destination)
            .wrap_err("remove orphan workspace revision directory")?;
    }
    let parent = destination
        .parent()
        .ok_or_else(|| eyre!("workspace revision has no parent"))?;
    std::fs::create_dir_all(parent).wrap_err("create workspace revision parent")?;
    let temporary = parent.join(format!(".{to}.tmp"));
    let _ = std::fs::remove_dir_all(&temporary);
    copy_path(&source, &temporary).wrap_err("copy immutable workspace revision")?;
    std::fs::rename(temporary, destination).wrap_err("publish workspace revision directory")?;
    Ok(())
}

fn copy_path(source: &Path, destination: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source)?;
    if metadata.is_dir() {
        std::fs::create_dir(destination).wrap_err("create copied workspace directory")?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            copy_path(&entry.path(), &destination.join(entry.file_name()))?;
        }
    } else if metadata.is_file() {
        std::fs::copy(source, destination).wrap_err("copy workspace regular file")?;
    } else if metadata.file_type().is_symlink() {
        symlink(
            std::fs::read_link(source).wrap_err("read workspace symbolic link")?,
            destination,
        )
        .wrap_err("copy workspace symbolic link")?;
    } else {
        return Err(eyre!("workspace contains an unsupported special file"));
    }
    Ok(())
}

fn apply_edits(root: &Path, params: &WorkspaceApplyParams) -> Result<()> {
    let mut temporary_paths = HashSet::new();
    for edit in &params.edits {
        let path = safe_target(root, &edit.path, edit.content.is_some())?;
        if let Some(content) = &edit.content {
            let parent = path
                .parent()
                .ok_or_else(|| eyre!("workspace path has no parent"))?;
            let temporary = parent.join(format!(".maxops-{}.tmp", uuid::Uuid::now_v7()));
            ensure!(
                temporary_paths.insert(temporary.clone()),
                "temporary path collision"
            );
            let permissions = std::fs::symlink_metadata(&path)
                .ok()
                .filter(|metadata| metadata.is_file())
                .map(|metadata| metadata.permissions())
                .unwrap_or_else(|| std::fs::Permissions::from_mode(0o644));
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .wrap_err("create temporary workspace edit")?;
            std::io::Write::write_all(&mut file, content.as_bytes())
                .wrap_err("write temporary workspace edit")?;
            file.sync_all().wrap_err("sync temporary workspace edit")?;
            std::fs::set_permissions(&temporary, permissions)
                .wrap_err("set workspace edit permissions")?;
            std::fs::rename(&temporary, &path).wrap_err("publish workspace file edit")?;
            std::fs::File::open(parent)
                .wrap_err("open workspace edit parent for sync")?
                .sync_all()
                .wrap_err("sync workspace edit parent")?;
        } else {
            let metadata = std::fs::symlink_metadata(&path)
                .wrap_err("workspace delete target does not exist")?;
            ensure!(
                metadata.is_file(),
                "workspace delete target is not a regular file"
            );
            std::fs::remove_file(path)?;
        }
    }
    Ok(())
}

fn safe_existing_file(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = safe_target(root, relative, false)?;
    let metadata = std::fs::symlink_metadata(&path)?;
    ensure!(metadata.is_file(), "workspace path is not a regular file");
    Ok(path)
}

fn safe_target(root: &Path, relative: &str, create_parents: bool) -> Result<PathBuf> {
    let relative = Path::new(relative);
    ensure!(!relative.is_absolute(), "workspace path must be relative");
    let components: Vec<_> = relative.components().collect();
    ensure!(!components.is_empty(), "workspace path must not be empty");
    ensure!(
        components
            .iter()
            .all(|component| matches!(component, Component::Normal(_))),
        "workspace path contains traversal or platform components"
    );
    ensure!(
        !components
            .iter()
            .any(|component| component.as_os_str() == ".git"),
        "workspace path may not modify Git administrative names"
    );
    let mut current = root.to_path_buf();
    for component in &components[..components.len() - 1] {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) => ensure!(metadata.is_dir(), "workspace ancestor is not a directory"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && create_parents => {
                std::fs::create_dir(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    current.push(components.last().expect("nonempty").as_os_str());
    if let Ok(metadata) = std::fs::symlink_metadata(&current) {
        ensure!(metadata.is_file(), "workspace target is not a regular file");
    }
    Ok(current)
}

async fn git_commit_tree(
    app: &App,
    mirror: &Path,
    tree: &str,
    parent: &str,
    message: &str,
    repository: &RepositoryConfig,
) -> Result<String> {
    let mut command = git_command(app, Some(mirror), None);
    command
        .args(["commit-tree", tree, "-p", parent])
        .env("GIT_AUTHOR_NAME", &repository.author_name)
        .env("GIT_AUTHOR_EMAIL", &repository.author_email)
        .env("GIT_COMMITTER_NAME", &repository.author_name)
        .env("GIT_COMMITTER_EMAIL", &repository.author_email)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| eyre!("Git commit stdin unavailable"))?
        .write_all(message.as_bytes())
        .await?;
    let output = child.wait_with_output().await?;
    ensure!(output.status.success(), "Git commit-tree failed");
    ensure!(
        output.stdout.len() <= 128,
        "Git commit identity is oversized"
    );
    let commit = String::from_utf8(output.stdout)?.trim().to_owned();
    ensure!(
        maxops_proto::valid_git_oid(&commit),
        "Git returned an invalid commit"
    );
    Ok(commit)
}

fn git_command(app: &App, git_dir: Option<&Path>, index: Option<&Path>) -> Command {
    let mut command = Command::new(&app.config.git);
    command
        .env_clear()
        .env("LC_ALL", "C.UTF-8")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .arg("-c")
        .arg("core.hooksPath=/dev/null");
    if let Some(git_dir) = git_dir {
        command.arg("--git-dir").arg(git_dir);
    }
    if let Some(index) = index {
        command.env("GIT_INDEX_FILE", index);
    }
    command
}

async fn git_status<'a>(
    app: &App,
    git_dir: Option<&Path>,
    index: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    ensure!(
        git_success(app, git_dir, index, args).await?,
        "Git command failed"
    );
    Ok(())
}

async fn git_success<'a>(
    app: &App,
    git_dir: Option<&Path>,
    index: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<bool> {
    let status = git_command(app, git_dir, index)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    Ok(status.success())
}

async fn git_status_os(
    app: &App,
    git_dir: Option<&Path>,
    index: Option<&Path>,
    args: Vec<OsString>,
) -> Result<()> {
    let status = git_command(app, git_dir, index)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    ensure!(status.success(), "Git command failed");
    Ok(())
}

async fn git_text<'a>(
    app: &App,
    git_dir: Option<&Path>,
    index: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<String> {
    let output = git_output(app, git_dir, index, args).await?;
    let text = String::from_utf8(output)?.trim().to_owned();
    ensure!(!text.is_empty(), "Git returned empty output");
    Ok(text)
}

async fn git_output<'a>(
    app: &App,
    git_dir: Option<&Path>,
    index: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<u8>> {
    let output = git_command(app, git_dir, index)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await?;
    ensure!(output.status.success(), "Git command failed");
    ensure!(
        output.stdout.len() <= MAX_GIT_OUTPUT,
        "Git output exceeds 1 MiB"
    );
    Ok(output.stdout)
}

async fn git_status_with_revision<'a>(
    app: &App,
    mirror: &Path,
    workspace: &WorkspaceId,
    revision: u64,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    let tree = revision_tree(app, workspace, revision);
    let index = revision_index(app, workspace, revision);
    let mut command = git_command(app, Some(mirror), Some(&index));
    let status = command
        .arg("--work-tree")
        .arg(tree)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    ensure!(status.success(), "Git workspace command failed");
    Ok(())
}

async fn git_text_with_revision<'a>(
    app: &App,
    mirror: &Path,
    workspace: &WorkspaceId,
    revision: u64,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<String> {
    let output = git_output_with_revision(app, mirror, workspace, revision, args).await?;
    let text = String::from_utf8(output)?.trim().to_owned();
    ensure!(!text.is_empty(), "Git returned empty output");
    Ok(text)
}

async fn git_output_with_revision<'a>(
    app: &App,
    mirror: &Path,
    workspace: &WorkspaceId,
    revision: u64,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<u8>> {
    let tree = revision_tree(app, workspace, revision);
    let index = revision_index(app, workspace, revision);
    let output = git_command(app, Some(mirror), Some(&index))
        .arg("--work-tree")
        .arg(tree)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await?;
    ensure!(output.status.success(), "Git workspace command failed");
    ensure!(
        output.stdout.len() <= MAX_GIT_OUTPUT,
        "Git output exceeds 1 MiB"
    );
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn workspace_paths_reject_escape_symlinks_and_git_metadata() {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("tree");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("regular.txt"), "safe").unwrap();
        symlink("/etc/shadow", root.join("escape")).unwrap();
        symlink("/tmp", root.join("escape-dir")).unwrap();

        assert!(safe_existing_file(&root, "regular.txt").is_ok());
        assert!(safe_existing_file(&root, "../outside").is_err());
        assert!(safe_existing_file(&root, "/etc/shadow").is_err());
        assert!(safe_existing_file(&root, "escape").is_err());
        assert!(safe_target(&root, "escape-dir/new", true).is_err());
        assert!(safe_target(&root, ".git/config", true).is_err());
    }

    #[test]
    fn workspace_apply_replaces_regular_files_without_following_links() {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("tree");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("config.txt"), "before\n").unwrap();
        let outside = temporary.path().join("outside.txt");
        std::fs::write(&outside, "outside\n").unwrap();
        symlink(&outside, root.join("linked.txt")).unwrap();
        let workspace_id = WorkspaceId::parse("019d0000-0000-7000-8000-000000000001").unwrap();

        apply_edits(
            &root,
            &WorkspaceApplyParams {
                repository: "fixture".into(),
                workspace_id: workspace_id.clone(),
                expected_revision: 1,
                edits: vec![maxops_proto::WorkspaceEdit {
                    path: "config.txt".into(),
                    content: Some("after\n".into()),
                }],
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("config.txt")).unwrap(),
            "after\n"
        );

        let escaped = apply_edits(
            &root,
            &WorkspaceApplyParams {
                repository: "fixture".into(),
                workspace_id,
                expected_revision: 1,
                edits: vec![maxops_proto::WorkspaceEdit {
                    path: "linked.txt".into(),
                    content: Some("changed\n".into()),
                }],
            },
        );
        assert!(escaped.is_err());
        assert_eq!(std::fs::read_to_string(outside).unwrap(), "outside\n");
    }
}
