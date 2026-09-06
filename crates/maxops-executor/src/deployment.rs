use super::App;
use color_eyre::eyre::{Result, ensure, eyre};
use maxops_proto::{
    CommandSpec, DeploymentAction, DeploymentJobSpec, DeploymentRunnerSpec, ExecRunParams, JobId,
    NewJob, RuntimeStateRequest, RuntimeStateResponse, WorkspaceState,
};
use std::{collections::BTreeMap, path::PathBuf};
use tokio::io::AsyncWriteExt;

pub(super) fn is_deployment_job(operation: &str) -> bool {
    matches!(
        operation,
        "deploy.prepare" | "deploy.build" | "deploy.activate" | "deploy.verify" | "deploy.rollback"
    )
}

pub(super) fn serializes_host_changes(operation: &str) -> bool {
    matches!(
        operation,
        "deploy.activate" | "deploy.verify" | "deploy.rollback"
    )
}

pub(super) fn observe_runtime(
    app: &App,
    principal: &str,
    request: RuntimeStateRequest,
) -> Result<RuntimeStateResponse> {
    ensure!(!principal.is_empty(), "empty runtime observer identity");
    let profile = app
        .config
        .deployment_profiles
        .get(&request.deployment_profile)
        .ok_or_else(|| eyre!("deployment profile is not configured"))?;
    ensure!(
        profile.target_host == app.config.host,
        "deployment profile targets another host"
    );
    let running_closure = std::fs::read_link(&profile.running_link)
        .ok()
        .map(|path| path.to_string_lossy().into_owned());
    let profile_link = std::fs::read_link(&profile.profile_path).ok();
    let persistent_profile = std::fs::canonicalize(&profile.profile_path)
        .ok()
        .map(|path| path.to_string_lossy().into_owned());
    let generation = profile_link
        .as_deref()
        .and_then(|path| path.file_name())
        .and_then(|name| name.to_str())
        .and_then(|name| name.rsplit_once("-link"))
        .and_then(|(prefix, _)| prefix.rsplit_once('-'))
        .and_then(|(_, generation)| generation.parse().ok());
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    Ok(RuntimeStateResponse {
        host: app.config.host.clone(),
        deployment_profile: request.deployment_profile,
        running_closure,
        persistent_profile,
        generation,
        boot_id,
        observed_at: maxops_proto::now(),
    })
}

pub(super) async fn validate_job(app: &App, job: &NewJob) -> Result<DeploymentJobSpec> {
    let spec: DeploymentJobSpec = serde_json::from_value(job.spec.clone())?;
    ensure!(
        operation_for(spec.action) == job.operation,
        "deployment action does not match operation"
    );
    ensure!(
        spec.plan.expires_at.as_second() >= maxops_proto::now().as_second(),
        "deployment plan expired"
    );
    let configured = app
        .config
        .deployment_profiles
        .get(&spec.plan.deployment_profile)
        .ok_or_else(|| eyre!("deployment profile is not configured"))?;
    ensure!(
        configured.repository == spec.plan.repository,
        "deployment repository changed"
    );
    ensure!(
        configured.target_host == spec.plan.target_host,
        "deployment target changed"
    );
    ensure!(configured.kind == spec.plan.kind, "deployment kind changed");
    ensure!(
        configured.flake_attribute == spec.plan.flake_attribute,
        "deployment flake attribute changed"
    );
    match spec.action {
        DeploymentAction::Prepare | DeploymentAction::Build => {
            ensure!(
                app.config.repositories.contains_key(&spec.plan.repository),
                "deployment repository is not configured on builder"
            );
            ensure!(
                job.host == app.config.host,
                "deployment build targets another executor"
            );
            let workspace = app
                .store
                .get_owned_workspace(
                    &job.principal,
                    &spec.plan.repository,
                    &spec.plan.workspace_id,
                )
                .await?;
            ensure!(
                workspace.executor == app.config.host,
                "workspace belongs to another executor"
            );
            ensure!(
                workspace.revision == spec.plan.workspace_revision,
                "workspace revision changed"
            );
            ensure!(
                workspace.tree_hash == spec.plan.tree_hash,
                "workspace tree changed"
            );
            ensure!(
                workspace.commit_hash.as_deref() == Some(spec.plan.source_commit.as_str()),
                "workspace commit changed"
            );
            ensure!(
                matches!(
                    workspace.state,
                    WorkspaceState::Committed | WorkspaceState::Published
                ),
                "workspace must be committed before deployment"
            );
            ensure!(
                revision_tree(app, &spec).is_dir(),
                "immutable workspace revision is unavailable"
            );
        }
        DeploymentAction::Activate | DeploymentAction::Verify | DeploymentAction::Rollback => {
            ensure!(
                job.host == app.config.host && app.config.host == spec.plan.target_host,
                "deployment stage targets another host"
            );
            let artifact = spec
                .artifact
                .as_ref()
                .ok_or_else(|| eyre!("deployment artifact is required"))?;
            ensure!(
                artifact.source_commit == spec.plan.source_commit,
                "artifact source changed"
            );
            ensure!(
                artifact.tree_hash == spec.plan.tree_hash,
                "artifact tree changed"
            );
            ensure!(
                spec.plan.lock_digest.as_deref() == Some(artifact.lock_digest.as_str())
                    && spec.plan.drv_path.as_deref() == Some(artifact.drv_path.as_str()),
                "artifact does not match prepared plan"
            );
        }
    }
    Ok(spec)
}

pub(super) async fn runner_params(
    app: &App,
    principal: &str,
    spec_value: &serde_json::Value,
) -> Result<ExecRunParams> {
    let job = NewJob {
        principal: principal.to_owned(),
        host: app.config.host.clone(),
        operation: spec_value
            .get("action")
            .and_then(serde_json::Value::as_str)
            .and_then(operation_from_action)
            .ok_or_else(|| eyre!("invalid deployment action"))?
            .to_owned(),
        spec_version: 1,
        spec: spec_value.clone(),
        policy_version: String::new(),
        deadline: None,
    };
    let spec = validate_job(app, &job).await?;
    let configured = &app.config.deployment_profiles[&spec.plan.deployment_profile];
    let profile = match spec.action {
        DeploymentAction::Prepare | DeploymentAction::Build => &configured.build_profile,
        DeploymentAction::Activate | DeploymentAction::Rollback => &configured.activate_profile,
        DeploymentAction::Verify => &configured.verify_profile,
    };
    Ok(ExecRunParams {
        host: app.config.host.clone(),
        profile: profile.clone(),
        command: CommandSpec::Argv(vec![
            app.config.deploy_runner.to_string_lossy().into_owned(),
            "--credential".into(),
            "deployment".into(),
        ]),
        cwd: matches!(
            spec.action,
            DeploymentAction::Prepare | DeploymentAction::Build
        )
        .then(|| revision_tree(app, &spec).to_string_lossy().into_owned()),
        env: BTreeMap::new(),
        credential_refs: Vec::new(),
        timeout_seconds: None,
    })
}

pub(super) async fn persist_spec(
    app: &App,
    id: &JobId,
    spec_value: &serde_json::Value,
) -> Result<()> {
    let spec: DeploymentJobSpec = serde_json::from_value(spec_value.clone())?;
    let configured = app
        .config
        .deployment_profiles
        .get(&spec.plan.deployment_profile)
        .ok_or_else(|| eyre!("deployment profile is not configured"))?;
    let runner = DeploymentRunnerSpec {
        job: spec,
        nix: app.config.nix.to_string_lossy().into_owned(),
        nix_env: app.config.nix_env.to_string_lossy().into_owned(),
        profile_path: configured.profile_path.to_string_lossy().into_owned(),
        running_link: configured.running_link.to_string_lossy().into_owned(),
        activation_program: configured.activation_program.clone(),
        activation_arguments: configured.activation_arguments.clone(),
        artifact_source: configured.artifact_source.clone(),
        verify_commands: configured.verify_commands.clone(),
        verify_attempts: configured.verify_attempts,
        verify_interval_seconds: configured.verify_interval_seconds,
        automatic_rollback: configured.automatic_rollback,
    };
    let path = spec_path(app, id);
    let temporary = path.with_extension("json.tmp");
    let mut file = tokio::fs::File::create(&temporary).await?;
    file.write_all(&serde_json::to_vec(&runner)?).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

pub(super) fn spec_path(app: &App, id: &JobId) -> PathBuf {
    app.config
        .spec_directory
        .join(format!("{}.deployment.json", id.as_str()))
}

fn revision_tree(app: &App, spec: &DeploymentJobSpec) -> PathBuf {
    app.config
        .workspace_root
        .join(spec.plan.workspace_id.as_str())
        .join("revisions")
        .join(spec.plan.workspace_revision.to_string())
        .join("tree")
}

fn operation_for(action: DeploymentAction) -> &'static str {
    match action {
        DeploymentAction::Prepare => "deploy.prepare",
        DeploymentAction::Build => "deploy.build",
        DeploymentAction::Activate => "deploy.activate",
        DeploymentAction::Verify => "deploy.verify",
        DeploymentAction::Rollback => "deploy.rollback",
    }
}

fn operation_from_action(action: &str) -> Option<&'static str> {
    match action {
        "prepare" => Some("deploy.prepare"),
        "build" => Some("deploy.build"),
        "activate" => Some("deploy.activate"),
        "verify" => Some("deploy.verify"),
        "rollback" => Some("deploy.rollback"),
        _ => None,
    }
}
