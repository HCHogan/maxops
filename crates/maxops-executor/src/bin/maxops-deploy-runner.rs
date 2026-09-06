use clap::Parser;
use color_eyre::eyre::{Context, Result, ensure, eyre};
use maxops_proto::{
    DeploymentAction, DeploymentArtifact, DeploymentReport, DeploymentReportStatus,
    DeploymentRunnerSpec,
};
use std::{
    path::{Path, PathBuf},
    process::{ExitCode, Stdio},
    time::Duration,
};
use tokio::process::Command;

#[derive(Parser)]
#[command(version, about = "Target-local maxops Nix deployment stage runner")]
struct Args {
    #[arg(long, default_value = "deployment")]
    credential: String,
}

#[tokio::main]
async fn main() -> Result<ExitCode> {
    color_eyre::install()?;
    let args = Args::parse();
    let directory = std::env::var_os("CREDENTIALS_DIRECTORY")
        .ok_or_else(|| eyre!("credential directory is unavailable"))?;
    let path = PathBuf::from(directory).join(args.credential);
    let spec: DeploymentRunnerSpec = serde_json::from_slice(&tokio::fs::read(path).await?)?;
    validate(&spec)?;
    let (report, code) = match spec.job.action {
        DeploymentAction::Prepare => (prepare(&spec).await?, 0),
        DeploymentAction::Build => (build(&spec).await?, 0),
        DeploymentAction::Activate => activate(&spec).await?,
        DeploymentAction::Verify => verify(&spec).await?,
        DeploymentAction::Rollback => rollback(&spec).await?,
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(ExitCode::from(code))
}

fn validate(spec: &DeploymentRunnerSpec) -> Result<()> {
    for path in [
        &spec.nix,
        &spec.nix_env,
        &spec.profile_path,
        &spec.running_link,
    ] {
        ensure!(
            Path::new(path).is_absolute(),
            "deployment paths must be absolute"
        );
    }
    let activation = Path::new(&spec.activation_program);
    ensure!(
        !activation.is_absolute()
            && activation
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_))),
        "activation program must be a relative path"
    );
    ensure!(spec.verify_attempts > 0, "verify attempts must be positive");
    ensure!(
        spec.verify_commands.iter().all(|argv| !argv.is_empty()),
        "verification commands must have a program"
    );
    Ok(())
}

async fn prepare(spec: &DeploymentRunnerSpec) -> Result<DeploymentReport> {
    let lock_digest = lock_digest().await?;
    let selector = format!(".#{}.drvPath", spec.job.plan.flake_attribute);
    let drv_path = command_text(&spec.nix, ["eval", "--raw", &selector]).await?;
    ensure_store_path(&drv_path)?;
    Ok(report(
        DeploymentAction::Prepare,
        DeploymentReportStatus::Prepared,
        Some(drv_path),
        None,
        Some(lock_digest),
        "prepared exact derivation and lock digest",
    ))
}

async fn build(spec: &DeploymentRunnerSpec) -> Result<DeploymentReport> {
    let lock_digest = lock_digest().await?;
    ensure!(
        spec.job.plan.lock_digest.as_deref() == Some(lock_digest.as_str()),
        "workspace lock digest changed after preparation"
    );
    let selector = format!(".#{}", spec.job.plan.flake_attribute);
    let out_path = command_text(
        &spec.nix,
        ["build", "--no-link", "--print-out-paths", &selector],
    )
    .await?;
    ensure_store_path(&out_path)?;
    let drv_path = command_text(&spec.nix, ["path-info", "--derivation", &out_path]).await?;
    ensure_store_path(&drv_path)?;
    ensure!(
        spec.job.plan.drv_path.as_deref() == Some(drv_path.as_str()),
        "built derivation differs from prepared plan"
    );
    Ok(report(
        DeploymentAction::Build,
        DeploymentReportStatus::Built,
        Some(drv_path),
        Some(out_path),
        Some(lock_digest),
        "built exact prepared derivation",
    ))
}

async fn activate(spec: &DeploymentRunnerSpec) -> Result<(DeploymentReport, u8)> {
    let artifact = artifact(spec)?;
    let (running, profile) = observe(spec);
    if !baseline_matches(spec, running.as_deref(), profile.as_deref()) {
        return Ok((
            observed_report(
                DeploymentAction::Activate,
                DeploymentReportStatus::Stale,
                running,
                profile,
                "runtime baseline changed before activation",
            ),
            20,
        ));
    }
    ensure_artifact(spec, artifact).await?;
    if let Err(error) = set_and_activate(spec, &artifact.out_path).await {
        return recover_after_failure(
            spec,
            DeploymentAction::Activate,
            &format!("activation failed: {error:#}"),
        )
        .await;
    }
    if let Err(error) = verify_intended(spec, artifact).await {
        return recover_after_failure(
            spec,
            DeploymentAction::Activate,
            &format!("activation verification failed: {error:#}"),
        )
        .await;
    }
    let (running, profile) = observe(spec);
    Ok((
        observed_report(
            DeploymentAction::Activate,
            DeploymentReportStatus::Activated,
            running,
            profile,
            "artifact activated and target-local checks passed",
        ),
        0,
    ))
}

async fn verify(spec: &DeploymentRunnerSpec) -> Result<(DeploymentReport, u8)> {
    let artifact = artifact(spec)?;
    match verify_intended(spec, artifact).await {
        Ok(()) => {
            let (running, profile) = observe(spec);
            Ok((
                observed_report(
                    DeploymentAction::Verify,
                    DeploymentReportStatus::Verified,
                    running,
                    profile,
                    "activated artifact passed target acceptance",
                ),
                0,
            ))
        }
        Err(error) => {
            recover_after_failure(
                spec,
                DeploymentAction::Verify,
                &format!("acceptance failed: {error:#}"),
            )
            .await
        }
    }
}

async fn rollback(spec: &DeploymentRunnerSpec) -> Result<(DeploymentReport, u8)> {
    recover_after_failure(spec, DeploymentAction::Rollback, "rollback requested").await
}

async fn recover_after_failure(
    spec: &DeploymentRunnerSpec,
    action: DeploymentAction,
    failure: &str,
) -> Result<(DeploymentReport, u8)> {
    let artifact = artifact(spec)?;
    let (running, profile) = observe(spec);
    if !current_is_owned(
        artifact,
        spec.job.plan.runtime_baseline.running_closure.as_deref(),
        running.as_deref(),
        profile.as_deref(),
    ) {
        return Ok((
            observed_report(
                action,
                DeploymentReportStatus::Superseded,
                running,
                profile,
                &format!("{failure}; a different runtime state now owns the host"),
            ),
            21,
        ));
    }
    if !spec.automatic_rollback && !matches!(action, DeploymentAction::Rollback) {
        return Ok((
            observed_report(
                action,
                DeploymentReportStatus::Failed,
                running,
                profile,
                &format!("{failure}; automatic rollback is disabled"),
            ),
            22,
        ));
    }
    let baseline = spec
        .job
        .plan
        .runtime_baseline
        .persistent_profile
        .as_deref()
        .or(spec.job.plan.runtime_baseline.running_closure.as_deref())
        .ok_or_else(|| eyre!("plan has no restorable runtime baseline"))?;
    if let Err(error) = set_and_activate(spec, baseline).await {
        let (running, profile) = observe(spec);
        return Ok((
            observed_report(
                action,
                DeploymentReportStatus::RecoveryFailed,
                running,
                profile,
                &format!("{failure}; recovery failed: {error:#}"),
            ),
            23,
        ));
    }
    let (running, profile) = observe(spec);
    if !restored_baseline_matches(spec, running.as_deref(), profile.as_deref()) {
        return Ok((
            observed_report(
                action,
                DeploymentReportStatus::RecoveryFailed,
                running,
                profile,
                &format!("{failure}; recovery completed but baseline was not restored"),
            ),
            23,
        ));
    }
    Ok((
        observed_report(
            action,
            DeploymentReportStatus::RolledBack,
            running,
            profile,
            &format!("{failure}; original runtime baseline restored"),
        ),
        24,
    ))
}

async fn verify_intended(spec: &DeploymentRunnerSpec, artifact: &DeploymentArtifact) -> Result<()> {
    let mut last_error = None;
    for attempt in 0..spec.verify_attempts {
        let (running, profile) = observe(spec);
        if current_is_intended(artifact, running.as_deref(), profile.as_deref()) {
            match run_verification_commands(&spec.verify_commands).await {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        } else {
            last_error = Some(eyre!("running or persistent closure differs from artifact"));
        }
        if attempt + 1 < spec.verify_attempts {
            tokio::time::sleep(Duration::from_secs(u64::from(spec.verify_interval_seconds))).await;
        }
    }
    Err(last_error.unwrap_or_else(|| eyre!("verification did not run")))
}

async fn run_verification_commands(commands: &[Vec<String>]) -> Result<()> {
    for argv in commands {
        let (program, arguments) = argv.split_first().ok_or_else(|| eyre!("empty check"))?;
        let status = Command::new(program)
            .args(arguments)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .await?;
        ensure!(status.success(), "target verification command failed");
    }
    Ok(())
}

async fn ensure_artifact(spec: &DeploymentRunnerSpec, artifact: &DeploymentArtifact) -> Result<()> {
    if Path::new(&artifact.out_path).exists() {
        return Ok(());
    }
    let source = spec
        .artifact_source
        .as_deref()
        .ok_or_else(|| eyre!("artifact is absent on target and no source is configured"))?;
    command_status(&spec.nix, ["copy", "--from", source, &artifact.out_path]).await
}

async fn set_and_activate(spec: &DeploymentRunnerSpec, out_path: &str) -> Result<()> {
    ensure_store_path(out_path)?;
    command_status(&spec.nix_env, ["-p", &spec.profile_path, "--set", out_path])
        .await
        .wrap_err("set persistent deployment profile")?;
    let program = Path::new(out_path).join(&spec.activation_program);
    let status = Command::new(&program)
        .args(&spec.activation_arguments)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .await
        .wrap_err("start activation program")?;
    ensure!(status.success(), "activation program failed");
    Ok(())
}

fn observe(spec: &DeploymentRunnerSpec) -> (Option<String>, Option<String>) {
    let running = std::fs::read_link(&spec.running_link)
        .ok()
        .map(|path| path.to_string_lossy().into_owned());
    let profile = std::fs::canonicalize(&spec.profile_path)
        .ok()
        .map(|path| path.to_string_lossy().into_owned());
    (running, profile)
}

fn baseline_matches(
    spec: &DeploymentRunnerSpec,
    running: Option<&str>,
    profile: Option<&str>,
) -> bool {
    optional_matches(
        spec.job.plan.runtime_baseline.running_closure.as_deref(),
        running,
    ) && optional_matches(
        spec.job.plan.runtime_baseline.persistent_profile.as_deref(),
        profile,
    ) && spec
        .job
        .plan
        .runtime_baseline
        .boot_id
        .as_deref()
        .is_none_or(|expected| read_boot_id().as_deref() == Some(expected))
        && spec
            .job
            .plan
            .runtime_baseline
            .generation
            .is_none_or(|expected| profile_generation(&spec.profile_path) == Some(expected))
}

fn restored_baseline_matches(
    spec: &DeploymentRunnerSpec,
    running: Option<&str>,
    profile: Option<&str>,
) -> bool {
    optional_matches(
        spec.job.plan.runtime_baseline.running_closure.as_deref(),
        running,
    ) && optional_matches(
        spec.job.plan.runtime_baseline.persistent_profile.as_deref(),
        profile,
    ) && spec
        .job
        .plan
        .runtime_baseline
        .boot_id
        .as_deref()
        .is_none_or(|expected| read_boot_id().as_deref() == Some(expected))
}

fn current_is_intended(
    artifact: &DeploymentArtifact,
    running: Option<&str>,
    profile: Option<&str>,
) -> bool {
    running == Some(artifact.out_path.as_str()) && profile == Some(artifact.out_path.as_str())
}

fn current_is_owned(
    artifact: &DeploymentArtifact,
    baseline_running: Option<&str>,
    running: Option<&str>,
    profile: Option<&str>,
) -> bool {
    profile == Some(artifact.out_path.as_str())
        && (running == Some(artifact.out_path.as_str()) || running == baseline_running)
}

fn read_boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn profile_generation(path: &str) -> Option<u64> {
    let link = std::fs::read_link(path).ok()?;
    let name = link.file_name()?.to_str()?;
    let (prefix, _) = name.rsplit_once("-link")?;
    let (_, generation) = prefix.rsplit_once('-')?;
    generation.parse().ok()
}

fn optional_matches(expected: Option<&str>, observed: Option<&str>) -> bool {
    expected.is_none_or(|expected| observed == Some(expected))
}

fn artifact(spec: &DeploymentRunnerSpec) -> Result<&DeploymentArtifact> {
    spec.job
        .artifact
        .as_ref()
        .ok_or_else(|| eyre!("deployment stage requires a built artifact"))
}

async fn lock_digest() -> Result<String> {
    let bytes = tokio::fs::read("flake.lock")
        .await
        .wrap_err("read frozen flake.lock")?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

async fn command_text<'a>(
    program: &str,
    arguments: impl IntoIterator<Item = &'a str>,
) -> Result<String> {
    let output = deployment_command(program)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    ensure_command_succeeded(output.status, &output.stderr)?;
    ensure!(
        output.stdout.len() <= 4096,
        "deployment command output is oversized"
    );
    let text = String::from_utf8(output.stdout)?.trim().to_owned();
    ensure!(
        !text.is_empty() && !text.contains('\n'),
        "invalid deployment command output"
    );
    Ok(text)
}

async fn command_status<'a>(
    program: &str,
    arguments: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    let output = deployment_command(program)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await?;
    ensure_command_succeeded(output.status, &output.stderr)
}

fn deployment_command(program: &str) -> Command {
    let home = std::env::var_os("HOME").unwrap_or_else(|| "/var/empty".into());
    let mut command = Command::new(program);
    command
        .env_clear()
        .env("HOME", home)
        .env("LC_ALL", "C.UTF-8")
        .env("NIX_CONFIG", "experimental-features = nix-command flakes")
        .env("NIX_REMOTE", "daemon");
    command
}

fn ensure_command_succeeded(status: std::process::ExitStatus, stderr: &[u8]) -> Result<()> {
    ensure!(
        stderr.len() <= 64 * 1024,
        "deployment command stderr is oversized"
    );
    if status.success() {
        return Ok(());
    }
    let detail = String::from_utf8_lossy(stderr);
    Err(eyre!(
        "deployment command failed with {status}: {}",
        detail.trim()
    ))
}

fn ensure_store_path(value: &str) -> Result<()> {
    ensure!(
        value.starts_with("/nix/store/")
            && !value.contains(['\n', '\r'])
            && Path::new(value).is_absolute(),
        "deployment result is not a Nix store path"
    );
    Ok(())
}

fn report(
    action: DeploymentAction,
    status: DeploymentReportStatus,
    drv_path: Option<String>,
    out_path: Option<String>,
    lock_digest: Option<String>,
    detail: &str,
) -> DeploymentReport {
    DeploymentReport {
        action,
        status,
        drv_path,
        out_path,
        lock_digest,
        observed_running: None,
        observed_profile: None,
        detail: detail.into(),
        completed_at: maxops_proto::now(),
    }
}

fn observed_report(
    action: DeploymentAction,
    status: DeploymentReportStatus,
    observed_running: Option<String>,
    observed_profile: Option<String>,
    detail: &str,
) -> DeploymentReport {
    DeploymentReport {
        action,
        status,
        drv_path: None,
        out_path: None,
        lock_digest: None,
        observed_running,
        observed_profile,
        detail: detail.into(),
        completed_at: maxops_proto::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollback_ownership_requires_both_runtime_links() {
        let artifact = DeploymentArtifact {
            builder_host: "builder".into(),
            source_commit: "a".repeat(40),
            tree_hash: "b".repeat(40),
            lock_digest: "c".repeat(64),
            drv_path: "/nix/store/example.drv".into(),
            out_path: "/nix/store/example-system".into(),
            built_at: maxops_proto::now(),
        };
        assert!(current_is_intended(
            &artifact,
            Some("/nix/store/example-system"),
            Some("/nix/store/example-system")
        ));
        assert!(!current_is_intended(
            &artifact,
            Some("/nix/store/external-system"),
            Some("/nix/store/example-system")
        ));
        assert!(current_is_owned(
            &artifact,
            Some("/nix/store/base-system"),
            Some("/nix/store/base-system"),
            Some("/nix/store/example-system")
        ));
        assert!(!current_is_owned(
            &artifact,
            Some("/nix/store/base-system"),
            Some("/nix/store/human-system"),
            Some("/nix/store/human-system")
        ));
        assert!(!current_is_owned(
            &artifact,
            Some("/nix/store/base-system"),
            Some("/nix/store/example-system"),
            Some("/nix/store/human-system")
        ));
    }
}
