use color_eyre::eyre::{Context, Result, ensure, eyre};
use maxops_proto::CommandSpec;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerSpec {
    pub command: CommandSpec,
    pub interpreter: PathBuf,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub credential_refs: Vec<String>,
    #[serde(default)]
    pub pass_credentials_directory: bool,
    pub output_limit_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunnerResult {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_discarded_bytes: u64,
    pub stderr_discarded_bytes: u64,
    pub completed_at: jiff::Timestamp,
}

pub async fn run_spec(spec: &RunnerSpec, output_directory: &Path) -> Result<RunnerResult> {
    ensure!(spec.output_limit_bytes > 0, "output limit must be positive");
    tokio::fs::create_dir_all(output_directory).await?;
    let mut command = match &spec.command {
        CommandSpec::Argv(argv) => {
            let (program, arguments) = argv.split_first().ok_or_else(|| eyre!("empty argv"))?;
            let mut command = Command::new(program);
            command.args(arguments);
            command
        }
        CommandSpec::Script(script) => {
            let mut command = Command::new(&spec.interpreter);
            command.arg("-c").arg(script);
            command
        }
    };
    command
        .env_clear()
        .envs(&spec.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(false);
    if !spec.credential_refs.is_empty() || spec.pass_credentials_directory {
        let directory = std::env::var_os("CREDENTIALS_DIRECTORY")
            .ok_or_else(|| eyre!("credential directory is unavailable"))?;
        command.env("CREDENTIALS_DIRECTORY", directory);
    }
    if let Some(cwd) = &spec.cwd {
        command.current_dir(cwd);
    }
    let mut child = command.spawn().wrap_err("start job command")?;
    let stdout = child.stdout.take().ok_or_else(|| eyre!("missing stdout"))?;
    let stderr = child.stderr.take().ok_or_else(|| eyre!("missing stderr"))?;
    let stdout_path = output_directory.join("stdout.bin");
    let stderr_path = output_directory.join("stderr.bin");
    let limit = spec.output_limit_bytes;
    let stdout_task = tokio::spawn(async move { copy_bounded(stdout, &stdout_path, limit).await });
    let stderr_task = tokio::spawn(async move { copy_bounded(stderr, &stderr_path, limit).await });
    let status = child.wait().await.wrap_err("wait for job command")?;
    let (stdout_bytes, stdout_discarded_bytes) = stdout_task.await??;
    let (stderr_bytes, stderr_discarded_bytes) = stderr_task.await??;
    #[cfg(unix)]
    let signal = std::os::unix::process::ExitStatusExt::signal(&status);
    #[cfg(not(unix))]
    let signal = None;
    Ok(RunnerResult {
        success: status.success(),
        exit_code: status.code(),
        signal,
        stdout_bytes,
        stderr_bytes,
        stdout_discarded_bytes,
        stderr_discarded_bytes,
        completed_at: maxops_proto::now(),
    })
}

pub async fn write_result_atomic(path: &Path, result: &RunnerResult) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| eyre!("result path has no parent"))?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec(result)?;
    let mut file = tokio::fs::File::create(&temporary).await?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary, path).await?;
    let directory = tokio::fs::File::open(parent).await?;
    directory.sync_all().await?;
    Ok(())
}

async fn copy_bounded(
    mut reader: impl AsyncRead + Unpin,
    path: &Path,
    limit: u64,
) -> Result<(u64, u64)> {
    let mut file = tokio::fs::File::create(path).await?;
    let mut written = 0_u64;
    let mut discarded = 0_u64;
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        let available = limit.saturating_sub(written);
        let keep = usize::try_from(available.min(count as u64)).unwrap_or(count);
        if keep > 0 {
            file.write_all(&buffer[..keep]).await?;
            written += keep as u64;
        }
        discarded += (count - keep) as u64;
    }
    file.sync_all().await?;
    Ok((written, discarded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runner_preserves_binary_output_and_drains_after_limit() {
        let directory = tempfile::tempdir().unwrap();
        let spec = RunnerSpec {
            command: CommandSpec::Argv(vec![
                "/bin/sh".into(),
                "-c".into(),
                "printf '\\001abcdef'; printf 'problem' >&2".into(),
            ]),
            interpreter: "/bin/sh".into(),
            cwd: None,
            env: BTreeMap::new(),
            credential_refs: Vec::new(),
            pass_credentials_directory: false,
            output_limit_bytes: 4,
        };
        let result = run_spec(&spec, directory.path()).await.unwrap();
        assert!(result.success);
        assert_eq!(
            tokio::fs::read(directory.path().join("stdout.bin"))
                .await
                .unwrap(),
            b"\x01abc"
        );
        assert_eq!(
            tokio::fs::read(directory.path().join("stderr.bin"))
                .await
                .unwrap(),
            b"prob"
        );
        assert_eq!(result.stdout_discarded_bytes, 3);
        assert_eq!(result.stderr_discarded_bytes, 3);
    }

    #[tokio::test]
    async fn completion_record_is_valid_after_atomic_rename() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("result.json");
        let result = RunnerResult {
            success: true,
            exit_code: Some(0),
            signal: None,
            stdout_bytes: 0,
            stderr_bytes: 0,
            stdout_discarded_bytes: 0,
            stderr_discarded_bytes: 0,
            completed_at: maxops_proto::now(),
        };
        write_result_atomic(&path, &result).await.unwrap();
        let restored: RunnerResult =
            serde_json::from_slice(&tokio::fs::read(path).await.unwrap()).unwrap();
        assert!(restored.success);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unwritable_output_storage_never_reports_success() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let spec = RunnerSpec {
            command: CommandSpec::Argv(vec!["/bin/sh".into(), "-c".into(), "printf output".into()]),
            interpreter: "/bin/sh".into(),
            cwd: None,
            env: BTreeMap::new(),
            credential_refs: Vec::new(),
            pass_credentials_directory: false,
            output_limit_bytes: 64,
        };
        let result = run_spec(&spec, directory.path()).await;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(result.is_err());
        assert!(!directory.path().join("result.json").exists());
    }
}
