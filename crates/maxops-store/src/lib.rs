//! Local durable state for a maxops hub or executor.
//!
//! Each process owns its own SQLite database. The store never attempts a
//! cross-host transaction and keeps observations separate from operation intent.

use color_eyre::eyre::{Context, Result, ensure, eyre};
use maxops_proto::{
    JobEvent, JobEventKind, JobHandle, JobId, JobRecord, JobState, NewJob, ResourceObservation,
    WorkspaceId, WorkspaceRecord, WorkspaceState,
};
use serde_json::Value;
use sqlx::{
    Row, SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{path::Path, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::Mutex;

static MIGRATOR: Migrator = sqlx::migrate!();
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    writer: Arc<Mutex<()>>,
}

#[derive(Debug)]
pub struct SubmitResult {
    pub job: JobRecord,
    pub created: bool,
}

impl Store {
    pub async fn open(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .wrap_err("open SQLite store")?;
        reject_newer_schema(&pool).await?;
        MIGRATOR.run(&pool).await.wrap_err("migrate SQLite store")?;
        Ok(Self {
            pool,
            writer: Arc::new(Mutex::new(())),
        })
    }

    pub async fn submit_job(&self, idempotency_key: &str, new: &NewJob) -> Result<SubmitResult> {
        validate_new_job(idempotency_key, new)?;
        let spec = canonical_json(&new.spec);
        let spec_json = serde_json::to_string(&spec)?;
        let spec_hash = job_spec_hash(new)?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;

        if let Some(row) =
            sqlx::query("SELECT job_id, spec_hash FROM idempotency WHERE principal = ? AND key = ?")
                .bind(&new.principal)
                .bind(idempotency_key)
                .fetch_optional(&mut *transaction)
                .await?
        {
            let existing_hash: String = row.try_get("spec_hash")?;
            ensure!(
                existing_hash == spec_hash,
                "idempotency key was already used with a different job specification"
            );
            let id: String = row.try_get("job_id")?;
            transaction.commit().await?;
            return Ok(SubmitResult {
                job: self.get_job_by_text(&id).await?,
                created: false,
            });
        }

        let id = uuid::Uuid::now_v7().to_string();
        let at = maxops_proto::now().to_string();
        let deadline = new.deadline.map(|value| value.to_string());
        sqlx::query(
            "INSERT INTO jobs (
                id, principal, host, operation, spec_version, spec_json, spec_hash,
                state, revision, policy_version, created_at, updated_at, deadline
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&new.principal)
        .bind(&new.host)
        .bind(&new.operation)
        .bind(i64::from(new.spec_version))
        .bind(&spec_json)
        .bind(&spec_hash)
        .bind(&new.policy_version)
        .bind(&at)
        .bind(&at)
        .bind(deadline)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO idempotency (principal, key, spec_hash, job_id, created_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&new.principal)
        .bind(idempotency_key)
        .bind(&spec_hash)
        .bind(&id)
        .bind(&at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO job_events (job_id, kind, state, occurred_at, payload_json)
             VALUES (?, 'submitted', 'queued', ?, '{}')",
        )
        .bind(&id)
        .bind(&at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(SubmitResult {
            job: self.get_job_by_text(&id).await?,
            created: true,
        })
    }

    /// Accept a job with an upstream-assigned ID. Executors use this to make
    /// repeated dispatch safe without minting a second target identity.
    pub async fn accept_job(&self, id: &JobId, new: &NewJob) -> Result<SubmitResult> {
        validate_new_job("upstream-job-id", new)?;
        let spec = canonical_json(&new.spec);
        let spec_json = serde_json::to_string(&spec)?;
        let spec_hash = job_spec_hash(new)?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        if let Some(row) = sqlx::query("SELECT spec_hash FROM jobs WHERE id = ?")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
        {
            ensure!(
                row.try_get::<String, _>("spec_hash")? == spec_hash,
                "job ID was already used with a different specification"
            );
            transaction.commit().await?;
            return Ok(SubmitResult {
                job: self.get_job(id).await?,
                created: false,
            });
        }
        let at = maxops_proto::now().to_string();
        sqlx::query(
            "INSERT INTO jobs (
                id, principal, host, operation, spec_version, spec_json, spec_hash,
                state, revision, policy_version, created_at, updated_at, deadline
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 1, ?, ?, ?, ?)",
        )
        .bind(id.as_str())
        .bind(&new.principal)
        .bind(&new.host)
        .bind(&new.operation)
        .bind(i64::from(new.spec_version))
        .bind(spec_json)
        .bind(spec_hash)
        .bind(&new.policy_version)
        .bind(&at)
        .bind(&at)
        .bind(new.deadline.map(|value| value.to_string()))
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO job_events (job_id, kind, state, occurred_at, payload_json)
             VALUES (?, 'submitted', 'queued', ?, '{}')",
        )
        .bind(id.as_str())
        .bind(&at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(SubmitResult {
            job: self.get_job(id).await?,
            created: true,
        })
    }

    pub async fn get_job(&self, id: &JobId) -> Result<JobRecord> {
        self.get_job_by_text(id.as_str()).await
    }

    pub async fn get_owned_job(&self, principal: &str, id: &JobId) -> Result<JobRecord> {
        let job = self.get_job(id).await?;
        ensure!(job.principal == principal, "job not found");
        Ok(job)
    }

    pub async fn list_jobs(
        &self,
        principal: Option<&str>,
        host: Option<&str>,
        states: &[JobState],
        limit: u16,
    ) -> Result<Vec<JobRecord>> {
        ensure!((1..=200).contains(&limit), "job list limit must be 1..200");
        let rows = sqlx::query(
            "SELECT id, principal, host, operation, spec_version, spec_json, spec_hash,
                    state, revision, policy_version, created_at, updated_at, deadline,
                    cancel_requested, result_json
             FROM jobs
             WHERE (? IS NULL OR principal = ?) AND (? IS NULL OR host = ?)
             ORDER BY created_at DESC LIMIT 1000",
        )
        .bind(principal)
        .bind(principal)
        .bind(host)
        .bind(host)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(row_to_job)
            .filter(|result| match result {
                Ok(job) => states.is_empty() || states.contains(&job.handle.state),
                Err(_) => true,
            })
            .take(usize::from(limit))
            .collect()
    }

    pub async fn nonterminal_jobs(&self) -> Result<Vec<JobRecord>> {
        let terminal = [
            JobState::Succeeded,
            JobState::Failed,
            JobState::Cancelled,
            JobState::TimedOut,
            JobState::OutcomeUnknown,
        ];
        self.list_jobs(None, None, &[], 200).await.map(|jobs| {
            jobs.into_iter()
                .filter(|job| !terminal.contains(&job.handle.state))
                .collect()
        })
    }

    /// Acquire one target-local resource for a job. A lock owned by the same
    /// job is recovered idempotently; a lock left by a terminal job is stale
    /// and may be replaced. Locks owned by live jobs are never stolen.
    pub async fn try_acquire_resource(
        &self,
        resource_kind: &str,
        resource_key: &str,
        owner: &JobId,
    ) -> Result<bool> {
        ensure!(
            !resource_kind.is_empty() && !resource_key.is_empty(),
            "resource identity must not be empty"
        );
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        if let Some(row) = sqlx::query(
            "SELECT resources.owner_job_id, resources.revision, jobs.state
             FROM resources JOIN jobs ON jobs.id = resources.owner_job_id
             WHERE resource_kind = ? AND resource_key = ?",
        )
        .bind(resource_kind)
        .bind(resource_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let current_owner: String = row.try_get("owner_job_id")?;
            if current_owner == owner.as_str() {
                transaction.commit().await?;
                return Ok(true);
            }
            let state = row
                .try_get::<&str, _>("state")?
                .parse::<JobState>()
                .map_err(|message| eyre!(message))?;
            if !state.is_terminal() {
                transaction.commit().await?;
                return Ok(false);
            }
            let revision: i64 = row.try_get("revision")?;
            sqlx::query(
                "UPDATE resources SET owner_job_id = ?, revision = ?, acquired_at = ?
                 WHERE resource_kind = ? AND resource_key = ? AND revision = ?",
            )
            .bind(owner.as_str())
            .bind(revision + 1)
            .bind(maxops_proto::now().to_string())
            .bind(resource_kind)
            .bind(resource_key)
            .bind(revision)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(true);
        }
        sqlx::query(
            "INSERT INTO resources
             (resource_kind, resource_key, owner_job_id, revision, acquired_at)
             VALUES (?, ?, ?, 1, ?)",
        )
        .bind(resource_kind)
        .bind(resource_key)
        .bind(owner.as_str())
        .bind(maxops_proto::now().to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn release_resource(
        &self,
        resource_kind: &str,
        resource_key: &str,
        owner: &JobId,
    ) -> Result<bool> {
        let _writer = self.writer.lock().await;
        let result = sqlx::query(
            "DELETE FROM resources
             WHERE resource_kind = ? AND resource_key = ? AND owner_job_id = ?",
        )
        .bind(resource_kind)
        .bind(resource_key)
        .bind(owner.as_str())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn create_workspace(
        &self,
        id: &WorkspaceId,
        repository: &str,
        executor: &str,
        base_commit: &str,
        tree_hash: &str,
        creator: &str,
    ) -> Result<WorkspaceRecord> {
        ensure!(
            maxops_proto::valid_repository_id(repository),
            "invalid repository ID"
        );
        ensure!(
            maxops_proto::valid_host(executor),
            "invalid workspace executor"
        );
        ensure!(
            maxops_proto::valid_git_oid(base_commit),
            "invalid base commit"
        );
        ensure!(maxops_proto::valid_git_oid(tree_hash), "invalid tree hash");
        ensure!(
            !creator.is_empty() && creator.len() <= 128,
            "invalid workspace creator"
        );
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        if let Some(row) = workspace_row(id, &mut transaction).await? {
            let existing = row_to_workspace(&row)?;
            ensure!(
                existing.repository == repository
                    && existing.executor == executor
                    && existing.base_commit.eq_ignore_ascii_case(base_commit)
                    && existing.tree_hash.eq_ignore_ascii_case(tree_hash)
                    && existing.creator == creator,
                "workspace ID was already used with different metadata"
            );
            transaction.commit().await?;
            return Ok(existing);
        }
        let at = maxops_proto::now().to_string();
        sqlx::query(
            "INSERT INTO workspaces (
                id, repository_id, executor, base_commit, revision, tree_hash,
                commit_hash, state, creator, created_at, retain_until
             ) VALUES (?, ?, ?, ?, 1, ?, NULL, 'clean', ?, ?, NULL)",
        )
        .bind(id.as_str())
        .bind(repository)
        .bind(executor)
        .bind(base_commit.to_ascii_lowercase())
        .bind(tree_hash.to_ascii_lowercase())
        .bind(creator)
        .bind(at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.get_workspace(id).await
    }

    pub async fn get_workspace(&self, id: &WorkspaceId) -> Result<WorkspaceRecord> {
        let row = sqlx::query(
            "SELECT id, repository_id, executor, base_commit, revision, tree_hash,
                    commit_hash, state, creator, created_at, retain_until
             FROM workspaces WHERE id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| eyre!("workspace not found"))?;
        row_to_workspace(&row)
    }

    pub async fn get_owned_workspace(
        &self,
        creator: &str,
        repository: &str,
        id: &WorkspaceId,
    ) -> Result<WorkspaceRecord> {
        let workspace = self.get_workspace(id).await?;
        ensure!(
            workspace.creator == creator && workspace.repository == repository,
            "workspace not found"
        );
        Ok(workspace)
    }

    pub async fn transition_workspace(
        &self,
        id: &WorkspaceId,
        creator: &str,
        expected_revision: u64,
        tree_hash: &str,
        commit_hash: Option<&str>,
        state: WorkspaceState,
    ) -> Result<WorkspaceRecord> {
        ensure!(maxops_proto::valid_git_oid(tree_hash), "invalid tree hash");
        ensure!(
            commit_hash.is_none_or(maxops_proto::valid_git_oid),
            "invalid commit hash"
        );
        let _writer = self.writer.lock().await;
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| eyre!("workspace revision overflow"))?;
        let changed = sqlx::query(
            "UPDATE workspaces
             SET revision = ?, tree_hash = ?, commit_hash = ?, state = ?
             WHERE id = ? AND creator = ? AND revision = ?",
        )
        .bind(to_i64(next_revision, "workspace revision")?)
        .bind(tree_hash.to_ascii_lowercase())
        .bind(commit_hash.map(str::to_ascii_lowercase))
        .bind(state.as_str())
        .bind(id.as_str())
        .bind(creator)
        .bind(to_i64(expected_revision, "workspace revision")?)
        .execute(&self.pool)
        .await?;
        ensure!(changed.rows_affected() == 1, "workspace revision changed");
        self.get_workspace(id).await
    }

    pub async fn transition_job(
        &self,
        id: &JobId,
        expected_revision: u64,
        next: JobState,
        payload: &Value,
        result: Option<&Value>,
    ) -> Result<JobRecord> {
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT state, revision FROM jobs WHERE id = ?")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| eyre!("job not found"))?;
        let current = JobState::from_str(row.try_get::<&str, _>("state")?)
            .map_err(|message| eyre!(message))?;
        let revision = to_u64(row.try_get::<i64, _>("revision")?, "job revision")?;
        ensure!(revision == expected_revision, "job revision changed");
        ensure!(
            current.can_transition_to(next),
            "invalid job state transition from {} to {}",
            current.as_str(),
            next.as_str()
        );
        let next_revision = revision
            .checked_add(1)
            .ok_or_else(|| eyre!("job revision overflow"))?;
        let at = maxops_proto::now().to_string();
        let payload_json = serde_json::to_string(&canonical_json(payload))?;
        let result_json = result
            .map(canonical_json)
            .map(|value| serde_json::to_string(&value))
            .transpose()?;
        let changed = sqlx::query(
            "UPDATE jobs SET state = ?, revision = ?, updated_at = ?, result_json = ?
             WHERE id = ? AND revision = ?",
        )
        .bind(next.as_str())
        .bind(to_i64(next_revision, "job revision")?)
        .bind(&at)
        .bind(result_json)
        .bind(id.as_str())
        .bind(to_i64(revision, "job revision")?)
        .execute(&mut *transaction)
        .await?;
        ensure!(changed.rows_affected() == 1, "job revision changed");
        let kind = if current == JobState::Reconciling || current == JobState::OutcomeUnknown {
            JobEventKind::Reconciled
        } else {
            JobEventKind::StateChanged
        };
        sqlx::query(
            "INSERT INTO job_events (job_id, kind, state, occurred_at, payload_json)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(id.as_str())
        .bind(kind.as_str())
        .bind(next.as_str())
        .bind(&at)
        .bind(payload_json)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.get_job(id).await
    }

    pub async fn request_cancel(
        &self,
        id: &JobId,
        expected_revision: u64,
        reason: &str,
    ) -> Result<JobRecord> {
        ensure!(
            !reason.is_empty() && reason.len() <= 1024,
            "invalid cancel reason"
        );
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT state, revision FROM jobs WHERE id = ?")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| eyre!("job not found"))?;
        let state = JobState::from_str(row.try_get::<&str, _>("state")?)
            .map_err(|message| eyre!(message))?;
        let revision = to_u64(row.try_get::<i64, _>("revision")?, "job revision")?;
        ensure!(revision == expected_revision, "job revision changed");
        ensure!(!state.is_terminal(), "terminal job cannot be cancelled");
        let next_revision = revision
            .checked_add(1)
            .ok_or_else(|| eyre!("job revision overflow"))?;
        let at = maxops_proto::now().to_string();
        let changed = sqlx::query(
            "UPDATE jobs SET cancel_requested = 1, revision = ?, updated_at = ?
             WHERE id = ? AND revision = ?",
        )
        .bind(to_i64(next_revision, "job revision")?)
        .bind(&at)
        .bind(id.as_str())
        .bind(to_i64(revision, "job revision")?)
        .execute(&mut *transaction)
        .await?;
        ensure!(changed.rows_affected() == 1, "job revision changed");
        sqlx::query(
            "INSERT INTO job_events (job_id, kind, state, occurred_at, payload_json)
             VALUES (?, 'cancel_requested', ?, ?, ?)",
        )
        .bind(id.as_str())
        .bind(state.as_str())
        .bind(&at)
        .bind(serde_json::to_string(
            &serde_json::json!({"reason": reason}),
        )?)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.get_job(id).await
    }

    pub async fn job_events(&self, id: &JobId, after_sequence: u64) -> Result<Vec<JobEvent>> {
        let rows = sqlx::query(
            "SELECT sequence, kind, state, occurred_at, payload_json
             FROM job_events WHERE job_id = ? AND sequence > ? ORDER BY sequence",
        )
        .bind(id.as_str())
        .bind(to_i64(after_sequence, "event sequence")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(JobEvent {
                    sequence: to_u64(row.try_get("sequence")?, "event sequence")?,
                    job_id: id.clone(),
                    kind: JobEventKind::parse(row.try_get::<&str, _>("kind")?)
                        .map_err(|message| eyre!(message))?,
                    state: JobState::from_str(row.try_get::<&str, _>("state")?)
                        .map_err(|message| eyre!(message))?,
                    occurred_at: parse_timestamp(row.try_get("occurred_at")?)?,
                    payload: serde_json::from_str(row.try_get("payload_json")?)?,
                })
            })
            .collect()
    }

    pub async fn record_observation(&self, observation: &ResourceObservation) -> Result<u64> {
        ensure!(
            !observation.resource_key.is_empty(),
            "empty observation resource key"
        );
        let _writer = self.writer.lock().await;
        let result = sqlx::query(
            "INSERT INTO observations (
                resource_kind, resource_key, observed_at, value_json, evidence, related_change_id
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(observation.resource_kind.as_str())
        .bind(&observation.resource_key)
        .bind(observation.observed_at.to_string())
        .bind(serde_json::to_string(&canonical_json(&observation.value))?)
        .bind(&observation.evidence)
        .bind(&observation.related_change_id)
        .execute(&self.pool)
        .await?;
        to_u64(result.last_insert_rowid(), "observation sequence")
    }

    pub async fn latest_observation(
        &self,
        resource_kind: maxops_proto::ResourceKind,
        resource_key: &str,
    ) -> Result<Option<ResourceObservation>> {
        let row = sqlx::query(
            "SELECT resource_kind, resource_key, observed_at, value_json, evidence,
                    related_change_id
             FROM observations WHERE resource_kind = ? AND resource_key = ?
             ORDER BY sequence DESC LIMIT 1",
        )
        .bind(resource_kind.as_str())
        .bind(resource_key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(ResourceObservation {
                resource_kind: maxops_proto::ResourceKind::parse(
                    row.try_get::<&str, _>("resource_kind")?,
                )
                .map_err(|message| eyre!(message))?,
                resource_key: row.try_get("resource_key")?,
                observed_at: parse_timestamp(row.try_get("observed_at")?)?,
                value: serde_json::from_str(row.try_get("value_json")?)?,
                evidence: row.try_get("evidence")?,
                related_change_id: row.try_get("related_change_id")?,
            })
        })
        .transpose()
    }

    /// Create a consistent standalone backup. The destination must not exist.
    pub async fn backup_to(&self, destination: &Path) -> Result<()> {
        ensure!(!destination.exists(), "backup destination already exists");
        let destination = destination
            .to_str()
            .ok_or_else(|| eyre!("backup destination is not valid UTF-8"))?;
        let _writer = self.writer.lock().await;
        sqlx::query("VACUUM INTO ?")
            .bind(destination)
            .execute(&self.pool)
            .await
            .wrap_err("create consistent SQLite backup")?;
        Ok(())
    }

    async fn get_job_by_text(&self, id: &str) -> Result<JobRecord> {
        let row = sqlx::query(
            "SELECT id, principal, host, operation, spec_version, spec_json, spec_hash,
                    state, revision, policy_version, created_at, updated_at, deadline,
                    cancel_requested, result_json
             FROM jobs WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| eyre!("job not found"))?;
        row_to_job(&row)
    }
}

fn validate_new_job(idempotency_key: &str, new: &NewJob) -> Result<()> {
    ensure!(
        !idempotency_key.is_empty()
            && idempotency_key.len() <= MAX_IDEMPOTENCY_KEY_BYTES
            && idempotency_key.bytes().all(|byte| byte.is_ascii_graphic()),
        "idempotency key must contain 1..128 printable ASCII characters without spaces"
    );
    ensure!(
        !new.principal.is_empty() && new.principal.len() <= 128,
        "invalid principal"
    );
    ensure!(maxops_proto::valid_host(&new.host), "invalid host");
    ensure!(
        !new.operation.is_empty() && new.operation.len() <= 128,
        "invalid operation"
    );
    ensure!(new.spec_version > 0, "invalid spec version");
    ensure!(
        !new.policy_version.is_empty() && new.policy_version.len() <= 128,
        "invalid policy version"
    );
    ensure!(
        new.spec.is_object(),
        "job specification must be a JSON object"
    );
    Ok(())
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries: Vec<_> = object.iter().collect();
            entries.sort_by_key(|(key, _)| *key);
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonical_json(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        _ => value.clone(),
    }
}

pub fn job_spec_hash(new: &NewJob) -> Result<String> {
    let request_fingerprint = canonical_json(&serde_json::json!({
        "host": new.host,
        "operation": new.operation,
        "spec_version": new.spec_version,
        "spec": new.spec,
    }));
    let fingerprint_json = serde_json::to_vec(&request_fingerprint)?;
    Ok(blake3::hash(&fingerprint_json).to_hex().to_string())
}

fn row_to_job(row: &sqlx::sqlite::SqliteRow) -> Result<JobRecord> {
    let id = JobId::parse(row.try_get::<String, _>("id")?).map_err(|message| eyre!(message))?;
    let state =
        JobState::from_str(row.try_get::<&str, _>("state")?).map_err(|message| eyre!(message))?;
    let revision = to_u64(row.try_get("revision")?, "job revision")?;
    let operation: String = row.try_get("operation")?;
    let host: String = row.try_get("host")?;
    Ok(JobRecord {
        handle: JobHandle {
            job_id: id,
            state,
            revision,
            operation,
            host,
        },
        principal: row.try_get("principal")?,
        spec_version: u32::try_from(row.try_get::<i64, _>("spec_version")?)
            .map_err(|_| eyre!("invalid spec version in database"))?,
        spec: serde_json::from_str(row.try_get("spec_json")?)?,
        spec_hash: row.try_get("spec_hash")?,
        policy_version: row.try_get("policy_version")?,
        created_at: parse_timestamp(row.try_get("created_at")?)?,
        updated_at: parse_timestamp(row.try_get("updated_at")?)?,
        deadline: row
            .try_get::<Option<String>, _>("deadline")?
            .map(parse_timestamp)
            .transpose()?,
        cancel_requested: row.try_get::<i64, _>("cancel_requested")? != 0,
        result: row
            .try_get::<Option<String>, _>("result_json")?
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
    })
}

async fn workspace_row<'a>(
    id: &WorkspaceId,
    transaction: &mut sqlx::Transaction<'a, sqlx::Sqlite>,
) -> Result<Option<sqlx::sqlite::SqliteRow>> {
    Ok(sqlx::query(
        "SELECT id, repository_id, executor, base_commit, revision, tree_hash,
                commit_hash, state, creator, created_at, retain_until
         FROM workspaces WHERE id = ?",
    )
    .bind(id.as_str())
    .fetch_optional(&mut **transaction)
    .await?)
}

fn row_to_workspace(row: &sqlx::sqlite::SqliteRow) -> Result<WorkspaceRecord> {
    Ok(WorkspaceRecord {
        workspace_id: WorkspaceId::parse(row.try_get::<String, _>("id")?)
            .map_err(|message| eyre!(message))?,
        repository: row.try_get("repository_id")?,
        executor: row.try_get("executor")?,
        base_commit: row.try_get("base_commit")?,
        revision: to_u64(row.try_get("revision")?, "workspace revision")?,
        tree_hash: row.try_get("tree_hash")?,
        commit_hash: row.try_get("commit_hash")?,
        state: WorkspaceState::from_str(row.try_get::<&str, _>("state")?)
            .map_err(|message| eyre!(message))?,
        creator: row.try_get("creator")?,
        created_at: parse_timestamp(row.try_get("created_at")?)?,
        retain_until: row
            .try_get::<Option<String>, _>("retain_until")?
            .map(parse_timestamp)
            .transpose()?,
    })
}

fn parse_timestamp(value: String) -> Result<jiff::Timestamp> {
    value.parse().wrap_err("invalid timestamp in database")
}

fn to_i64(value: u64, name: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| eyre!("{name} exceeds SQLite integer range"))
}

fn to_u64(value: i64, name: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| eyre!("invalid {name} in database"))
}

async fn reject_newer_schema(pool: &SqlitePool) -> Result<()> {
    let exists: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await?;
    if exists == 0 {
        return Ok(());
    }
    let database_version: Option<i64> =
        sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations WHERE success = 1")
            .fetch_one(pool)
            .await?;
    let supported = MIGRATOR
        .iter()
        .map(|migration| migration.version)
        .max()
        .unwrap_or(0);
    ensure!(
        database_version.unwrap_or(0) <= supported,
        "database schema is newer than this maxops binary"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use maxops_proto::ResourceKind;
    use serde_json::json;
    use std::sync::Arc;

    fn job(spec: Value) -> NewJob {
        NewJob {
            principal: "automation-a".into(),
            host: "host-a".into(),
            operation: "exec.run".into(),
            spec_version: 1,
            spec,
            policy_version: "policy-1".into(),
            deadline: None,
        }
    }

    #[tokio::test]
    async fn concurrent_idempotent_submission_creates_one_job_and_event() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store.db"))
            .await
            .unwrap();
        let new = Arc::new(job(json!({"argv":["true"],"env":{"B":"2","A":"1"}})));
        let mut tasks = Vec::new();
        for _ in 0..24 {
            let store = store.clone();
            let new = new.clone();
            tasks.push(tokio::spawn(async move {
                store.submit_job("same-request", &new).await.unwrap()
            }));
        }
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        assert_eq!(results.iter().filter(|result| result.created).count(), 1);
        let first = &results[0].job.handle.job_id;
        assert!(
            results
                .iter()
                .all(|result| &result.job.handle.job_id == first)
        );
        assert_eq!(store.job_events(first, 0).await.unwrap().len(), 1);

        let same = job(json!({"env":{"A":"1","B":"2"},"argv":["true"]}));
        assert!(
            !store
                .submit_job("same-request", &same)
                .await
                .unwrap()
                .created
        );
        let conflict = job(json!({"argv":["false"]}));
        assert!(store.submit_job("same-request", &conflict).await.is_err());
        let mut other_host = job(json!({"argv":["true"],"env":{"A":"1","B":"2"}}));
        other_host.host = "host-b".into();
        assert!(store.submit_job("same-request", &other_host).await.is_err());
    }

    #[tokio::test]
    async fn state_and_event_revision_change_together() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store.db"))
            .await
            .unwrap();
        let submitted = store
            .submit_job("state-test", &job(json!({"argv":["true"]})))
            .await
            .unwrap();
        let id = &submitted.job.handle.job_id;
        let dispatching = store
            .transition_job(id, 1, JobState::Dispatching, &json!({"executor":"a"}), None)
            .await
            .unwrap();
        assert_eq!(dispatching.handle.revision, 2);
        assert_eq!(dispatching.handle.state, JobState::Dispatching);
        assert!(
            store
                .transition_job(id, 1, JobState::Running, &json!({}), None)
                .await
                .is_err()
        );
        assert_eq!(store.job_events(id, 0).await.unwrap().len(), 2);

        let cancelling = store.request_cancel(id, 2, "test stop").await.unwrap();
        assert!(cancelling.cancel_requested);
        assert_eq!(cancelling.handle.revision, 3);
        assert_eq!(store.job_events(id, 0).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn resource_lock_serializes_jobs_and_recovers_from_terminal_owner() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store.db"))
            .await
            .unwrap();
        let first = store
            .submit_job("lock-first", &job(json!({"unit":"demo.service"})))
            .await
            .unwrap()
            .job;
        let second = store
            .submit_job("lock-second", &job(json!({"unit":"demo.service"})))
            .await
            .unwrap()
            .job;
        assert!(
            store
                .try_acquire_resource("systemd_manager", "host-a", &first.handle.job_id)
                .await
                .unwrap()
        );
        assert!(
            !store
                .try_acquire_resource("systemd_manager", "host-a", &second.handle.job_id)
                .await
                .unwrap()
        );
        store
            .transition_job(
                &first.handle.job_id,
                first.handle.revision,
                JobState::Failed,
                &json!({}),
                None,
            )
            .await
            .unwrap();
        assert!(
            store
                .try_acquire_resource("systemd_manager", "host-a", &second.handle.job_id)
                .await
                .unwrap()
        );
        assert!(
            !store
                .release_resource("systemd_manager", "host-a", &first.handle.job_id)
                .await
                .unwrap()
        );
        assert!(
            store
                .release_resource("systemd_manager", "host-a", &second.handle.job_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn workspace_revision_compare_and_swap_preserves_owner() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store.db"))
            .await
            .unwrap();
        let id = WorkspaceId::parse("00000000-0000-0000-0000-000000000123").unwrap();
        let base = "0123456789abcdef0123456789abcdef01234567";
        let tree = "1123456789abcdef0123456789abcdef01234567";
        let workspace = store
            .create_workspace(&id, "infra", "host-a", base, tree, "automation-a")
            .await
            .unwrap();
        assert_eq!(workspace.revision, 1);
        assert_eq!(workspace.state, WorkspaceState::Clean);
        assert!(
            store
                .get_owned_workspace("other", "infra", &id)
                .await
                .is_err()
        );
        let commit = "2123456789abcdef0123456789abcdef01234567";
        let committed = store
            .transition_workspace(
                &id,
                "automation-a",
                1,
                tree,
                Some(commit),
                WorkspaceState::Committed,
            )
            .await
            .unwrap();
        assert_eq!(committed.revision, 2);
        assert_eq!(committed.commit_hash.as_deref(), Some(commit));
        assert!(
            store
                .transition_workspace(
                    &id,
                    "automation-a",
                    1,
                    tree,
                    Some(commit),
                    WorkspaceState::Published,
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn backup_reopens_with_job_and_observations_remain_external_facts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.db");
        let backup = directory.path().join("backup.db");
        let store = Store::open(&path).await.unwrap();
        let submitted = store
            .submit_job("backup-test", &job(json!({"argv":["true"]})))
            .await
            .unwrap();
        let observed = ResourceObservation {
            resource_kind: ResourceKind::RunningSystem,
            resource_key: "host-a/system".into(),
            observed_at: maxops_proto::now(),
            value: json!({"closure":"/nix/store/external-system"}),
            evidence: "agent snapshot".into(),
            related_change_id: None,
        };
        store.record_observation(&observed).await.unwrap();
        let latest = store
            .latest_observation(ResourceKind::RunningSystem, "host-a/system")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.value, observed.value);
        assert_eq!(
            store
                .get_job(&submitted.job.handle.job_id)
                .await
                .unwrap()
                .spec,
            json!({"argv":["true"]})
        );

        store.backup_to(&backup).await.unwrap();
        let restored = Store::open(&backup).await.unwrap();
        assert_eq!(
            restored
                .get_job(&submitted.job.handle.job_id)
                .await
                .unwrap()
                .handle
                .job_id,
            submitted.job.handle.job_id
        );
    }

    #[tokio::test]
    async fn rejects_database_from_a_newer_binary() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.db");
        let store = Store::open(&path).await.unwrap();
        sqlx::query(
            "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, execution_time)
             VALUES (999, 'future', CURRENT_TIMESTAMP, 1, X'00', 0)",
        )
        .execute(&store.pool)
        .await
        .unwrap();
        store.pool.close().await;
        assert!(Store::open(&path).await.is_err());
    }
}
