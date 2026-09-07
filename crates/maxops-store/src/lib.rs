//! Local durable state for a maxops hub or executor.
//!
//! Each process owns its own SQLite database. The store never attempts a
//! cross-host transaction and keeps observations separate from operation intent.

use color_eyre::eyre::{Context, Result, ensure, eyre};
use maxops_proto::{
    ChangeId, ChangeJobs, ChangePlan, ChangeRecord, ChangeState, DeliveryStage, DeploymentArtifact,
    EpisodeId, EventId, EventKind, EventRecord, EventsListParams, EventsListResponse, JobEvent,
    JobEventKind, JobHandle, JobId, JobRecord, JobState, NewJob, RemediationId, RemediationRecord,
    RemediationState, ResourceObservation, WorkspaceId, WorkspaceRecord, WorkspaceState,
};
use serde_json::Value;
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

static MIGRATOR: Migrator = sqlx::migrate!();
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    writer: Arc<Mutex<()>>,
    path: PathBuf,
    changed: Arc<tokio::sync::Notify>,
}

#[derive(Debug)]
pub struct SubmitResult {
    pub job: JobRecord,
    pub created: bool,
}

pub struct ChangeJobLink<'a> {
    pub change: &'a ChangeRecord,
    pub next: ChangeState,
    /// None claims the change for a new workflow; Some links a primitive stage.
    pub action: Option<maxops_proto::DeploymentAction>,
    pub workflow: Option<&'a JobId>,
}

pub struct ChangeTransition<'a> {
    pub expected_revision: u64,
    pub next: ChangeState,
    pub plan: &'a ChangePlan,
    pub artifact: Option<&'a DeploymentArtifact>,
    pub jobs: &'a ChangeJobs,
    pub recovery_state: Option<&'a str>,
}

pub struct AlertEventInput {
    pub source: String,
    pub fingerprint: String,
    pub host: String,
    pub firing: bool,
    pub occurred_at: jiff::Timestamp,
    pub payload: Value,
}

pub struct NewFleetEvent {
    pub source: String,
    pub fingerprint: String,
    pub episode_id: EpisodeId,
    pub kind: EventKind,
    pub host: String,
    pub occurred_at: jiff::Timestamp,
    pub related_job_id: Option<JobId>,
    pub related_change_id: Option<ChangeId>,
    pub payload: Value,
}

pub struct RemediationCompletion<'a> {
    pub expected_revision: u64,
    pub outcome: RemediationState,
    pub related_job_id: Option<&'a JobId>,
    pub related_change_id: Option<&'a ChangeId>,
    pub summary: &'a str,
}

#[derive(Clone, Debug)]
pub struct SubscriptionCursor {
    pub id: String,
    pub cursor: u64,
}

#[derive(Clone, Debug)]
pub struct DeliveryRecord {
    pub stage: DeliveryStage,
    pub attempts: u32,
}

#[derive(Clone, Debug)]
pub struct StoreStats {
    pub jobs_queued: u64,
    pub jobs_nonterminal: u64,
    pub jobs_outcome_unknown: u64,
    pub jobs_completed_total: u64,
    pub job_duration_seconds_sum: f64,
    pub reconciliations_total: u64,
    pub events_total: u64,
    pub deliveries_pending: u64,
    pub remediations_active: u64,
    pub database_size_bytes: u64,
    pub storage_available_bytes: u64,
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
            path: path.to_owned(),
            changed: Arc::new(tokio::sync::Notify::new()),
        })
    }

    pub async fn submit_job(&self, idempotency_key: &str, new: &NewJob) -> Result<SubmitResult> {
        let id =
            JobId::parse(uuid::Uuid::now_v7().to_string()).map_err(|message| eyre!(message))?;
        self.submit_job_with_id(idempotency_key, &id, new).await
    }

    pub async fn submit_job_with_id(
        &self,
        idempotency_key: &str,
        id: &JobId,
        new: &NewJob,
    ) -> Result<SubmitResult> {
        self.submit_linked_job(idempotency_key, id, new, None).await
    }

    /// Job identity, idempotency receipt, change revision and stage ownership
    /// commit together. No orphaned stage can be dispatched after a failed CAS.
    pub async fn submit_linked_job(
        &self,
        idempotency_key: &str,
        id: &JobId,
        new: &NewJob,
        link: Option<ChangeJobLink<'_>>,
    ) -> Result<SubmitResult> {
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
            self.changed.notify_waiters();
            return Ok(SubmitResult {
                job: self.get_job_by_text(&id).await?,
                created: false,
            });
        }

        let at = maxops_proto::now().to_string();
        let deadline = new.deadline.map(|value| value.to_string());
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
        .bind(&spec_json)
        .bind(&spec_hash)
        .bind(&new.policy_version)
        .bind(&at)
        .bind(&at)
        .bind(deadline)
        .execute(&mut *transaction)
        .await?;
        if let Some(link) = link {
            let change = link.change;
            let row = sqlx::query("SELECT c.revision, c.workflow_job_id, j.state AS workflow_state, j.cancel_requested FROM changes c LEFT JOIN jobs j ON j.id = c.workflow_job_id WHERE c.id = ? AND c.creator = ?")
                .bind(change.plan.change_id.as_str()).bind(&new.principal)
                .fetch_optional(&mut *transaction).await?.ok_or_else(|| eyre!("change not found"))?;
            ensure!(
                to_u64(row.try_get("revision")?, "change revision")? == change.revision,
                "change revision changed"
            );
            let owner: Option<String> = row.try_get("workflow_job_id")?;
            let owner_state: Option<String> = row.try_get("workflow_state")?;
            if let Some(workflow) = link.workflow {
                ensure!(
                    owner.as_deref() == Some(workflow.as_str()),
                    "workflow ownership changed"
                );
                ensure!(
                    owner_state.as_deref() == Some("running")
                        && row.try_get::<i64, _>("cancel_requested")? == 0,
                    "workflow is not running"
                );
            } else if let Some(state) = owner_state {
                let state = JobState::from_str(&state).map_err(|message| eyre!(message))?;
                ensure!(
                    state.is_terminal() && state != JobState::OutcomeUnknown,
                    "change is owned by a deployment workflow"
                );
            }
            ensure!(
                change.state == link.next || change.state.can_transition_to(link.next),
                "invalid change state transition"
            );
            let mut jobs = change.jobs.clone();
            if let Some(action) = link.action {
                use maxops_proto::DeploymentAction::*;
                *match action {
                    Build => &mut jobs.build,
                    Activate => &mut jobs.activate,
                    Verify => &mut jobs.verify,
                    Rollback => &mut jobs.rollback,
                    Prepare => &mut jobs.prepare,
                } = Some(id.clone());
            }
            let owner = if link.action.is_none() {
                Some(id.as_str())
            } else {
                owner.as_deref()
            };
            sqlx::query("UPDATE changes SET revision = revision + 1, state = ?, jobs_json = ?, workflow_job_id = ?, updated_at = ? WHERE id = ? AND creator = ? AND revision = ?")
                .bind(link.next.as_str()).bind(serde_json::to_string(&jobs)?).bind(owner).bind(&at)
                .bind(change.plan.change_id.as_str()).bind(&new.principal).bind(to_i64(change.revision, "change revision")?)
                .execute(&mut *transaction).await?;
        }
        sqlx::query(
            "INSERT INTO idempotency (principal, key, spec_hash, job_id, created_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&new.principal)
        .bind(idempotency_key)
        .bind(&spec_hash)
        .bind(id.as_str())
        .bind(&at)
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
        self.changed.notify_waiters();
        Ok(SubmitResult {
            job: self.get_job(id).await?,
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
            self.changed.notify_waiters();
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
        self.changed.notify_waiters();
        Ok(SubmitResult {
            job: self.get_job(id).await?,
            created: true,
        })
    }

    pub async fn get_job(&self, id: &JobId) -> Result<JobRecord> {
        self.get_job_by_text(id.as_str()).await
    }

    pub async fn get_idempotent_job(
        &self,
        principal: &str,
        key: &str,
    ) -> Result<Option<JobRecord>> {
        let row = sqlx::query("SELECT job_id FROM idempotency WHERE principal = ? AND key = ?")
            .bind(principal)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        match row {
            Some(row) => Ok(Some(
                self.get_job_by_text(row.try_get::<&str, _>("job_id")?)
                    .await?,
            )),
            None => Ok(None),
        }
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

    pub async fn list_visible_jobs(
        &self,
        principal: &str,
        hosts: &BTreeSet<String>,
        params: &maxops_proto::JobsListParams,
    ) -> Result<Vec<JobRecord>> {
        ensure!(
            (1..=200).contains(&params.limit),
            "job list limit must be 1..200"
        );
        let cursor = match &params.cursor {
            Some(id) => {
                let job = self.get_owned_job(principal, id).await?;
                ensure!(hosts.contains(&job.handle.host), "job not found");
                Some((job.created_at.to_string(), id.as_str()))
            }
            None => None,
        };
        let mut query = QueryBuilder::<Sqlite>::new("SELECT * FROM jobs WHERE principal = ");
        query.push_bind(principal);
        if hosts.is_empty() {
            return Ok(vec![]);
        }
        query.push(" AND host IN (");
        let mut names = query.separated(",");
        for host in hosts {
            names.push_bind(host);
        }
        names.push_unseparated(")");
        if let Some(host) = &params.host {
            query.push(" AND host = ").push_bind(host);
        }
        if !params.states.is_empty() {
            query.push(" AND state IN (");
            let mut states = query.separated(",");
            for state in &params.states {
                states.push_bind(state.as_str());
            }
            states.push_unseparated(")");
        }
        if let Some((at, id)) = cursor {
            query
                .push(" AND (created_at, id) < (")
                .push_bind(at)
                .push(",")
                .push_bind(id)
                .push(")");
        }
        query
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(i64::from(params.limit) + 1);
        query
            .build()
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(row_to_job)
            .collect()
    }

    pub async fn list_visible_changes(
        &self,
        principal: &str,
        hosts: &BTreeSet<String>,
        deployments: &BTreeSet<String>,
        params: &maxops_proto::ChangeHistoryParams,
    ) -> Result<Vec<ChangeRecord>> {
        ensure!(
            (1..=200).contains(&params.limit),
            "change list limit must be 1..200"
        );
        let cursor = match &params.cursor {
            Some(id) => {
                let change = self.get_owned_change(principal, id).await?;
                ensure!(hosts.contains(&change.plan.target_host), "change not found");
                Some((change.plan.created_at.to_string(), id.as_str()))
            }
            None => None,
        };
        let mut query = QueryBuilder::<Sqlite>::new("SELECT * FROM changes WHERE creator = ");
        query.push_bind(principal);
        if hosts.is_empty() {
            return Ok(vec![]);
        }
        query.push(" AND host IN (");
        let mut names = query.separated(",");
        for host in hosts {
            names.push_bind(host);
        }
        names.push_unseparated(")");
        if deployments.is_empty() {
            return Ok(vec![]);
        }
        query.push(" AND json_extract(intent_json, '$.deployment_profile') IN (");
        let mut profiles = query.separated(",");
        for deployment in deployments {
            profiles.push_bind(deployment);
        }
        profiles.push_unseparated(")");
        if let Some(host) = &params.host {
            query.push(" AND host = ").push_bind(host);
        }
        if let Some((at, id)) = cursor {
            query
                .push(" AND (created_at, id) < (")
                .push_bind(at)
                .push(",")
                .push_bind(id)
                .push(")");
        }
        query
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(i64::from(params.limit) + 1);
        query
            .build()
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(row_to_change)
            .collect()
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

    /// Persist an executor-owned workspace projection in the hub database.
    /// A later observation may advance content but cannot reassign identity.
    pub async fn record_workspace(&self, workspace: &WorkspaceRecord) -> Result<WorkspaceRecord> {
        let _writer = self.writer.lock().await;
        let changed = sqlx::query(
            "INSERT INTO workspaces (
                id, repository_id, executor, base_commit, revision, tree_hash,
                commit_hash, state, creator, created_at, retain_until
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                revision = excluded.revision,
                tree_hash = excluded.tree_hash,
                commit_hash = excluded.commit_hash,
                state = excluded.state,
                retain_until = excluded.retain_until
             WHERE workspaces.repository_id = excluded.repository_id
               AND workspaces.executor = excluded.executor
               AND workspaces.base_commit = excluded.base_commit
               AND workspaces.creator = excluded.creator",
        )
        .bind(workspace.workspace_id.as_str())
        .bind(&workspace.repository)
        .bind(&workspace.executor)
        .bind(&workspace.base_commit)
        .bind(to_i64(workspace.revision, "workspace revision")?)
        .bind(workspace.tree_hash.to_ascii_lowercase())
        .bind(
            workspace
                .commit_hash
                .as_ref()
                .map(|value| value.to_ascii_lowercase()),
        )
        .bind(workspace.state.as_str())
        .bind(&workspace.creator)
        .bind(workspace.created_at.to_string())
        .bind(workspace.retain_until.map(|value| value.to_string()))
        .execute(&self.pool)
        .await?;
        ensure!(
            changed.rows_affected() == 1,
            "workspace projection identity changed"
        );
        self.get_workspace(&workspace.workspace_id).await
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

    pub async fn create_change(
        &self,
        plan: &ChangePlan,
        creator: &str,
        prepare_job: &JobId,
    ) -> Result<ChangeRecord> {
        ensure!(
            !creator.is_empty() && creator.len() <= 128,
            "invalid change creator"
        );
        let jobs = ChangeJobs {
            prepare: Some(prepare_job.clone()),
            ..ChangeJobs::default()
        };
        let _writer = self.writer.lock().await;
        let at = maxops_proto::now().to_string();
        sqlx::query(
            "INSERT INTO changes (
                id, workspace_id, workspace_revision, host, profile, source_json,
                artifact_json, baseline_json, intent_json, state, recovery_state,
                created_at, updated_at, creator, revision, jobs_json
             ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, 'checking', NULL, ?, ?, ?, 1, ?)",
        )
        .bind(plan.change_id.as_str())
        .bind(plan.workspace_id.as_str())
        .bind(to_i64(plan.workspace_revision, "workspace revision")?)
        .bind(&plan.target_host)
        .bind(&plan.deployment_profile)
        .bind(serde_json::to_string(&canonical_json(
            &serde_json::to_value(&plan.source_baseline)?,
        ))?)
        .bind(serde_json::to_string(&canonical_json(
            &serde_json::to_value(&plan.runtime_baseline)?,
        ))?)
        .bind(serde_json::to_string(&canonical_json(
            &serde_json::to_value(plan)?,
        ))?)
        .bind(&at)
        .bind(&at)
        .bind(creator)
        .bind(serde_json::to_string(&jobs)?)
        .execute(&self.pool)
        .await?;
        self.get_change(&plan.change_id).await
    }

    pub async fn get_change(&self, id: &ChangeId) -> Result<ChangeRecord> {
        let row = sqlx::query(
            "SELECT intent_json, artifact_json, state, recovery_state, creator,
                    revision, jobs_json, updated_at
             FROM changes WHERE id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| eyre!("change not found"))?;
        row_to_change(&row)
    }

    pub async fn get_owned_change(&self, creator: &str, id: &ChangeId) -> Result<ChangeRecord> {
        let change = self.get_change(id).await?;
        ensure!(change.creator == creator, "change not found");
        Ok(change)
    }

    pub async fn list_changes(
        &self,
        creator: &str,
        host: Option<&str>,
        limit: u16,
    ) -> Result<Vec<ChangeRecord>> {
        ensure!(
            (1..=200).contains(&limit),
            "change list limit must be 1..200"
        );
        let rows = sqlx::query(
            "SELECT intent_json, artifact_json, state, recovery_state, creator,
                    revision, jobs_json, updated_at
             FROM changes
             WHERE creator = ? AND (? IS NULL OR host = ?)
             ORDER BY created_at DESC LIMIT ?",
        )
        .bind(creator)
        .bind(host)
        .bind(host)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_change).collect()
    }

    pub async fn transition_change(
        &self,
        id: &ChangeId,
        creator: &str,
        update: ChangeTransition<'_>,
    ) -> Result<ChangeRecord> {
        let ChangeTransition {
            expected_revision,
            next,
            plan,
            artifact,
            jobs,
            recovery_state,
        } = update;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT state, revision FROM changes WHERE id = ? AND creator = ?")
            .bind(id.as_str())
            .bind(creator)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| eyre!("change not found"))?;
        let current = ChangeState::from_str(row.try_get::<&str, _>("state")?)
            .map_err(|message| eyre!(message))?;
        let revision = to_u64(row.try_get("revision")?, "change revision")?;
        ensure!(revision == expected_revision, "change revision changed");
        ensure!(
            current == next || current.can_transition_to(next),
            "invalid change state transition from {} to {}",
            current.as_str(),
            next.as_str()
        );
        let next_revision = revision
            .checked_add(1)
            .ok_or_else(|| eyre!("change revision overflow"))?;
        let at = maxops_proto::now().to_string();
        let changed = sqlx::query(
            "UPDATE changes
             SET state = ?, revision = ?, intent_json = ?, artifact_json = ?,
                 jobs_json = ?, recovery_state = ?, updated_at = ?
             WHERE id = ? AND creator = ? AND revision = ?",
        )
        .bind(next.as_str())
        .bind(to_i64(next_revision, "change revision")?)
        .bind(serde_json::to_string(&canonical_json(
            &serde_json::to_value(plan)?,
        ))?)
        .bind(
            artifact
                .map(serde_json::to_value)
                .transpose()?
                .map(|value| serde_json::to_string(&canonical_json(&value)))
                .transpose()?,
        )
        .bind(serde_json::to_string(jobs)?)
        .bind(recovery_state)
        .bind(at)
        .bind(id.as_str())
        .bind(creator)
        .bind(to_i64(expected_revision, "change revision")?)
        .execute(&mut *transaction)
        .await?;
        ensure!(changed.rows_affected() == 1, "change revision changed");
        transaction.commit().await?;
        self.get_change(id).await
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
        self.changed.notify_waiters();
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
        self.changed.notify_waiters();
        self.get_job(id).await
    }

    pub fn job_changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }

    pub async fn job_events(&self, id: &JobId, after_sequence: u64) -> Result<Vec<JobEvent>> {
        self.job_events_page(id, after_sequence, 200).await
    }

    pub async fn job_events_page(
        &self,
        id: &JobId,
        after_sequence: u64,
        limit: u16,
    ) -> Result<Vec<JobEvent>> {
        ensure!((1..=200).contains(&limit), "event limit must be 1..200");
        let rows = sqlx::query(
            "SELECT sequence, kind, state, occurred_at, payload_json
             FROM job_events WHERE job_id = ? AND sequence > ? ORDER BY sequence LIMIT ?",
        )
        .bind(id.as_str())
        .bind(to_i64(after_sequence, "event sequence")?)
        .bind(i64::from(limit))
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

    pub async fn ingest_alert(&self, input: AlertEventInput) -> Result<EventRecord> {
        validate_event_identity(&input.source, &input.fingerprint, &input.host)?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        let existing = sqlx::query(
            "SELECT episode_id, active FROM fleet_event_episodes
             WHERE source = ? AND fingerprint = ? AND host = ?",
        )
        .bind(&input.source)
        .bind(&input.fingerprint)
        .bind(&input.host)
        .fetch_optional(&mut *transaction)
        .await?;
        let episode_id = match existing.as_ref() {
            Some(row) if input.firing && row.try_get::<i64, _>("active")? != 0 => {
                EpisodeId::parse(row.try_get::<String, _>("episode_id")?)
                    .map_err(|message| eyre!(message))?
            }
            Some(row) if !input.firing => EpisodeId::parse(row.try_get::<String, _>("episode_id")?)
                .map_err(|message| eyre!(message))?,
            _ => EpisodeId::parse(uuid::Uuid::now_v7().to_string())
                .map_err(|message| eyre!(message))?,
        };
        let received_at = maxops_proto::now();
        sqlx::query(
            "INSERT INTO fleet_event_episodes
             (source, fingerprint, host, episode_id, active, last_received_at)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(source, fingerprint, host) DO UPDATE SET
                 episode_id = excluded.episode_id,
                 active = excluded.active,
                 last_received_at = excluded.last_received_at",
        )
        .bind(&input.source)
        .bind(&input.fingerprint)
        .bind(&input.host)
        .bind(episode_id.as_str())
        .bind(i64::from(input.firing))
        .bind(received_at.to_string())
        .execute(&mut *transaction)
        .await?;
        let event_id =
            EventId::parse(uuid::Uuid::now_v7().to_string()).map_err(|message| eyre!(message))?;
        let kind = if input.firing {
            EventKind::AlertFiring
        } else {
            EventKind::AlertResolved
        };
        insert_fleet_event(
            &mut transaction,
            &event_id,
            &NewFleetEvent {
                source: input.source,
                fingerprint: input.fingerprint,
                episode_id,
                kind,
                host: input.host,
                occurred_at: input.occurred_at,
                related_job_id: None,
                related_change_id: None,
                payload: input.payload,
            },
            received_at,
        )
        .await?;
        transaction.commit().await?;
        self.get_event(&event_id).await
    }

    pub async fn append_fleet_event(&self, input: NewFleetEvent) -> Result<EventRecord> {
        validate_event_identity(&input.source, &input.fingerprint, &input.host)?;
        let event_id =
            EventId::parse(uuid::Uuid::now_v7().to_string()).map_err(|message| eyre!(message))?;
        let received_at = maxops_proto::now();
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        insert_fleet_event(&mut transaction, &event_id, &input, received_at).await?;
        transaction.commit().await?;
        self.get_event(&event_id).await
    }

    pub async fn get_event(&self, id: &EventId) -> Result<EventRecord> {
        let row = sqlx::query(
            "SELECT sequence, id, source, fingerprint, episode_id, kind, host,
                    occurred_at, received_at, related_job_id, related_change_id, payload_json
             FROM fleet_events WHERE id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| eyre!("event not found"))?;
        row_to_event(&row)
    }

    pub async fn event_for_host(&self, id: &EventId, host: &str) -> Result<EventRecord> {
        let event = self.get_event(id).await?;
        ensure!(event.host == host, "event not found");
        Ok(event)
    }

    pub async fn event_for_job_kind(
        &self,
        job_id: &JobId,
        kind: EventKind,
    ) -> Result<Option<EventRecord>> {
        let row = sqlx::query(
            "SELECT sequence, id, source, fingerprint, episode_id, kind, host,
                    occurred_at, received_at, related_job_id, related_change_id, payload_json
             FROM fleet_events WHERE related_job_id = ? AND kind = ?
             ORDER BY sequence LIMIT 1",
        )
        .bind(job_id.as_str())
        .bind(kind.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_event).transpose()
    }

    pub async fn list_events(
        &self,
        allowed_hosts: &BTreeSet<String>,
        params: &EventsListParams,
    ) -> Result<EventsListResponse> {
        params.validate().map_err(|message| eyre!(message))?;
        ensure!(!allowed_hosts.is_empty(), "event host scope is empty");
        if let Some(host) = &params.host {
            ensure!(allowed_hosts.contains(host), "host not permitted");
        }
        let hosts: Vec<&String> = params
            .host
            .as_ref()
            .map(|host| vec![host])
            .unwrap_or_else(|| allowed_hosts.iter().collect());

        let mut minimum = QueryBuilder::<Sqlite>::new(
            "SELECT MIN(sequence) AS sequence FROM fleet_events WHERE host IN (",
        );
        {
            let mut separated = minimum.separated(", ");
            for host in &hosts {
                separated.push_bind(*host);
            }
        }
        minimum.push(")");
        let first: Option<i64> = minimum.build_query_scalar().fetch_one(&self.pool).await?;
        let earliest_cursor = match first {
            Some(sequence) => to_u64(sequence, "event sequence")?.saturating_sub(1),
            None => {
                let latest: Option<i64> = sqlx::query_scalar(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'fleet_events'",
                )
                .fetch_optional(&self.pool)
                .await?;
                latest
                    .map(|sequence| to_u64(sequence, "event sequence"))
                    .transpose()?
                    .unwrap_or(0)
            }
        };
        let cursor = params.cursor.unwrap_or(earliest_cursor);
        ensure!(
            params.cursor.is_none() || cursor >= earliest_cursor,
            "event cursor expired; resync from {earliest_cursor}"
        );

        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT sequence, id, source, fingerprint, episode_id, kind, host,
                    occurred_at, received_at, related_job_id, related_change_id, payload_json
             FROM fleet_events WHERE sequence > ",
        );
        query.push_bind(to_i64(cursor, "event cursor")?);
        query.push(" AND host IN (");
        {
            let mut separated = query.separated(", ");
            for host in &hosts {
                separated.push_bind(*host);
            }
        }
        query.push(")");
        if !params.kinds.is_empty() {
            query.push(" AND kind IN (");
            {
                let mut separated = query.separated(", ");
                for kind in &params.kinds {
                    separated.push_bind(kind.as_str());
                }
            }
            query.push(")");
        }
        query.push(" ORDER BY sequence LIMIT ");
        query.push_bind(i64::from(params.limit));
        let events: Vec<EventRecord> = query
            .build()
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(row_to_event)
            .collect::<Result<_>>()?;
        let next_cursor = if events.len() == usize::from(params.limit) {
            events.last().map(|event| event.sequence).unwrap_or(cursor)
        } else {
            let mut maximum = QueryBuilder::<Sqlite>::new(
                "SELECT MAX(sequence) FROM fleet_events WHERE host IN (",
            );
            {
                let mut separated = maximum.separated(", ");
                for host in &hosts {
                    separated.push_bind(*host);
                }
            }
            maximum.push(")");
            let last: Option<i64> = maximum.build_query_scalar().fetch_one(&self.pool).await?;
            last.map(|sequence| to_u64(sequence, "event sequence"))
                .transpose()?
                .unwrap_or(cursor)
                .max(cursor)
        };
        Ok(EventsListResponse {
            events,
            next_cursor,
            earliest_cursor,
        })
    }

    pub async fn prune_events_through(&self, sequence: u64) -> Result<u64> {
        let _writer = self.writer.lock().await;
        let result = sqlx::query("DELETE FROM fleet_events WHERE sequence <= ?")
            .bind(to_i64(sequence, "event sequence")?)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    pub async fn ensure_subscription(
        &self,
        id: &str,
        filter: &Value,
        target: &Value,
        credential_ref: Option<&str>,
    ) -> Result<SubscriptionCursor> {
        ensure!(!id.is_empty() && id.len() <= 128, "invalid subscription ID");
        let _writer = self.writer.lock().await;
        sqlx::query(
            "INSERT INTO subscriptions
             (id, principal, filter_json, target_json, credential_ref, cursor, created_at)
             VALUES (?, 'system', ?, ?, ?, 0, ?)
             ON CONFLICT(id) DO UPDATE SET
                 filter_json = excluded.filter_json,
                 target_json = excluded.target_json,
                 credential_ref = excluded.credential_ref",
        )
        .bind(id)
        .bind(serde_json::to_string(&canonical_json(filter))?)
        .bind(serde_json::to_string(&canonical_json(target))?)
        .bind(credential_ref)
        .bind(maxops_proto::now().to_string())
        .execute(&self.pool)
        .await?;
        self.subscription_cursor(id).await
    }

    pub async fn subscription_cursor(&self, id: &str) -> Result<SubscriptionCursor> {
        let row = sqlx::query("SELECT id, cursor FROM subscriptions WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| eyre!("subscription not found"))?;
        Ok(SubscriptionCursor {
            id: row.try_get("id")?,
            cursor: to_u64(row.try_get("cursor")?, "subscription cursor")?,
        })
    }

    pub async fn next_event(&self, after: u64) -> Result<Option<EventRecord>> {
        let row = sqlx::query(
            "SELECT sequence, id, source, fingerprint, episode_id, kind, host,
                    occurred_at, received_at, related_job_id, related_change_id, payload_json
             FROM fleet_events WHERE sequence > ? ORDER BY sequence LIMIT 1",
        )
        .bind(to_i64(after, "event cursor")?)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(row_to_event).transpose()
    }

    pub async fn begin_delivery(
        &self,
        subscription_id: &str,
        event_sequence: u64,
        retry_after_seconds: u32,
    ) -> Result<Option<DeliveryRecord>> {
        let now = maxops_proto::now();
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        if let Some(row) = sqlx::query(
            "SELECT stage, attempts, retry_at FROM event_deliveries
             WHERE subscription_id = ? AND event_sequence = ?",
        )
        .bind(subscription_id)
        .bind(to_i64(event_sequence, "event sequence")?)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let stage = DeliveryStage::parse(row.try_get::<&str, _>("stage")?)
                .map_err(|message| eyre!(message))?;
            if stage != DeliveryStage::Queued {
                transaction.commit().await?;
                return Ok(None);
            }
            if row
                .try_get::<Option<String>, _>("retry_at")?
                .map(parse_timestamp)
                .transpose()?
                .is_some_and(|retry_at| retry_at > now)
            {
                transaction.commit().await?;
                return Ok(None);
            }
        }
        let retry_at = now
            .checked_add(Duration::from_secs(u64::from(retry_after_seconds)))
            .map_err(|_| eyre!("delivery retry deadline overflow"))?;
        sqlx::query(
            "INSERT INTO event_deliveries
             (subscription_id, event_sequence, stage, attempts, retry_at, updated_at)
             VALUES (?, ?, 'queued', 1, ?, ?)
             ON CONFLICT(subscription_id, event_sequence) DO UPDATE SET
                 attempts = attempts + 1,
                 retry_at = excluded.retry_at,
                 updated_at = excluded.updated_at",
        )
        .bind(subscription_id)
        .bind(to_i64(event_sequence, "event sequence")?)
        .bind(retry_at.to_string())
        .bind(now.to_string())
        .execute(&mut *transaction)
        .await?;
        let attempts: i64 = sqlx::query_scalar(
            "SELECT attempts FROM event_deliveries
             WHERE subscription_id = ? AND event_sequence = ?",
        )
        .bind(subscription_id)
        .bind(to_i64(event_sequence, "event sequence")?)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(DeliveryRecord {
            stage: DeliveryStage::Queued,
            attempts: u32::try_from(attempts).map_err(|_| eyre!("invalid delivery attempts"))?,
        }))
    }

    pub async fn delivery_record(
        &self,
        subscription_id: &str,
        event_sequence: u64,
    ) -> Result<DeliveryRecord> {
        let row = sqlx::query(
            "SELECT stage, attempts FROM event_deliveries
             WHERE subscription_id = ? AND event_sequence = ?",
        )
        .bind(subscription_id)
        .bind(to_i64(event_sequence, "event sequence")?)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| eyre!("delivery intent not found"))?;
        Ok(DeliveryRecord {
            stage: DeliveryStage::parse(row.try_get::<&str, _>("stage")?)
                .map_err(|message| eyre!(message))?,
            attempts: u32::try_from(row.try_get::<i64, _>("attempts")?)
                .map_err(|_| eyre!("invalid delivery attempts"))?,
        })
    }

    pub async fn acknowledge_delivery(
        &self,
        subscription_id: &str,
        event_sequence: u64,
        stage: DeliveryStage,
        response_status: Option<u16>,
    ) -> Result<()> {
        ensure!(
            stage != DeliveryStage::Queued,
            "delivery acknowledgement is not final"
        );
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        let changed = sqlx::query(
            "UPDATE event_deliveries
             SET stage = ?, retry_at = NULL, response_status = ?, updated_at = ?
             WHERE subscription_id = ? AND event_sequence = ?",
        )
        .bind(stage.as_str())
        .bind(response_status.map(i64::from))
        .bind(maxops_proto::now().to_string())
        .bind(subscription_id)
        .bind(to_i64(event_sequence, "event sequence")?)
        .execute(&mut *transaction)
        .await?;
        ensure!(changed.rows_affected() == 1, "delivery intent not found");
        sqlx::query(
            "UPDATE subscriptions SET cursor = MAX(cursor, ?), acknowledged_at = ? WHERE id = ?",
        )
        .bind(to_i64(event_sequence, "event sequence")?)
        .bind(maxops_proto::now().to_string())
        .bind(subscription_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn skip_subscription_event(
        &self,
        subscription_id: &str,
        event_sequence: u64,
    ) -> Result<()> {
        let _writer = self.writer.lock().await;
        sqlx::query("UPDATE subscriptions SET cursor = MAX(cursor, ?) WHERE id = ?")
            .bind(to_i64(event_sequence, "event sequence")?)
            .bind(subscription_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn begin_remediation(
        &self,
        event_id: &EventId,
        host: &str,
        principal: &str,
        related_job_id: &JobId,
        max_attempts: u16,
        cooldown_seconds: u32,
    ) -> Result<RemediationRecord> {
        ensure!(max_attempts > 0, "remediation attempts must be positive");
        let remediation_id = RemediationId::parse(uuid::Uuid::now_v7().to_string())
            .map_err(|message| eyre!(message))?;
        let now = maxops_proto::now();
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        if let Some(existing) = sqlx::query_scalar::<_, String>(
            "SELECT id FROM remediations WHERE related_job_id = ? AND principal = ?",
        )
        .bind(related_job_id.as_str())
        .bind(principal)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let existing = RemediationId::parse(existing).map_err(|message| eyre!(message))?;
            transaction.commit().await?;
            return self.get_remediation(&existing, principal).await;
        }
        let event =
            sqlx::query("SELECT episode_id, fingerprint, host FROM fleet_events WHERE id = ?")
                .bind(event_id.as_str())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| eyre!("event not found"))?;
        ensure!(event.try_get::<&str, _>("host")? == host, "event not found");
        let episode_id = EpisodeId::parse(event.try_get::<String, _>("episode_id")?)
            .map_err(|message| eyre!(message))?;
        let fingerprint: String = event.try_get("fingerprint")?;
        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM remediations WHERE episode_id = ? AND state = 'active'",
        )
        .bind(episode_id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(active == 0, "remediation already active for this episode");
        let host_active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM remediations WHERE host = ? AND state = 'active'",
        )
        .bind(host)
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(host_active == 0, "remediation already active for this host");
        let attempts: i64 =
            sqlx::query_scalar("SELECT count(*) FROM remediations WHERE episode_id = ?")
                .bind(episode_id.as_str())
                .fetch_one(&mut *transaction)
                .await?;
        ensure!(
            attempts < i64::from(max_attempts),
            "remediation attempt budget exhausted"
        );
        let last_started: Option<String> =
            sqlx::query_scalar("SELECT MAX(started_at) FROM remediations WHERE episode_id = ?")
                .bind(episode_id.as_str())
                .fetch_one(&mut *transaction)
                .await?;
        if let Some(last_started) = last_started {
            let last_started = parse_timestamp(last_started)?;
            ensure!(
                now.as_second() - last_started.as_second() >= i64::from(cooldown_seconds),
                "remediation cooldown active"
            );
        }
        let attempt = attempts + 1;
        sqlx::query(
            "INSERT INTO remediations
             (id, event_id, episode_id, host, principal, attempt, revision, state, started_at,
              related_job_id)
             VALUES (?, ?, ?, ?, ?, ?, 1, 'active', ?, ?)",
        )
        .bind(remediation_id.as_str())
        .bind(event_id.as_str())
        .bind(episode_id.as_str())
        .bind(host)
        .bind(principal)
        .bind(attempt)
        .bind(now.to_string())
        .bind(related_job_id.as_str())
        .execute(&mut *transaction)
        .await?;
        let lifecycle_event =
            EventId::parse(uuid::Uuid::now_v7().to_string()).map_err(|message| eyre!(message))?;
        insert_fleet_event(
            &mut transaction,
            &lifecycle_event,
            &NewFleetEvent {
                source: "maxops.remediation".into(),
                fingerprint,
                episode_id,
                kind: EventKind::RemediationStarted,
                host: host.to_owned(),
                occurred_at: now,
                related_job_id: Some(related_job_id.clone()),
                related_change_id: None,
                payload: serde_json::json!({
                    "remediation_id": remediation_id,
                    "attempt": attempt,
                    "principal": principal,
                }),
            },
            now,
        )
        .await?;
        transaction.commit().await?;
        self.get_remediation(&remediation_id, principal).await
    }

    pub async fn finish_remediation(
        &self,
        id: &RemediationId,
        principal: &str,
        completion: RemediationCompletion<'_>,
    ) -> Result<RemediationRecord> {
        ensure!(
            completion.outcome != RemediationState::Active,
            "remediation outcome must be terminal"
        );
        ensure!(
            !completion.summary.is_empty() && completion.summary.len() <= 1024,
            "invalid remediation summary"
        );
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT event_id, episode_id, host, revision, state FROM remediations
             WHERE id = ? AND principal = ?",
        )
        .bind(id.as_str())
        .bind(principal)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| eyre!("remediation not found"))?;
        let revision = to_u64(row.try_get("revision")?, "remediation revision")?;
        ensure!(
            revision == completion.expected_revision,
            "remediation revision changed"
        );
        ensure!(
            row.try_get::<&str, _>("state")? == "active",
            "remediation is terminal"
        );
        let event_id = EventId::parse(row.try_get::<String, _>("event_id")?)
            .map_err(|message| eyre!(message))?;
        let episode_id = EpisodeId::parse(row.try_get::<String, _>("episode_id")?)
            .map_err(|message| eyre!(message))?;
        let host: String = row.try_get("host")?;
        let fingerprint: String =
            sqlx::query_scalar("SELECT fingerprint FROM fleet_events WHERE id = ?")
                .bind(event_id.as_str())
                .fetch_one(&mut *transaction)
                .await?;
        let now = maxops_proto::now();
        sqlx::query(
            "UPDATE remediations SET state = ?, revision = revision + 1, finished_at = ?,
                 related_job_id = ?, related_change_id = ?, summary = ?
             WHERE id = ? AND principal = ? AND revision = ?",
        )
        .bind(completion.outcome.as_str())
        .bind(now.to_string())
        .bind(completion.related_job_id.map(JobId::as_str))
        .bind(completion.related_change_id.map(ChangeId::as_str))
        .bind(completion.summary)
        .bind(id.as_str())
        .bind(principal)
        .bind(to_i64(
            completion.expected_revision,
            "remediation revision",
        )?)
        .execute(&mut *transaction)
        .await?;
        let lifecycle_event =
            EventId::parse(uuid::Uuid::now_v7().to_string()).map_err(|message| eyre!(message))?;
        insert_fleet_event(
            &mut transaction,
            &lifecycle_event,
            &NewFleetEvent {
                source: "maxops.remediation".into(),
                fingerprint,
                episode_id,
                kind: EventKind::RemediationFinished,
                host,
                occurred_at: now,
                related_job_id: completion.related_job_id.cloned(),
                related_change_id: completion.related_change_id.cloned(),
                payload: serde_json::json!({
                    "remediation_id": id,
                    "outcome": completion.outcome,
                    "summary": completion.summary,
                }),
            },
            now,
        )
        .await?;
        transaction.commit().await?;
        self.get_remediation(id, principal).await
    }

    pub async fn get_remediation(
        &self,
        id: &RemediationId,
        principal: &str,
    ) -> Result<RemediationRecord> {
        let row = sqlx::query(
            "SELECT id, event_id, episode_id, host, principal, attempt, revision, state,
                    started_at, finished_at, related_job_id, related_change_id, summary
             FROM remediations WHERE id = ? AND principal = ?",
        )
        .bind(id.as_str())
        .bind(principal)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| eyre!("remediation not found"))?;
        row_to_remediation(&row)
    }

    pub async fn stats(&self) -> Result<StoreStats> {
        let jobs_queued: i64 =
            sqlx::query_scalar("SELECT count(*) FROM jobs WHERE state = 'queued'")
                .fetch_one(&self.pool)
                .await?;
        let jobs_nonterminal: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM jobs WHERE state NOT IN
             ('succeeded', 'failed', 'cancelled', 'timed_out', 'outcome_unknown')",
        )
        .fetch_one(&self.pool)
        .await?;
        let jobs_outcome_unknown: i64 =
            sqlx::query_scalar("SELECT count(*) FROM jobs WHERE state = 'outcome_unknown'")
                .fetch_one(&self.pool)
                .await?;
        let (jobs_completed_total, job_duration_seconds_sum): (i64, f64) = sqlx::query_as(
            "SELECT count(*), COALESCE(
                SUM((julianday(updated_at) - julianday(created_at)) * 86400.0), 0.0
             ) FROM jobs WHERE state IN
             ('succeeded', 'failed', 'cancelled', 'timed_out', 'outcome_unknown')",
        )
        .fetch_one(&self.pool)
        .await?;
        let reconciliations_total: i64 =
            sqlx::query_scalar("SELECT count(*) FROM job_events WHERE kind = 'reconciled'")
                .fetch_one(&self.pool)
                .await?;
        let events_total: i64 = sqlx::query_scalar("SELECT count(*) FROM fleet_events")
            .fetch_one(&self.pool)
            .await?;
        let deliveries_pending: i64 =
            sqlx::query_scalar("SELECT count(*) FROM event_deliveries WHERE stage = 'queued'")
                .fetch_one(&self.pool)
                .await?;
        let remediations_active: i64 =
            sqlx::query_scalar("SELECT count(*) FROM remediations WHERE state = 'active'")
                .fetch_one(&self.pool)
                .await?;
        let filesystem = rustix::fs::statvfs(&self.path)?;
        let fragment_size = if filesystem.f_frsize == 0 {
            filesystem.f_bsize
        } else {
            filesystem.f_frsize
        };
        Ok(StoreStats {
            jobs_queued: to_u64(jobs_queued, "job count")?,
            jobs_nonterminal: to_u64(jobs_nonterminal, "job count")?,
            jobs_outcome_unknown: to_u64(jobs_outcome_unknown, "job count")?,
            jobs_completed_total: to_u64(jobs_completed_total, "job count")?,
            job_duration_seconds_sum,
            reconciliations_total: to_u64(reconciliations_total, "reconciliation count")?,
            events_total: to_u64(events_total, "event count")?,
            deliveries_pending: to_u64(deliveries_pending, "delivery count")?,
            remediations_active: to_u64(remediations_active, "remediation count")?,
            database_size_bytes: std::fs::metadata(&self.path)?.len(),
            storage_available_bytes: filesystem.f_bavail.saturating_mul(fragment_size),
        })
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

fn row_to_change(row: &sqlx::sqlite::SqliteRow) -> Result<ChangeRecord> {
    Ok(ChangeRecord {
        plan: serde_json::from_str(row.try_get("intent_json")?)?,
        creator: row.try_get("creator")?,
        revision: to_u64(row.try_get("revision")?, "change revision")?,
        state: ChangeState::from_str(row.try_get::<&str, _>("state")?)
            .map_err(|message| eyre!(message))?,
        artifact: row
            .try_get::<Option<String>, _>("artifact_json")?
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        jobs: serde_json::from_str(row.try_get("jobs_json")?)?,
        recovery_state: row.try_get("recovery_state")?,
        updated_at: parse_timestamp(row.try_get("updated_at")?)?,
    })
}

async fn insert_fleet_event(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    event_id: &EventId,
    input: &NewFleetEvent,
    received_at: jiff::Timestamp,
) -> Result<u64> {
    let result = sqlx::query(
        "INSERT INTO fleet_events
         (id, source, fingerprint, episode_id, kind, host, occurred_at, received_at,
          related_job_id, related_change_id, payload_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(event_id.as_str())
    .bind(&input.source)
    .bind(&input.fingerprint)
    .bind(input.episode_id.as_str())
    .bind(input.kind.as_str())
    .bind(&input.host)
    .bind(input.occurred_at.to_string())
    .bind(received_at.to_string())
    .bind(input.related_job_id.as_ref().map(JobId::as_str))
    .bind(input.related_change_id.as_ref().map(ChangeId::as_str))
    .bind(serde_json::to_string(&canonical_json(&input.payload))?)
    .execute(&mut **transaction)
    .await?;
    to_u64(result.last_insert_rowid(), "event sequence")
}

fn validate_event_identity(source: &str, fingerprint: &str, host: &str) -> Result<()> {
    ensure!(
        !source.is_empty() && source.len() <= 128,
        "invalid event source"
    );
    ensure!(
        !fingerprint.is_empty() && fingerprint.len() <= 512,
        "invalid event fingerprint"
    );
    ensure!(maxops_proto::valid_host(host), "invalid event host");
    Ok(())
}

fn row_to_event(row: &sqlx::sqlite::SqliteRow) -> Result<EventRecord> {
    Ok(EventRecord {
        sequence: to_u64(row.try_get("sequence")?, "event sequence")?,
        event_id: EventId::parse(row.try_get::<String, _>("id")?)
            .map_err(|message| eyre!(message))?,
        source: row.try_get("source")?,
        fingerprint: row.try_get("fingerprint")?,
        episode_id: EpisodeId::parse(row.try_get::<String, _>("episode_id")?)
            .map_err(|message| eyre!(message))?,
        kind: EventKind::parse(row.try_get::<&str, _>("kind")?)
            .map_err(|message| eyre!(message))?,
        host: row.try_get("host")?,
        occurred_at: parse_timestamp(row.try_get("occurred_at")?)?,
        received_at: parse_timestamp(row.try_get("received_at")?)?,
        related_job_id: row
            .try_get::<Option<String>, _>("related_job_id")?
            .map(JobId::parse)
            .transpose()
            .map_err(|message| eyre!(message))?,
        related_change_id: row
            .try_get::<Option<String>, _>("related_change_id")?
            .map(ChangeId::parse)
            .transpose()
            .map_err(|message| eyre!(message))?,
        payload: serde_json::from_str(row.try_get("payload_json")?)?,
    })
}

fn row_to_remediation(row: &sqlx::sqlite::SqliteRow) -> Result<RemediationRecord> {
    Ok(RemediationRecord {
        remediation_id: RemediationId::parse(row.try_get::<String, _>("id")?)
            .map_err(|message| eyre!(message))?,
        event_id: EventId::parse(row.try_get::<String, _>("event_id")?)
            .map_err(|message| eyre!(message))?,
        episode_id: EpisodeId::parse(row.try_get::<String, _>("episode_id")?)
            .map_err(|message| eyre!(message))?,
        host: row.try_get("host")?,
        principal: row.try_get("principal")?,
        attempt: u16::try_from(row.try_get::<i64, _>("attempt")?)
            .map_err(|_| eyre!("invalid remediation attempt"))?,
        revision: to_u64(row.try_get("revision")?, "remediation revision")?,
        state: RemediationState::parse(row.try_get::<&str, _>("state")?)
            .map_err(|message| eyre!(message))?,
        started_at: parse_timestamp(row.try_get("started_at")?)?,
        finished_at: row
            .try_get::<Option<String>, _>("finished_at")?
            .map(parse_timestamp)
            .transpose()?,
        related_job_id: row
            .try_get::<Option<String>, _>("related_job_id")?
            .map(JobId::parse)
            .transpose()
            .map_err(|message| eyre!(message))?,
        related_change_id: row
            .try_get::<Option<String>, _>("related_change_id")?
            .map(ChangeId::parse)
            .transpose()
            .map_err(|message| eyre!(message))?,
        summary: row.try_get("summary")?,
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
    async fn alert_episodes_and_replay_cursors_track_lifecycle() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store.db"))
            .await
            .unwrap();
        let alert = |firing| AlertEventInput {
            source: "alertmanager".into(),
            fingerprint: "fixture-fingerprint".into(),
            host: "host-a".into(),
            firing,
            occurred_at: maxops_proto::now(),
            payload: json!({"status": if firing { "firing" } else { "resolved" }}),
        };
        let first = store.ingest_alert(alert(true)).await.unwrap();
        let duplicate = store.ingest_alert(alert(true)).await.unwrap();
        assert_eq!(first.episode_id, duplicate.episode_id);
        let resolved = store.ingest_alert(alert(false)).await.unwrap();
        assert_eq!(first.episode_id, resolved.episode_id);
        let next = store.ingest_alert(alert(true)).await.unwrap();
        assert_ne!(first.episode_id, next.episode_id);

        let hosts = BTreeSet::from(["host-a".to_owned()]);
        let replay = store
            .list_events(
                &hosts,
                &EventsListParams {
                    cursor: Some(0),
                    host: None,
                    kinds: Vec::new(),
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(replay.events.len(), 4);
        assert_eq!(replay.next_cursor, next.sequence);
        store.prune_events_through(2).await.unwrap();
        let error = store
            .list_events(
                &hosts,
                &EventsListParams {
                    cursor: Some(0),
                    host: None,
                    kinds: Vec::new(),
                    limit: 100,
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("event cursor expired"));
    }

    #[tokio::test]
    async fn delivery_intent_retries_and_preserves_acknowledgement_stage() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store.db"))
            .await
            .unwrap();
        store
            .ensure_subscription(
                "fixture",
                &json!({"hosts":["host-a"]}),
                &json!({"url":"https://example.invalid/events"}),
                Some("fixture-sink"),
            )
            .await
            .unwrap();
        let event = store
            .ingest_alert(AlertEventInput {
                source: "alertmanager".into(),
                fingerprint: "delivery".into(),
                host: "host-a".into(),
                firing: true,
                occurred_at: maxops_proto::now(),
                payload: json!({"status":"firing"}),
            })
            .await
            .unwrap();
        let first = store
            .begin_delivery("fixture", event.sequence, 60)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.stage, DeliveryStage::Queued);
        assert_eq!(first.attempts, 1);
        assert!(
            store
                .begin_delivery("fixture", event.sequence, 60)
                .await
                .unwrap()
                .is_none()
        );
        store
            .acknowledge_delivery(
                "fixture",
                event.sequence,
                DeliveryStage::Accepted,
                Some(202),
            )
            .await
            .unwrap();
        assert_eq!(
            store.subscription_cursor("fixture").await.unwrap().cursor,
            event.sequence
        );
    }

    #[tokio::test]
    async fn remediation_claims_are_serial_and_budgeted_per_episode() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store.db"))
            .await
            .unwrap();
        let event = store
            .ingest_alert(AlertEventInput {
                source: "alertmanager".into(),
                fingerprint: "remediation".into(),
                host: "host-a".into(),
                firing: true,
                occurred_at: maxops_proto::now(),
                payload: json!({"status":"firing"}),
            })
            .await
            .unwrap();
        let claim_job = store
            .submit_job("remediation-job", &job(json!({"event_id":event.event_id})))
            .await
            .unwrap()
            .job;
        let first = store
            .begin_remediation(
                &event.event_id,
                "host-a",
                "automation-a",
                &claim_job.handle.job_id,
                2,
                0,
            )
            .await
            .unwrap();
        assert_eq!(first.attempt, 1);
        let replay = store
            .begin_remediation(
                &event.event_id,
                "host-a",
                "automation-a",
                &claim_job.handle.job_id,
                2,
                0,
            )
            .await
            .unwrap();
        assert_eq!(replay.remediation_id, first.remediation_id);
        let second_job = store
            .submit_job(
                "remediation-job-2",
                &job(json!({"event_id":event.event_id})),
            )
            .await
            .unwrap()
            .job;
        assert!(
            store
                .begin_remediation(
                    &event.event_id,
                    "host-a",
                    "automation-a",
                    &second_job.handle.job_id,
                    2,
                    0,
                )
                .await
                .is_err()
        );
        store
            .finish_remediation(
                &first.remediation_id,
                "automation-a",
                RemediationCompletion {
                    expected_revision: 1,
                    outcome: RemediationState::Failed,
                    related_job_id: Some(&claim_job.handle.job_id),
                    related_change_id: None,
                    summary: "first attempt failed",
                },
            )
            .await
            .unwrap();
        let second = store
            .begin_remediation(
                &event.event_id,
                "host-a",
                "automation-a",
                &second_job.handle.job_id,
                2,
                0,
            )
            .await
            .unwrap();
        store
            .finish_remediation(
                &second.remediation_id,
                "automation-a",
                RemediationCompletion {
                    expected_revision: 1,
                    outcome: RemediationState::Succeeded,
                    related_job_id: Some(&second_job.handle.job_id),
                    related_change_id: None,
                    summary: "second attempt succeeded",
                },
            )
            .await
            .unwrap();
        let third_job = store
            .submit_job(
                "remediation-job-3",
                &job(json!({"event_id":event.event_id})),
            )
            .await
            .unwrap()
            .job;
        let error = store
            .begin_remediation(
                &event.event_id,
                "host-a",
                "automation-a",
                &third_job.handle.job_id,
                2,
                0,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("budget exhausted"));
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
