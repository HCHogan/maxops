//! Bounded, transport-independent contracts for clients. Full records remain
//! available for audit; discovery and waiting never require response schemas.
use crate::{ChangeId, EventId, JobId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryResourceKind {
    #[default]
    Hosts,
    Units,
    Repositories,
    Deployments,
    ExecutionProfiles,
    DiagnosticProbes,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(transform = execution_profiles_require_host)]
pub struct ResourcesParams {
    /// Discovery namespace: hosts, units, repositories, deployments, execution_profiles or diagnostic_probes.
    #[serde(default)]
    pub kind: DiscoveryResourceKind,
    /// Exact permitted host from resources.list(kind=hosts); required for kind=execution_profiles, optional to filter other kinds.
    #[serde(default)]
    pub host: Option<String>,
    /// Continuation cursor returned by this operation; omit for the first page and preserve filters when continuing.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Maximum entries per page; use the returned cursor to continue.
    #[serde(default = "crate::default_job_limit")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u16,
}

fn execution_profiles_require_host(schema: &mut schemars::Schema) {
    schema.insert("if".into(), serde_json::json!({"properties":{"kind":{"const":"execution_profiles","description":"Execution-profile discovery requires an explicit host."}},"required":["kind"]}));
    schema.insert("then".into(), serde_json::json!({"required":["host"],"properties":{"host":{"type":"string","minLength":1,"description":"Exact permitted host from resources.list(kind=hosts)."}}}));
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EventGetParams {
    /// Event UUID from events.recent or events.list; links this operation to that evidence and episode.
    pub event_id: EventId,
    /// RFC 6901 pointer; empty selects the event, /payload selects its evidence.
    #[serde(default)]
    pub pointer: String,
    /// UTF-8 byte offset from next_offset; start at zero and continue at a character boundary.
    #[serde(default)]
    pub offset: u64,
    /// Maximum UTF-8 JSON fragment bytes (1..32768, default 8192); continue at next_offset.
    #[serde(default = "default_result_limit")]
    #[schemars(range(min = 1, max = 32768))]
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(transform = job_lookup_schema)]
pub struct JobWaitParams {
    /// Remote maxops job UUID from submission or jobs.list; never a consumer task number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<JobId>,
    /// Original submission Idempotency-Key scoped to this principal; supply exactly one of job_id or idempotency_key, once submission commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[!-~]+$"))]
    pub idempotency_key: Option<String>,
    /// Last observed job revision; return when it changes or the job is terminal. Omit to wait from the current revision.
    #[serde(default)]
    pub after_revision: Option<u64>,
    /// Maximum observation wait (0..10 seconds, default 10); timeout does not cancel the remote job.
    #[serde(default = "default_wait_seconds")]
    #[schemars(range(min = 0, max = 10))]
    pub timeout_seconds: u16,
}
pub fn default_wait_seconds() -> u16 {
    10
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobEventsParams {
    /// Remote maxops job UUID from submission or jobs.list; never a consumer task number.
    pub job_id: JobId,
    /// Last durable event sequence received; zero starts replay from the beginning.
    #[serde(default)]
    pub after_sequence: u64,
    /// Maximum entries per page; use the returned cursor to continue.
    #[serde(default = "crate::default_job_limit")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(transform = job_lookup_schema)]
pub struct JobResultParams {
    /// Remote maxops job UUID from submission or jobs.list; never a consumer task number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<JobId>,
    /// Original submission Idempotency-Key scoped to this principal; supply exactly one of job_id or idempotency_key, once submission commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[!-~]+$"))]
    pub idempotency_key: Option<String>,
    /// RFC 6901 pointer into the stored result; empty selects the root.
    #[serde(default)]
    pub pointer: String,
    /// UTF-8 byte offset from next_offset; start at zero and continue at a character boundary.
    #[serde(default)]
    pub offset: u64,
    /// Maximum UTF-8 JSON fragment bytes (1..32768, default 8192); continue at next_offset.
    #[serde(default = "default_result_limit")]
    #[schemars(range(min = 1, max = 32768))]
    pub limit: u32,
}
pub fn default_result_limit() -> u32 {
    8192
}

#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum DeployUntil {
    Built,
    Verified,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployRunParams {
    /// Change UUID returned by deploy.prepare or changes.history.
    pub change_id: ChangeId,
    /// Current change revision from changes.status; rejects a stale workflow baseline.
    pub expected_revision: u64,
    /// Stop after built (artifact only) or verified (activation and target-owned acceptance).
    pub until: DeployUntil,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
pub struct ExecutionProfileInfo {
    pub name: String,
    pub max_timeout_seconds: u32,
    pub output_limit_bytes: u64,
    pub user: String,
    pub privileged: bool,
    pub interpreter: String,
    pub working_roots: Vec<String>,
    /// Declared executable search path; login-shell environments are not inherited.
    pub path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AlertsParams {
    /// Exact host from resources.list(kind=hosts); omit for all permitted hosts.
    #[serde(default)]
    pub host: Option<String>,
    /// Maximum alerts per page before grouping, 1..200. Summary defaults to 20 alerts; full without bounds preserves the legacy response.
    #[serde(default)]
    #[schemars(range(min = 1, max = 200))]
    pub limit: Option<u16>,
    /// Revision-bound next_cursor from the same host filter and view; restart listing if the alert set changes.
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, utoipa::ToSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum MetricAggregation {
    #[default]
    None,
    Stats,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HostMetricsParams {
    /// Exact permitted host from resources.list(kind=hosts).
    pub host: String,
    /// none retains individual series; stats reduces each metric to min/max/mean and series counts over fresh unambiguous samples. Summary view always uses stats.
    #[serde(default)]
    pub aggregation: MetricAggregation,
}

/// Shared schema constraint for the four public job reads. Executor requests
/// continue to use UUIDs only; idempotency receipts are owned by the Hub.
pub fn job_lookup_schema(schema: &mut schemars::Schema) {
    schema.insert(
        "oneOf".into(),
        serde_json::json!([
            {"required":["job_id"],"not":{"required":["idempotency_key"]}},
            {"required":["idempotency_key"],"not":{"required":["job_id"]}}
        ]),
    );
    // Option fields are optional, but a supplied identifier must not be null.
    if let Some(properties) = schema
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
    {
        for name in ["job_id", "idempotency_key"] {
            if let Some(field) = properties.get_mut(name) {
                field["type"] = serde_json::json!("string");
                field.as_object_mut().unwrap().remove("default");
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(transform = job_lookup_schema)]
pub struct JobLookupParams {
    /// Remote maxops job UUID; supply exactly one of job_id or idempotency_key. Never a consumer task number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<JobId>,
    /// Original submission Idempotency-Key (1..128 printable ASCII characters without spaces), scoped to this authenticated principal. Supply instead of job_id; available once submission commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[!-~]+$"))]
    pub idempotency_key: Option<String>,
}

impl From<JobId> for JobLookupParams {
    fn from(job_id: JobId) -> Self {
        Self {
            job_id: Some(job_id),
            idempotency_key: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(transform = job_lookup_schema)]
pub struct JobOutputParams {
    /// Remote maxops job UUID; supply exactly one of job_id or idempotency_key. Never a consumer task number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<JobId>,
    /// Original submission Idempotency-Key scoped to this authenticated principal; supply instead of job_id, once submission commits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 128), regex(pattern = r"^[!-~]+$"))]
    pub idempotency_key: Option<String>,
    /// Byte offset from next_stdout_offset; zero starts at the beginning of stdout.
    #[serde(default)]
    pub stdout_offset: u64,
    /// Byte offset from next_stderr_offset; zero starts at the beginning of stderr.
    #[serde(default)]
    pub stderr_offset: u64,
    /// Maximum bytes per stream (1..65536, default 65536); continue using returned offsets.
    #[serde(default = "crate::default_log_limit")]
    #[schemars(range(min = 1, max = 65536))]
    pub limit: u32,
}

impl From<crate::JobLogsParams> for JobOutputParams {
    fn from(params: crate::JobLogsParams) -> Self {
        Self {
            job_id: Some(params.job_id),
            idempotency_key: None,
            stdout_offset: params.stdout_offset,
            stderr_offset: params.stderr_offset,
            limit: params.limit,
        }
    }
}
