//! Bounded, transport-independent contracts for clients. Full records remain
//! available for audit; discovery and waiting never require response schemas.
use crate::{ChangeId, JobId};
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
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourcesParams {
    #[serde(default)]
    pub kind: DiscoveryResourceKind,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default = "crate::default_job_limit")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobWaitParams {
    pub job_id: JobId,
    #[serde(default)]
    pub after_revision: Option<u64>,
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
    pub job_id: JobId,
    #[serde(default)]
    pub after_sequence: u64,
    #[serde(default = "crate::default_job_limit")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobResultParams {
    pub job_id: JobId,
    /// RFC 6901 pointer into the stored result; empty selects the root.
    #[serde(default)]
    pub pointer: String,
    #[serde(default)]
    pub offset: u64,
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
    pub change_id: ChangeId,
    pub expected_revision: u64,
    pub until: DeployUntil,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
pub struct ExecutionProfileInfo {
    pub name: String,
    pub max_timeout_seconds: u32,
    pub output_limit_bytes: u64,
}
