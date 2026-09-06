use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The Git state observed while preparing a change. `commit` is optional when
/// the source of a manually built running system cannot be proven.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceBaseline {
    pub repository_id: String,
    pub reference: String,
    #[serde(default)]
    pub commit: Option<String>,
    #[schemars(with = "String")]
    pub observed_at: jiff::Timestamp,
    pub evidence: String,
}

/// The actual host/profile state observed while preparing or executing a
/// change. It is never inferred from maxops's last deployment record.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeBaseline {
    pub host: String,
    pub profile: String,
    #[serde(default)]
    pub running_closure: Option<String>,
    #[serde(default)]
    pub persistent_profile: Option<String>,
    #[serde(default)]
    pub generation: Option<u64>,
    #[serde(default)]
    pub boot_id: Option<String>,
    #[serde(default)]
    pub source_commit: Option<String>,
    #[schemars(with = "String")]
    pub observed_at: jiff::Timestamp,
    pub evidence: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangeIntent {
    pub repository_id: String,
    pub source_commit: String,
    pub target_host: String,
    pub target_profile: String,
    pub source_baseline: SourceBaseline,
    pub runtime_baseline: RuntimeBaseline,
    pub policy_version: String,
    pub parameters: serde_json::Value,
    #[schemars(with = "String")]
    pub created_at: jiff::Timestamp,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ChangeState {
    Prepared,
    Checking,
    Building,
    Publishing,
    Ready,
    Activating,
    Verifying,
    Recovering,
    Succeeded,
    RolledBack,
    RecoveryFailed,
    Stale,
    Superseded,
    OutcomeUnknown,
}
