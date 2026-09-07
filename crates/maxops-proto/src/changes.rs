use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Clone, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct ChangeId(String);

impl ChangeId {
    pub fn parse(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = value.into();
        if value.len() == 36
            && value.bytes().enumerate().all(|(index, byte)| match index {
                8 | 13 | 18 | 23 => byte == b'-',
                _ => byte.is_ascii_hexdigit(),
            })
        {
            Ok(Self(value))
        } else {
            Err("invalid change ID")
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChangeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<'de> Deserialize<'de> for ChangeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

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
    Failed,
    Stale,
    Superseded,
    OutcomeUnknown,
}

impl ChangeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Checking => "checking",
            Self::Building => "building",
            Self::Publishing => "publishing",
            Self::Ready => "ready",
            Self::Activating => "activating",
            Self::Verifying => "verifying",
            Self::Recovering => "recovering",
            Self::Succeeded => "succeeded",
            Self::RolledBack => "rolled_back",
            Self::RecoveryFailed => "recovery_failed",
            Self::Failed => "failed",
            Self::Stale => "stale",
            Self::Superseded => "superseded",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::RolledBack
                | Self::RecoveryFailed
                | Self::Failed
                | Self::Stale
                | Self::Superseded
                | Self::OutcomeUnknown
        )
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        use ChangeState::*;
        matches!(
            (self, next),
            (Checking, Prepared | Failed | Stale | OutcomeUnknown)
                | (Prepared, Building | Stale | Failed)
                | (
                    Building,
                    Publishing | Ready | Failed | Stale | OutcomeUnknown
                )
                | (Publishing, Ready | Failed | Stale | OutcomeUnknown)
                | (Ready, Activating | Stale | Failed)
                | (
                    Activating,
                    Verifying
                        | RolledBack
                        | RecoveryFailed
                        | Failed
                        | Stale
                        | Superseded
                        | OutcomeUnknown
                )
                | (
                    Verifying,
                    Succeeded
                        | Recovering
                        | RolledBack
                        | RecoveryFailed
                        | Failed
                        | Superseded
                        | OutcomeUnknown
                )
                | (Succeeded, Recovering)
                | (
                    Recovering,
                    RolledBack | RecoveryFailed | Superseded | OutcomeUnknown
                )
        )
    }
}

impl FromStr for ChangeState {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "checking" => Ok(Self::Checking),
            "building" => Ok(Self::Building),
            "publishing" => Ok(Self::Publishing),
            "ready" => Ok(Self::Ready),
            "activating" => Ok(Self::Activating),
            "verifying" => Ok(Self::Verifying),
            "recovering" => Ok(Self::Recovering),
            "succeeded" => Ok(Self::Succeeded),
            "rolled_back" => Ok(Self::RolledBack),
            "recovery_failed" => Ok(Self::RecoveryFailed),
            "failed" => Ok(Self::Failed),
            "stale" => Ok(Self::Stale),
            "superseded" => Ok(Self::Superseded),
            "outcome_unknown" => Ok(Self::OutcomeUnknown),
            _ => Err("invalid change state"),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentKind {
    System,
    Home,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangePlan {
    pub change_id: ChangeId,
    pub repository: String,
    pub workspace_id: crate::WorkspaceId,
    pub workspace_revision: u64,
    pub tree_hash: String,
    pub source_commit: String,
    pub source_reference: String,
    pub source_remote_head: String,
    pub target_host: String,
    pub deployment_profile: String,
    pub kind: DeploymentKind,
    pub flake_attribute: String,
    #[serde(default)]
    pub drv_path: Option<String>,
    #[serde(default)]
    pub lock_digest: Option<String>,
    pub source_baseline: SourceBaseline,
    pub runtime_baseline: RuntimeBaseline,
    pub policy_version: String,
    #[schemars(with = "String")]
    pub created_at: jiff::Timestamp,
    #[schemars(with = "String")]
    pub expires_at: jiff::Timestamp,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeploymentArtifact {
    pub builder_host: String,
    pub source_commit: String,
    pub tree_hash: String,
    pub lock_digest: String,
    pub drv_path: String,
    pub out_path: String,
    #[schemars(with = "String")]
    pub built_at: jiff::Timestamp,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ChangeJobs {
    pub prepare: Option<crate::JobId>,
    pub build: Option<crate::JobId>,
    pub activate: Option<crate::JobId>,
    pub verify: Option<crate::JobId>,
    pub rollback: Option<crate::JobId>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangeRecord {
    pub plan: ChangePlan,
    pub creator: String,
    pub revision: u64,
    pub state: ChangeState,
    pub artifact: Option<DeploymentArtifact>,
    pub jobs: ChangeJobs,
    pub recovery_state: Option<String>,
    #[schemars(with = "String")]
    pub updated_at: jiff::Timestamp,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployPrepareParams {
    pub repository: String,
    pub workspace_id: crate::WorkspaceId,
    pub expected_revision: u64,
    pub target_host: String,
    pub profile: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployChangeParams {
    pub change_id: ChangeId,
    pub expected_revision: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangeStatusParams {
    pub change_id: ChangeId,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangeHistoryParams {
    #[serde(default)]
    pub cursor: Option<ChangeId>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "default_change_limit")]
    #[schemars(range(min = 1, max = 200))]
    #[schema(minimum = 1, maximum = 200, default = 50)]
    pub limit: u16,
}

pub const fn default_change_limit() -> u16 {
    50
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangeHistoryResponse {
    pub changes: Vec<ChangeRecord>,
    #[serde(default)]
    pub next_cursor: Option<ChangeId>,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentAction {
    Prepare,
    Build,
    Activate,
    Verify,
    Rollback,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeploymentJobSpec {
    pub action: DeploymentAction,
    pub plan: ChangePlan,
    pub artifact: Option<DeploymentArtifact>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeploymentRunnerSpec {
    pub job: DeploymentJobSpec,
    pub nix: String,
    pub nix_env: String,
    pub profile_path: String,
    pub running_link: String,
    pub activation_program: String,
    pub activation_arguments: Vec<String>,
    #[serde(default)]
    pub artifact_source: Option<String>,
    #[serde(default)]
    pub verify_commands: Vec<Vec<String>>,
    pub verify_attempts: u16,
    pub verify_interval_seconds: u32,
    pub automatic_rollback: bool,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentReportStatus {
    Prepared,
    Built,
    Activated,
    Verified,
    RolledBack,
    Stale,
    Superseded,
    RecoveryFailed,
    OutcomeUnknown,
    Failed,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeploymentReport {
    pub action: DeploymentAction,
    pub status: DeploymentReportStatus,
    #[serde(default)]
    pub drv_path: Option<String>,
    #[serde(default)]
    pub out_path: Option<String>,
    #[serde(default)]
    pub lock_digest: Option<String>,
    #[serde(default)]
    pub observed_running: Option<String>,
    #[serde(default)]
    pub observed_profile: Option<String>,
    pub detail: String,
    #[schemars(with = "String")]
    pub completed_at: jiff::Timestamp,
}

impl DeployPrepareParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !crate::valid_repository_id(&self.repository)
            || self.expected_revision == 0
            || !crate::valid_host(&self.target_host)
            || !crate::valid_check_id(&self.profile)
        {
            return Err("invalid deployment preparation request");
        }
        Ok(())
    }
}

impl DeployChangeParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.expected_revision == 0 {
            Err("change revision must be positive")
        } else {
            Ok(())
        }
    }
}

impl ChangeHistoryParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=200).contains(&self.limit)
            || self
                .host
                .as_deref()
                .is_some_and(|host| !crate::valid_host(host))
        {
            Err("invalid change history request")
        } else {
            Ok(())
        }
    }
}
