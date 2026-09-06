use crate::{WorkspaceTargetRequest, WorkspaceTargetResponse};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

#[derive(Clone, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct JobId(String);

impl JobId {
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
            Err("invalid job ID")
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<'de> Deserialize<'de> for JobId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Dispatching,
    Running,
    Reconciling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    OutcomeUnknown,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Dispatching => "dispatching",
            Self::Running => "running",
            Self::Reconciling => "reconciling",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Failed
                | Self::Cancelled
                | Self::TimedOut
                | Self::OutcomeUnknown
        )
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        use JobState::*;
        matches!(
            (self, next),
            (Queued, Dispatching | Failed | Cancelled | TimedOut)
                | (
                    Dispatching,
                    Running | Reconciling | Succeeded | Failed | Cancelled | TimedOut
                )
                | (
                    Running,
                    Reconciling | Succeeded | Failed | Cancelled | TimedOut
                )
                | (
                    Reconciling,
                    Running | Succeeded | Failed | Cancelled | TimedOut | OutcomeUnknown
                )
                | (OutcomeUnknown, Succeeded | Failed | Cancelled | TimedOut)
        )
    }
}

impl FromStr for JobState {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "dispatching" => Ok(Self::Dispatching),
            "running" => Ok(Self::Running),
            "reconciling" => Ok(Self::Reconciling),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            "outcome_unknown" => Ok(Self::OutcomeUnknown),
            _ => Err("invalid job state"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewJob {
    pub principal: String,
    pub host: String,
    pub operation: String,
    pub spec_version: u32,
    pub spec: serde_json::Value,
    pub policy_version: String,
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub deadline: Option<jiff::Timestamp>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
pub struct JobHandle {
    pub job_id: JobId,
    pub state: JobState,
    pub revision: u64,
    pub operation: String,
    pub host: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
pub struct JobRecord {
    pub handle: JobHandle,
    pub principal: String,
    pub spec_version: u32,
    pub spec: serde_json::Value,
    pub spec_hash: String,
    pub policy_version: String,
    #[schemars(with = "String")]
    pub created_at: jiff::Timestamp,
    #[schemars(with = "String")]
    pub updated_at: jiff::Timestamp,
    #[schemars(with = "Option<String>")]
    pub deadline: Option<jiff::Timestamp>,
    pub cancel_requested: bool,
    pub result: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CommandSpec {
    Argv(Vec<String>),
    Script(String),
}

impl CommandSpec {
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Argv(argv)
                if !argv.is_empty()
                    && argv.len() <= 256
                    && argv.iter().all(|value| value.len() <= 16 * 1024) =>
            {
                Ok(())
            }
            Self::Script(script) if !script.is_empty() && script.len() <= 64 * 1024 => Ok(()),
            Self::Argv(_) => Err("argv must contain 1..256 bounded arguments"),
            Self::Script(_) => Err("script must contain 1..65536 bytes"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecRunParams {
    pub host: String,
    pub profile: String,
    pub command: CommandSpec,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub credential_refs: Vec<String>,
    #[serde(default)]
    pub timeout_seconds: Option<u32>,
}

impl ExecRunParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.command.validate()?;
        if !crate::valid_host(&self.host) {
            return Err("invalid host");
        }
        if self.profile.is_empty()
            || self.profile.len() > 64
            || !self
                .profile
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err("invalid execution profile");
        }
        if self.env.len() > 128
            || self.env.iter().any(|(key, value)| {
                key.is_empty()
                    || key.len() > 256
                    || value.len() > 16 * 1024
                    || !key
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            })
        {
            return Err("invalid environment");
        }
        if self.credential_refs.len() > 32
            || self.credential_refs.iter().collect::<BTreeSet<_>>().len()
                != self.credential_refs.len()
            || self.credential_refs.iter().any(|value| {
                value.is_empty()
                    || value.len() > 128
                    || value == "spec"
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            })
        {
            return Err("credential references must be unique valid names");
        }
        if self.timeout_seconds.is_some_and(|value| value == 0) {
            return Err("timeout must be positive");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobsListParams {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub states: Vec<JobState>,
    #[serde(default = "default_job_limit")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u16,
}

pub fn default_job_limit() -> u16 {
    50
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobIdParams {
    pub job_id: JobId,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobLogsParams {
    pub job_id: JobId,
    #[serde(default)]
    pub stdout_offset: u64,
    #[serde(default)]
    pub stderr_offset: u64,
    #[serde(default = "default_log_limit")]
    #[schemars(range(min = 1, max = 65536))]
    pub limit: u32,
}

pub fn default_log_limit() -> u32 {
    64 * 1024
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobCancelParams {
    pub job_id: JobId,
    pub expected_revision: u64,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
pub struct JobsListResponse {
    pub jobs: Vec<JobRecord>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
pub struct JobLogsResponse {
    pub job_id: JobId,
    pub encoding: String,
    pub stdout_base64: String,
    pub stderr_base64: String,
    pub next_stdout_offset: u64,
    pub next_stderr_offset: u64,
    pub complete: bool,
    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(tag = "action", content = "params", rename_all = "snake_case")]
pub enum ExecutorRequest {
    Submit {
        job_id: JobId,
        job: NewJob,
    },
    Status(JobIdParams),
    Logs(JobLogsParams),
    Cancel(JobCancelParams),
    Workspace {
        principal: String,
        request: WorkspaceTargetRequest,
    },
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ExecutorResponse {
    Job(JobRecord),
    Logs(JobLogsResponse),
    Workspace(WorkspaceTargetResponse),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ExecutorWireResponse {
    Ok { response: Box<ExecutorResponse> },
    Error { code: String, message: String },
}
