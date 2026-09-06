use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize, utoipa::ToSchema,
)]
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
            (Queued, Dispatching | Cancelled)
                | (
                    Dispatching,
                    Running | Reconciling | Failed | Cancelled | TimedOut
                )
                | (
                    Running,
                    Reconciling | Succeeded | Failed | Cancelled | TimedOut
                )
                | (
                    Reconciling,
                    Succeeded | Failed | Cancelled | TimedOut | OutcomeUnknown
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
