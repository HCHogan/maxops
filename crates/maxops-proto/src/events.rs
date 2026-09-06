use crate::{JobId, JobState};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobEventKind {
    Submitted,
    StateChanged,
    CancelRequested,
    Reconciled,
}

impl JobEventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::StateChanged => "state_changed",
            Self::CancelRequested => "cancel_requested",
            Self::Reconciled => "reconciled",
        }
    }

    pub fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "submitted" => Ok(Self::Submitted),
            "state_changed" => Ok(Self::StateChanged),
            "cancel_requested" => Ok(Self::CancelRequested),
            "reconciled" => Ok(Self::Reconciled),
            _ => Err("invalid job event kind"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
pub struct JobEvent {
    pub sequence: u64,
    pub job_id: JobId,
    pub kind: JobEventKind,
    pub state: JobState,
    #[schemars(with = "String")]
    pub occurred_at: jiff::Timestamp,
    pub payload: serde_json::Value,
}
