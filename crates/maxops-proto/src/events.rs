use crate::{ChangeId, JobId, JobState, default_lines, default_since, valid_host, valid_unit};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! opaque_uuid {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize, utoipa::ToSchema)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
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
                    Err(concat!("invalid ", $label))
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

opaque_uuid!(EventId, "event ID");
opaque_uuid!(EpisodeId, "episode ID");
opaque_uuid!(RemediationId, "remediation ID");

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

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    AlertFiring,
    AlertResolved,
    DiagnosticCollected,
    RemediationStarted,
    RemediationFinished,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlertFiring => "alert_firing",
            Self::AlertResolved => "alert_resolved",
            Self::DiagnosticCollected => "diagnostic_collected",
            Self::RemediationStarted => "remediation_started",
            Self::RemediationFinished => "remediation_finished",
        }
    }

    pub fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "alert_firing" => Ok(Self::AlertFiring),
            "alert_resolved" => Ok(Self::AlertResolved),
            "diagnostic_collected" => Ok(Self::DiagnosticCollected),
            "remediation_started" => Ok(Self::RemediationStarted),
            "remediation_finished" => Ok(Self::RemediationFinished),
            _ => Err("invalid event kind"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EventRecord {
    pub sequence: u64,
    pub event_id: EventId,
    pub source: String,
    pub fingerprint: String,
    pub episode_id: EpisodeId,
    pub kind: EventKind,
    pub host: String,
    #[schemars(with = "String")]
    pub occurred_at: jiff::Timestamp,
    #[schemars(with = "String")]
    pub received_at: jiff::Timestamp,
    #[serde(default)]
    pub related_job_id: Option<JobId>,
    #[serde(default)]
    pub related_change_id: Option<ChangeId>,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EventsListParams {
    #[serde(default)]
    pub cursor: Option<u64>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub kinds: Vec<EventKind>,
    #[serde(default = "default_event_limit")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u16,
}

pub fn default_event_limit() -> u16 {
    100
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RecentEventsParams {
    #[serde(default)]
    pub host: Option<String>,
    /// Exact systemd unit; matches unit/name labels or a diagnostic unit.
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default = "crate::default_since")]
    #[schemars(range(min = 1, max = 604800))]
    pub since_seconds: u32,
    #[serde(default)]
    pub before_sequence: Option<u64>,
    #[serde(default = "default_recent_limit")]
    #[schemars(range(min = 1, max = 50))]
    pub limit: u16,
}

pub fn default_recent_limit() -> u16 {
    20
}

impl RecentEventsParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=50).contains(&self.limit) || !(1..=604800).contains(&self.since_seconds) {
            return Err("recent events require 1..50 entries and 1..604800 since_seconds");
        }
        if self.host.as_ref().is_some_and(|host| !valid_host(host)) {
            return Err("invalid host");
        }
        if self
            .unit
            .as_ref()
            .is_some_and(|unit| !crate::valid_observation_unit(unit))
        {
            return Err("invalid observation unit name");
        }
        Ok(())
    }
}

impl EventsListParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=200).contains(&self.limit) {
            return Err("event list limit must be 1..200");
        }
        if self.host.as_ref().is_some_and(|host| !valid_host(host)) {
            return Err("invalid host");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EventsListResponse {
    pub events: Vec<EventRecord>,
    pub next_cursor: u64,
    pub earliest_cursor: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticCollectParams {
    pub host: String,
    #[serde(default)]
    pub event_id: Option<EventId>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default = "default_lines")]
    #[schemars(range(min = 1, max = 200))]
    pub lines: u16,
    #[serde(default = "default_since")]
    #[schemars(range(min = 1, max = 86400))]
    pub since_seconds: u32,
    #[serde(default)]
    pub probes: Vec<String>,
}

impl DiagnosticCollectParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !valid_host(&self.host) {
            return Err("invalid host");
        }
        if self.unit.as_ref().is_some_and(|unit| !valid_unit(unit)) {
            return Err("invalid service unit name");
        }
        if !(1..=200).contains(&self.lines) || !(1..=86400).contains(&self.since_seconds) {
            return Err("diagnostics require 1..200 lines and 1..86400 since_seconds");
        }
        if self.probes.len() > 16
            || self.probes.iter().any(|probe| {
                probe.is_empty()
                    || probe.len() > 128
                    || !probe
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(&byte))
            })
        {
            return Err("invalid diagnostic probe list");
        }
        let mut unique = self.probes.clone();
        unique.sort();
        unique.dedup();
        if unique.len() != self.probes.len() {
            return Err("diagnostic probes must be unique");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceAssessment {
    Fact,
    Hypothesis,
    Missing,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticEvidence {
    pub evidence_id: String,
    pub source: String,
    pub assessment: EvidenceAssessment,
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticRuleResult {
    pub rule_id: String,
    pub confidence: String,
    pub evidence_ids: Vec<String>,
    pub conclusion: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticBundle {
    pub artifact_id: String,
    pub host: String,
    pub unit: Option<String>,
    #[schemars(with = "String")]
    pub collected_at: jiff::Timestamp,
    pub evidence: Vec<DiagnosticEvidence>,
    pub rules: Vec<DiagnosticRuleResult>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RemediationBeginParams {
    pub event_id: EventId,
    pub host: String,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RemediationState {
    Active,
    Succeeded,
    Failed,
    NoAction,
    Superseded,
}

impl RemediationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::NoAction => "no_action",
            Self::Superseded => "superseded",
        }
    }

    pub fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "active" => Ok(Self::Active),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "no_action" => Ok(Self::NoAction),
            "superseded" => Ok(Self::Superseded),
            _ => Err("invalid remediation state"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RemediationFinishParams {
    pub remediation_id: RemediationId,
    pub expected_revision: u64,
    pub outcome: RemediationState,
    #[serde(default)]
    pub related_job_id: Option<JobId>,
    #[serde(default)]
    pub related_change_id: Option<ChangeId>,
    pub summary: String,
}

impl RemediationFinishParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.outcome == RemediationState::Active {
            return Err("remediation outcome must be terminal");
        }
        if self.summary.is_empty() || self.summary.len() > 1024 {
            return Err("remediation summary must be 1..1024 bytes");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RemediationRecord {
    pub remediation_id: RemediationId,
    pub event_id: EventId,
    pub episode_id: EpisodeId,
    pub host: String,
    pub principal: String,
    pub attempt: u16,
    pub revision: u64,
    pub state: RemediationState,
    #[schemars(with = "String")]
    pub started_at: jiff::Timestamp,
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub finished_at: Option<jiff::Timestamp>,
    #[serde(default)]
    pub related_job_id: Option<JobId>,
    #[serde(default)]
    pub related_change_id: Option<ChangeId>,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStage {
    Queued,
    Accepted,
    Confirmed,
}

impl DeliveryStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Accepted => "accepted",
            Self::Confirmed => "confirmed",
        }
    }

    pub fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "queued" => Ok(Self::Queued),
            "accepted" => Ok(Self::Accepted),
            "confirmed" => Ok(Self::Confirmed),
            _ => Err("invalid delivery stage"),
        }
    }
}
