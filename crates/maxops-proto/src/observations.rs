use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
pub struct Facts {
    pub kernel: String,
    pub uptime_seconds: f64,
    #[serde(default)]
    pub boot_id: Option<String>,
    pub system_closure: Option<String>,
    #[serde(default)]
    pub system_profile: Option<String>,
    #[serde(default)]
    pub profile_generation: Option<u64>,
    #[serde(default)]
    pub profile_matches_running: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
pub struct UnitStatus {
    pub unit: String,
    pub description: String,
    pub load_state: String,
    pub active_state: String,
    pub sub_state: String,
    #[serde(default)]
    pub details: Option<UnitDetails>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
pub struct UnitDetails {
    pub main_pid: Option<u32>,
    pub memory_current_bytes: Option<u64>,
    pub restarts: Option<u32>,
    pub exec_main_code: Option<i32>,
    pub exec_main_status: Option<i32>,
    #[serde(default)]
    pub invocation_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
pub struct UnitObservation {
    pub host: String,
    #[schemars(with = "String")]
    pub observed_at: jiff::Timestamp,
    pub unit: UnitStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
pub struct Snapshot {
    pub host: String,
    /// Agent collection time in UTC. Not deployment time.
    #[schemars(with = "String")]
    pub observed_at: jiff::Timestamp,
    pub facts: Facts,
    pub units: Vec<UnitStatus>,
    /// False for older agents and explicit allowlists. True covers loaded units,
    /// not every installed unit file.
    #[serde(default)]
    pub read_all_units: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
pub struct LogEntry {
    pub timestamp_us: Option<String>,
    pub priority: Option<String>,
    pub message: serde_json::Value,
}

/// A current external fact. This is deliberately separate from maxops's own
/// deployment and job history because other fleet writers may change it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceObservation {
    pub resource_kind: ResourceKind,
    pub resource_key: String,
    #[schemars(with = "String")]
    pub observed_at: jiff::Timestamp,
    pub value: serde_json::Value,
    pub evidence: String,
    #[serde(default)]
    pub related_change_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    GitRef,
    RunningSystem,
    PersistentProfile,
    HomeProfile,
    Unit,
}

impl ResourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GitRef => "git_ref",
            Self::RunningSystem => "running_system",
            Self::PersistentProfile => "persistent_profile",
            Self::HomeProfile => "home_profile",
            Self::Unit => "unit",
        }
    }

    pub fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "git_ref" => Ok(Self::GitRef),
            "running_system" => Ok(Self::RunningSystem),
            "persistent_profile" => Ok(Self::PersistentProfile),
            "home_profile" => Ok(Self::HomeProfile),
            "unit" => Ok(Self::Unit),
            _ => Err("invalid observation resource kind"),
        }
    }
}
