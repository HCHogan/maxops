//! Versioned, read-only operations shared by every frontend.
pub mod transport;

use schemars::{JsonSchema, Schema, schema_for};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Empty {}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HostParams {
    pub host: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct FailedParams {
    #[serde(default)]
    pub host: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UnitParams {
    pub host: String,
    pub unit: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct LogParams {
    pub host: String,
    pub unit: String,
    #[serde(default = "default_lines")]
    #[schemars(range(min = 1, max = 200))]
    #[schema(minimum = 1, maximum = 200, default = 50)]
    pub lines: u16,
    #[serde(default = "default_since")]
    #[schemars(range(min = 1, max = 86400))]
    #[schema(minimum = 1, maximum = 86400, default = 3600)]
    pub since_seconds: u32,
}

pub fn default_lines() -> u16 {
    50
}
pub fn default_since() -> u32 {
    3600
}

impl LogParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=200).contains(&self.lines) || !(1..=86400).contains(&self.since_seconds) {
            return Err("logs require 1..200 lines and 1..86400 since_seconds");
        }
        if !valid_unit(&self.unit) {
            return Err("invalid service unit name");
        }
        Ok(())
    }
}

#[derive(Clone, Serialize)]
pub struct Operation {
    pub name: &'static str,
    pub summary: &'static str,
    pub capability: &'static str,
    pub read_only: bool,
    pub params_schema: Schema,
}

// Keep wire names, capability names, schemas and CLI discovery together.
// `tt` keeps the wire-name literal visible to utoipa's serde attribute parser.
macro_rules! operations {
    ($( $variant:ident($params:ty), $name:tt, $cap:literal, $summary:literal; )*) => {
        #[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
        #[serde(tag = "op", content = "params", deny_unknown_fields)]
        pub enum Request {
            $(#[serde(rename = $name)] $variant($params),)*
        }
        impl Request {
            pub fn capability(&self) -> &'static str {
                match self { $(Self::$variant(_) => $cap,)* }
            }
            pub fn name(&self) -> &'static str {
                match self { $(Self::$variant(_) => $name,)* }
            }
        }
        pub fn operations() -> Vec<Operation> {
            vec![$(Operation {
                name: $name, summary: $summary, capability: $cap, read_only: true,
                params_schema: schema_for!($params),
            },)*]
        }
    }
}

operations! {
    FleetOverview(Empty), "fleet.overview", "fleet:read", "Observed agent and exporter state for permitted hosts";
    UnitsFailed(FailedParams), "units.failed", "units:read", "Failed readable services; unreachable hosts remain explicit";
    HostFacts(HostParams), "host.facts", "host:read", "Kernel, uptime and the running system closure";
    HostMetrics(HostParams), "host.metrics", "metrics:read", "Host-scoped CPU, memory, load, filesystem and network observations from Prometheus";
    DeployStatus(FailedParams), "deploy.status", "host:read", "Running closure versus persistent system profile; unavailable hosts remain explicit";
    UnitsList(HostParams), "units.list", "units:read", "All explicitly readable services, including unloaded services";
    UnitsStatus(UnitParams), "units.status", "units:read", "State of one explicitly readable service";
    UnitsLogs(LogParams), "units.logs", "logs:read", "Bounded recent journal entries for one readable service";
    AlertsActive(Empty), "alerts.active", "alerts:read", "Active alerts with an instance label matching permitted hosts";
}

pub fn valid_unit(name: &str) -> bool {
    name.len() <= 255
        && name.ends_with(".service")
        && name.len() > 8
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:@".contains(&b))
}

pub fn valid_host(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}

#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct Facts {
    pub kernel: String,
    pub uptime_seconds: f64,
    pub system_closure: Option<String>,
    #[serde(default)]
    pub system_profile: Option<String>,
    #[serde(default)]
    pub profile_generation: Option<u64>,
    #[serde(default)]
    pub profile_matches_running: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct UnitStatus {
    pub unit: String,
    pub description: String,
    pub load_state: String,
    pub active_state: String,
    pub sub_state: String,
    #[serde(default)]
    pub details: Option<UnitDetails>,
}

#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct UnitDetails {
    pub main_pid: Option<u32>,
    pub memory_current_bytes: Option<u64>,
    pub restarts: Option<u32>,
    pub exec_main_code: Option<i32>,
    pub exec_main_status: Option<i32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct UnitObservation {
    pub host: String,
    pub observed_at: jiff::Timestamp,
    pub unit: UnitStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct Snapshot {
    pub host: String,
    /// Agent collection time in UTC. Not deployment time.
    pub observed_at: jiff::Timestamp,
    pub facts: Facts,
    pub units: Vec<UnitStatus>,
}

#[derive(Clone, Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct LogEntry {
    pub timestamp_us: Option<String>,
    pub priority: Option<String>,
    pub message: serde_json::Value,
}

pub fn now() -> jiff::Timestamp {
    jiff::Timestamp::now()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requests_fail_closed() {
        for value in [
            r#"{"op":"units.restart","params":{"host":"a","unit":"a.service"}}"#,
            r#"{"op":"host.facts","params":{"host":"a","uid":"admin"}}"#,
            r#"{"op":"host.facts","params":{"host":"a"},"confirmed":true}"#,
        ] {
            assert!(serde_json::from_str::<Request>(value).is_err());
        }
    }
    #[test]
    fn log_limits_and_unit_syntax() {
        let mut p = LogParams {
            host: "a".into(),
            unit: "a.service".into(),
            lines: 200,
            since_seconds: 86400,
        };
        assert!(p.validate().is_ok());
        p.lines = 201;
        assert!(p.validate().is_err());
        for unit in [
            "--help",
            "*.service",
            "../a.service",
            "a.service;reboot",
            "a.timer",
        ] {
            assert!(!valid_unit(unit));
        }
        assert!(valid_unit("worker@one.service"));
    }
}
