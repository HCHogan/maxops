//! Versioned operations and durable execution types shared by every frontend.
pub mod changes;
pub mod events;
pub mod jobs;
pub mod observations;
pub mod transport;
pub mod workspaces;

pub use changes::*;
pub use events::*;
pub use jobs::*;
pub use observations::*;
pub use workspaces::*;

use schemars::{JsonSchema, Schema, schema_for};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 2;

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
pub struct UnitActionParams {
    pub host: String,
    pub unit: String,
    #[serde(default)]
    pub expected_invocation_id: Option<String>,
}

impl UnitActionParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !valid_host(&self.host) {
            return Err("invalid host");
        }
        if !valid_unit(&self.unit) {
            return Err("invalid service unit name");
        }
        if self.expected_invocation_id.as_ref().is_some_and(|value| {
            value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        }) {
            return Err("expected invocation ID must contain 32 hexadecimal digits");
        }
        Ok(())
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Observation,
    JobSubmission,
    JobControl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyRequirement {
    None,
    Required,
}

#[derive(Clone, Serialize)]
pub struct Operation {
    pub name: &'static str,
    pub summary: &'static str,
    pub capability: &'static str,
    pub kind: OperationKind,
    pub read_only: bool,
    pub minimum_protocol_version: u16,
    pub idempotency: IdempotencyRequirement,
    pub params_schema: Schema,
    pub response_schema: Schema,
}

// Keep wire names, capability names, schemas and CLI discovery together.
// `tt` keeps the wire-name literal visible to utoipa's serde attribute parser.
macro_rules! operations {
    ($( $variant:ident($params:ty) -> $response:ty, $name:tt, $cap:literal, $kind:expr, $read_only:expr, $idempotency:expr, $summary:literal; )*) => {
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
                name: $name,
                summary: $summary,
                capability: $cap,
                kind: $kind,
                read_only: $read_only,
                minimum_protocol_version: match $kind {
                    OperationKind::Observation => 1,
                    OperationKind::JobSubmission | OperationKind::JobControl => 2,
                },
                idempotency: $idempotency,
                params_schema: schema_for!($params),
                response_schema: schema_for!($response),
            },)*]
        }
    }
}

operations! {
    FleetOverview(Empty) -> serde_json::Value, "fleet.overview", "fleet:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Observed agent and exporter state for permitted hosts";
    UnitsFailed(FailedParams) -> serde_json::Value, "units.failed", "units:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Failed readable services; unreachable hosts remain explicit";
    HostFacts(HostParams) -> serde_json::Value, "host.facts", "host:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Kernel, uptime and the running system closure";
    HostMetrics(HostParams) -> serde_json::Value, "host.metrics", "metrics:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Host-scoped CPU, memory, load, filesystem and network observations from Prometheus";
    DeployStatus(FailedParams) -> serde_json::Value, "deploy.status", "host:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Running closure versus persistent system profile; unavailable hosts remain explicit";
    UnitsList(HostParams) -> serde_json::Value, "units.list", "units:read", OperationKind::Observation, true, IdempotencyRequirement::None, "All explicitly readable services, including unloaded services";
    UnitsStatus(UnitParams) -> serde_json::Value, "units.status", "units:read", OperationKind::Observation, true, IdempotencyRequirement::None, "State of one explicitly readable service";
    UnitsLogs(LogParams) -> serde_json::Value, "units.logs", "logs:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Bounded recent journal entries for one readable service";
    AlertsActive(Empty) -> serde_json::Value, "alerts.active", "alerts:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Active alerts with an instance label matching permitted hosts";
    ExecRun(ExecRunParams) -> JobHandle, "exec.run", "exec:run", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Run a bounded command using a configured target profile";
    UnitsStart(UnitActionParams) -> JobHandle, "units.start", "units:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Start one explicitly manageable systemd service";
    UnitsStop(UnitActionParams) -> JobHandle, "units.stop", "units:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Stop one explicitly manageable systemd service";
    UnitsRestart(UnitActionParams) -> JobHandle, "units.restart", "units:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Restart one explicitly manageable systemd service";
    UnitsReload(UnitActionParams) -> JobHandle, "units.reload", "units:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Reload one explicitly manageable systemd service without upgrading to restart";
    JobsList(JobsListParams) -> JobsListResponse, "jobs.list", "jobs:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "List durable jobs owned by the authenticated principal";
    JobsStatus(JobIdParams) -> JobRecord, "jobs.status", "jobs:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Read one durable job and its current target-confirmed state";
    JobsLogs(JobLogsParams) -> JobLogsResponse, "jobs.logs", "jobs:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Read bounded command output using byte offsets";
    JobsCancel(JobCancelParams) -> JobRecord, "jobs.cancel", "jobs:cancel", OperationKind::JobControl, false, IdempotencyRequirement::None, "Request cancellation of a non-terminal job";
    WorkspaceCreate(WorkspaceCreateParams) -> JobHandle, "workspace.create", "workspace:write", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Create an isolated workspace at the currently observed configured remote ref";
    WorkspaceStatus(WorkspaceStatusParams) -> WorkspaceRecord, "workspace.status", "workspace:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Read a workspace's durable revision and Git identities";
    WorkspaceRead(WorkspaceReadParams) -> WorkspaceFile, "workspace.read", "workspace:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Read one regular UTF-8 file from an exact workspace revision";
    WorkspaceApply(WorkspaceApplyParams) -> WorkspaceRecord, "workspace.apply", "workspace:write", OperationKind::JobControl, false, IdempotencyRequirement::None, "Create a new immutable workspace revision from bounded file replacements or deletions";
    WorkspaceDiff(WorkspaceRevisionParams) -> WorkspaceDiff, "workspace.diff", "workspace:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Render the bounded Git patch for an exact workspace revision";
    WorkspaceCommit(WorkspaceCommitParams) -> WorkspaceRecord, "workspace.commit", "workspace:write", OperationKind::JobControl, false, IdempotencyRequirement::None, "Commit an exact workspace tree using the configured author identity";
    WorkspaceCheck(WorkspaceCheckParams) -> JobHandle, "workspace.check", "workspace:write", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Run one configured check against an immutable workspace revision";
    WorkspacePublish(WorkspacePublishParams) -> JobHandle, "workspace.publish", "workspace:publish", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Publish an exact committed workspace revision if the remote ref baseline is unchanged";
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

pub fn now() -> jiff::Timestamp {
    jiff::Timestamp::now()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requests_fail_closed() {
        for value in [
            r#"{"op":"units.restart","params":{"host":"a","unit":"a.service","force":true}}"#,
            r#"{"op":"host.facts","params":{"host":"a","uid":"admin"}}"#,
            r#"{"op":"host.facts","params":{"host":"a"},"confirmed":true}"#,
        ] {
            assert!(serde_json::from_str::<Request>(value).is_err());
        }
        for value in [
            r#""../../state.db""#,
            r#""00000000-0000-0000-0000-00000000000/""#,
            r#""000000000000000000000000000000000000""#,
        ] {
            assert!(serde_json::from_str::<JobId>(value).is_err());
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

    #[test]
    fn registry_exposes_execution_metadata_without_changing_observation_names() {
        let operations = operations();
        assert_eq!(operations.len(), 26);
        assert!(operations.iter().take(9).all(|operation| {
            matches!(operation.kind, OperationKind::Observation)
                && operation.read_only
                && operation.minimum_protocol_version == 1
                && matches!(operation.idempotency, IdempotencyRequirement::None)
        }));
        let value = serde_json::to_value(&operations).unwrap();
        assert!(value[0]["params_schema"].is_object());
        assert!(value[0]["response_schema"].is_object());
        let execute = operations
            .iter()
            .find(|operation| operation.name == "exec.run")
            .unwrap();
        assert_eq!(execute.kind, OperationKind::JobSubmission);
        assert_eq!(execute.minimum_protocol_version, 2);
        assert_eq!(execute.idempotency, IdempotencyRequirement::Required);
        assert!(!execute.read_only);
        let restart = operations
            .iter()
            .find(|operation| operation.name == "units.restart")
            .unwrap();
        assert_eq!(restart.capability, "units:manage");
        assert_eq!(restart.idempotency, IdempotencyRequirement::Required);
    }

    #[test]
    fn unit_actions_validate_external_state_preconditions() {
        let mut params = UnitActionParams {
            host: "host-a".into(),
            unit: "example.service".into(),
            expected_invocation_id: Some("0123456789abcdef0123456789abcdef".into()),
        };
        assert!(params.validate().is_ok());
        params.expected_invocation_id = Some("not-an-invocation".into());
        assert!(params.validate().is_err());
    }

    #[test]
    fn workspace_edits_are_revisioned_bounded_and_unique() {
        let mut params = WorkspaceApplyParams {
            repository: "infra".into(),
            workspace_id: WorkspaceId::parse("00000000-0000-0000-0000-000000000001").unwrap(),
            expected_revision: 1,
            edits: vec![WorkspaceEdit {
                path: "nixos/hosts/example/default.nix".into(),
                content: Some("{ ... }: { }".into()),
            }],
        };
        assert!(params.validate().is_ok());
        params.edits.push(params.edits[0].clone());
        assert!(params.validate().is_err());
        params.edits.pop();
        params.edits[0].content = Some("x".repeat(MAX_WORKSPACE_APPLY_BYTES + 1));
        assert!(params.validate().is_err());
        assert!(operations().iter().any(|operation| {
            operation.name == "workspace.publish"
                && operation.capability == "workspace:publish"
                && operation.idempotency == IdempotencyRequirement::Required
        }));
    }
}
