//! Versioned operations and durable execution types shared by every frontend.
pub mod changes;
pub mod client_api;
pub mod events;
pub mod jobs;
pub mod observations;
pub mod transport;
pub mod workspaces;

pub use changes::*;
pub use client_api::*;
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
pub struct UnitsListParams {
    pub host: String,
    /// Exact active state, for example failed or active. Omit to list all states.
    #[serde(default)]
    pub state: Option<String>,
    /// Literal unit-name prefix, not a shell pattern.
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default = "default_lines")]
    #[schemars(range(min = 1, max = 200))]
    pub limit: u16,
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
        if !valid_observation_unit(&self.unit) {
            return Err("invalid observation unit name");
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
    UnitsList(UnitsListParams) -> serde_json::Value, "units.list", "units:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Page authorized loaded units and configured unloaded units; filter by state or literal prefix. Coverage is explicit; an empty list is not whole-host health";
    UnitsStatus(UnitParams) -> serde_json::Value, "units.status", "units:read", OperationKind::Observation, true, IdempotencyRequirement::None, "State of one authorized systemd unit, including services, timers, targets and scopes";
    UnitsLogs(LogParams) -> serde_json::Value, "units.logs", "logs:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Bounded recent journal entries for one readable service";
    AlertsActive(Empty) -> serde_json::Value, "alerts.active", "alerts:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Active alerts with an instance label matching permitted hosts";
    EventsGet(EventGetParams) -> serde_json::Value, "events.get", "events:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Read bounded event evidence by event_id, JSON pointer and byte offset; use /payload for details omitted from summaries";
    EventsRecent(RecentEventsParams) -> serde_json::Value, "events.recent", "events:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Recent incident events, newest first, default last hour and 20 entries; filter by host/unit and use next_before_sequence for older pages";
    EventsList(EventsListParams) -> EventsListResponse, "events.list", "events:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Replay durable fleet events oldest first after a scoped cursor; use events.recent for incident diagnosis";
    SelfStatus(Empty) -> serde_json::Value, "self.status", "self:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Hub readiness, component state and bounded queue counters";
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
    DeployPrepare(DeployPrepareParams) -> JobHandle, "deploy.prepare", "deploy:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Freeze source and observed runtime baselines into a durable change plan";
    DeployBuild(DeployChangeParams) -> JobHandle, "deploy.build", "deploy:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Build the exact prepared workspace revision into a verified Nix artifact";
    DeployActivate(DeployChangeParams) -> JobHandle, "deploy.activate", "deploy:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Conditionally activate a built artifact against the plan runtime baseline";
    DeployVerify(DeployChangeParams) -> JobHandle, "deploy.verify", "deploy:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Run target-owned acceptance checks against the activated artifact";
    DeployRollback(DeployChangeParams) -> JobHandle, "deploy.rollback", "deploy:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Conditionally restore the plan baseline only while this change still owns runtime state";
    ChangesStatus(ChangeStatusParams) -> ChangeRecord, "changes.status", "changes:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Read a durable change plan and reconcile its current stage job";
    ChangesHistory(ChangeHistoryParams) -> ChangeHistoryResponse, "changes.history", "changes:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "List durable changes owned by the authenticated principal";
    DiagnosticsCollect(DiagnosticCollectParams) -> JobHandle, "diagnostics.collect", "diagnostics:collect", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Collect bounded target evidence and configured probes into a durable bundle";
    RemediationsBegin(RemediationBeginParams) -> JobHandle, "remediations.begin", "remediations:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Claim one budgeted remediation attempt for an event episode";
    RemediationsFinish(RemediationFinishParams) -> RemediationRecord, "remediations.finish", "remediations:manage", OperationKind::JobControl, false, IdempotencyRequirement::None, "Record a remediation outcome and related job or change";
    ResourcesList(ResourcesParams) -> serde_json::Value, "resources.list", "self:read", OperationKind::Observation, true, IdempotencyRequirement::None, "Discover authorized hosts, units, repositories, deployments or target execution profiles in bounded pages";
    JobsWait(JobWaitParams) -> serde_json::Value, "jobs.wait", "jobs:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Wait up to 10 seconds for a job revision or terminal outcome; no submission is replayed";
    JobsEvents(JobEventsParams) -> serde_json::Value, "jobs.events", "jobs:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Replay durable events for an owned job after a sequence cursor";
    JobsResult(JobResultParams) -> serde_json::Value, "jobs.result", "jobs:read", OperationKind::JobControl, true, IdempotencyRequirement::None, "Read stored result JSON using a JSON pointer (empty selects root); command stdout/stderr are in jobs.logs, not /stdout";
    DeployRun(DeployRunParams) -> JobHandle, "deploy.run", "deploy:manage", OperationKind::JobSubmission, false, IdempotencyRequirement::Required, "Durably run a prepared change through build or verified activation; preserves revision and external-writer guards";
}

pub fn valid_unit(name: &str) -> bool {
    name.len() <= 255
        && name.ends_with(".service")
        && name.len() > 8
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:@".contains(&b))
}

/// Exact systemd unit names for observation only. Mutations still use valid_unit.
pub fn valid_observation_unit(name: &str) -> bool {
    let Some((stem, kind)) = name.rsplit_once('.') else {
        return false;
    };
    if name.len() > 255
        || stem.is_empty()
        || !matches!(
            kind,
            "service"
                | "timer"
                | "socket"
                | "target"
                | "path"
                | "mount"
                | "automount"
                | "swap"
                | "slice"
                | "scope"
                | "device"
        )
    {
        return false;
    }
    let mut bytes = stem.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'\\' {
            if bytes.next() != Some(b'x')
                || !bytes.next().is_some_and(|b| b.is_ascii_hexdigit())
                || !bytes.next().is_some_and(|b| b.is_ascii_hexdigit())
            {
                return false;
            }
        } else if !(byte.is_ascii_alphanumeric() || b"_-.:@".contains(&byte)) {
            return false;
        }
    }
    true
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
    fn observation_unit_names_do_not_expand_mutation_names_or_allow_patterns() {
        for name in [
            "health.service",
            "backup.timer",
            "multi-user.target",
            "docker-a.scope",
            r"dev-disk\x2dlabel.device",
        ] {
            assert!(valid_observation_unit(name), "{name}");
        }
        for name in [
            "*.service",
            "../demo.service",
            "a/b.service",
            "demo.service;id",
            r"bad\name.service",
            ".service",
        ] {
            assert!(!valid_observation_unit(name), "{name}");
        }
        assert!(!valid_unit("multi-user.target"));
    }

    #[test]
    fn job_schema_exposes_the_remote_uuid_contract() {
        let schema = serde_json::to_value(schema_for!(JobId)).unwrap();
        assert_eq!(schema["type"], "string");
        let wait = serde_json::to_value(schema_for!(JobWaitParams)).unwrap();
        assert_eq!(wait["properties"]["job_id"]["minLength"], 36);
        assert_eq!(schema["minLength"], 36);
        assert_eq!(schema["maxLength"], 36);
        assert!(schema["pattern"].as_str().unwrap().contains("{12}"));
        assert!(
            serde_json::from_value::<JobWaitParams>(serde_json::json!({"job_id":"133"})).is_err()
        );
        assert!(
            serde_json::from_value::<JobWaitParams>(
                serde_json::json!({"job_id":"01a08fdf-744d-7401-a700-616632d53bee"})
            )
            .is_ok()
        );
    }

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
        assert_eq!(operations.len(), 45);
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
