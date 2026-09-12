use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

pub const MAX_WORKSPACE_APPLY_BYTES: usize = 1024 * 1024;
pub const MAX_WORKSPACE_READ_BYTES: u32 = 256 * 1024;

#[derive(Clone, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct WorkspaceId(String);

impl WorkspaceId {
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
            Err("invalid workspace ID")
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkspaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<'de> Deserialize<'de> for WorkspaceId {
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
pub enum WorkspaceState {
    Clean,
    Dirty,
    Committed,
    Published,
}

impl WorkspaceState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::Dirty => "dirty",
            Self::Committed => "committed",
            Self::Published => "published",
        }
    }
}

impl FromStr for WorkspaceState {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "clean" => Ok(Self::Clean),
            "dirty" => Ok(Self::Dirty),
            "committed" => Ok(Self::Committed),
            "published" => Ok(Self::Published),
            _ => Err("invalid workspace state"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRecord {
    pub workspace_id: WorkspaceId,
    pub repository: String,
    pub executor: String,
    pub base_commit: String,
    pub revision: u64,
    pub tree_hash: String,
    #[serde(default)]
    pub commit_hash: Option<String>,
    pub state: WorkspaceState,
    pub creator: String,
    #[schemars(with = "String")]
    pub created_at: jiff::Timestamp,
    #[schemars(with = "Option<String>")]
    pub retain_until: Option<jiff::Timestamp>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCreateParams {
    /// Permitted repository name from resources.list(kind=repositories).
    pub repository: String,
    /// Expected full remote commit hash observed for the configured ref; a later external push rejects this operation instead of being overwritten.
    #[serde(default)]
    pub expected_remote_head: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceStatusParams {
    /// Permitted repository name from resources.list(kind=repositories).
    pub repository: String,
    /// Workspace UUID from workspace.create or workspace.status; use the same repository.
    pub workspace_id: WorkspaceId,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRevisionParams {
    /// Permitted repository name from resources.list(kind=repositories).
    pub repository: String,
    /// Workspace UUID from workspace.create or workspace.status; use the same repository.
    pub workspace_id: WorkspaceId,
    /// Current workspace revision from workspace.status; compare-and-swap guard against intervening edits. Re-read after every change.
    pub expected_revision: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceReadParams {
    /// Permitted repository name from resources.list(kind=repositories).
    pub repository: String,
    /// Workspace UUID from workspace.create or workspace.status; use the same repository.
    pub workspace_id: WorkspaceId,
    /// Current workspace revision from workspace.status; compare-and-swap guard against intervening edits. Re-read after every change.
    pub expected_revision: u64,
    /// Relative regular-file path within the workspace; no absolute paths, traversal, .git or symlinks.
    pub path: String,
    /// Maximum UTF-8 file bytes to read; oversized files are rejected rather than truncated.
    #[serde(default = "default_workspace_read_bytes")]
    #[schemars(range(min = 1, max = 262144))]
    #[schema(minimum = 1, maximum = 262144, default = 262144)]
    pub max_bytes: u32,
}

pub const fn default_workspace_read_bytes() -> u32 {
    MAX_WORKSPACE_READ_BYTES
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceEdit {
    /// Relative regular-file path within the workspace; no absolute paths, traversal, .git or symlinks.
    pub path: String,
    /// UTF-8 replacement contents. `null` deletes an existing regular file.
    pub content: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceApplyParams {
    /// Permitted repository name from resources.list(kind=repositories).
    pub repository: String,
    /// Workspace UUID from workspace.create or workspace.status; use the same repository.
    pub workspace_id: WorkspaceId,
    /// Current workspace revision from workspace.status; compare-and-swap guard against intervening edits. Re-read after every change.
    pub expected_revision: u64,
    /// Bounded replacements or deletions of regular UTF-8 files; creates one new immutable revision.
    pub edits: Vec<WorkspaceEdit>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCommitParams {
    /// Permitted repository name from resources.list(kind=repositories).
    pub repository: String,
    /// Workspace UUID from workspace.create or workspace.status; use the same repository.
    pub workspace_id: WorkspaceId,
    /// Current workspace revision from workspace.status; compare-and-swap guard against intervening edits. Re-read after every change.
    pub expected_revision: u64,
    /// Commit message for the exact workspace revision; author identity comes from repository policy.
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCheckParams {
    /// Permitted repository name from resources.list(kind=repositories).
    pub repository: String,
    /// Workspace UUID from workspace.create or workspace.status; use the same repository.
    pub workspace_id: WorkspaceId,
    /// Current workspace revision from workspace.status; compare-and-swap guard against intervening edits. Re-read after every change.
    pub expected_revision: u64,
    /// Operator-configured check name for the repository from resources.list(kind=repositories); discovery lists repository names, so obtain configured check names from its operator policy. No arbitrary command.
    pub check: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspacePublishParams {
    /// Permitted repository name from resources.list(kind=repositories).
    pub repository: String,
    /// Workspace UUID from workspace.create or workspace.status; use the same repository.
    pub workspace_id: WorkspaceId,
    /// Current workspace revision from workspace.status; compare-and-swap guard against intervening edits. Re-read after every change.
    pub expected_revision: u64,
    /// Configured publishable Git ref, such as refs/heads/main; obtain it from repository operator policy (resources.list(kind=repositories) discovers names only). No force push.
    pub reference: String,
    /// Expected full remote commit hash observed for the configured ref; a later external push rejects this operation instead of being overwritten.
    pub expected_remote_head: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceFile {
    pub workspace_id: WorkspaceId,
    pub revision: u64,
    pub path: String,
    pub encoding: String,
    pub content: String,
    pub digest: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceDiff {
    pub workspace_id: WorkspaceId,
    pub revision: u64,
    pub against_commit: String,
    pub patch: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(tag = "action", content = "params", rename_all = "snake_case")]
pub enum WorkspaceTargetRequest {
    Status(WorkspaceStatusParams),
    Read(WorkspaceReadParams),
    Apply(WorkspaceApplyParams),
    Diff(WorkspaceRevisionParams),
    Commit(WorkspaceCommitParams),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, utoipa::ToSchema)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum WorkspaceTargetResponse {
    Record(WorkspaceRecord),
    File(WorkspaceFile),
    Diff(WorkspaceDiff),
}

pub fn valid_repository_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub fn valid_git_oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn valid_check_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

impl WorkspaceCreateParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_repository(&self.repository)?;
        if self
            .expected_remote_head
            .as_deref()
            .is_some_and(|value| !valid_git_oid(value))
        {
            return Err("invalid expected remote commit");
        }
        Ok(())
    }
}

impl WorkspaceStatusParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_repository(&self.repository)
    }
}

impl WorkspaceRevisionParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_revision(&self.repository, self.expected_revision)
    }
}

impl WorkspaceReadParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_revision(&self.repository, self.expected_revision)?;
        if self.path.is_empty()
            || self.path.len() > 4096
            || self.path.as_bytes().contains(&0)
            || !(1..=MAX_WORKSPACE_READ_BYTES).contains(&self.max_bytes)
        {
            return Err("invalid workspace read request");
        }
        Ok(())
    }
}

impl WorkspaceApplyParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_revision(&self.repository, self.expected_revision)?;
        if self.edits.is_empty() || self.edits.len() > 128 {
            return Err("workspace apply requires 1..128 edits");
        }
        let mut paths = std::collections::BTreeSet::new();
        let mut bytes = 0_usize;
        for edit in &self.edits {
            if edit.path.is_empty()
                || edit.path.len() > 4096
                || edit.path.as_bytes().contains(&0)
                || !paths.insert(&edit.path)
            {
                return Err("workspace edit paths must be unique and bounded");
            }
            bytes = bytes
                .checked_add(edit.content.as_ref().map_or(0, String::len))
                .ok_or("workspace edits are too large")?;
        }
        if bytes > MAX_WORKSPACE_APPLY_BYTES {
            return Err("workspace edits exceed 1 MiB");
        }
        Ok(())
    }
}

impl WorkspaceCommitParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_revision(&self.repository, self.expected_revision)?;
        if self.message.trim().is_empty()
            || self.message.len() > 8192
            || self.message.as_bytes().contains(&0)
        {
            return Err("commit message must contain 1..8192 bytes");
        }
        Ok(())
    }
}

impl WorkspaceCheckParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_revision(&self.repository, self.expected_revision)?;
        if !valid_check_id(&self.check) {
            return Err("invalid check name");
        }
        Ok(())
    }
}

impl WorkspacePublishParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_revision(&self.repository, self.expected_revision)?;
        if self.reference.is_empty()
            || self.reference.len() > 512
            || !valid_git_oid(&self.expected_remote_head)
        {
            return Err("invalid publish baseline");
        }
        Ok(())
    }
}

fn validate_repository(repository: &str) -> Result<(), &'static str> {
    if valid_repository_id(repository) {
        Ok(())
    } else {
        Err("invalid repository ID")
    }
}

fn validate_revision(repository: &str, revision: u64) -> Result<(), &'static str> {
    validate_repository(repository)?;
    if revision == 0 {
        return Err("workspace revision must be positive");
    }
    Ok(())
}
