//! Shared bounded HTTP transport. Never include tokens or upstream bodies in errors.
use axum::{
    Json,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::{fmt, net::SocketAddr, path::Path, time::Duration};
use subtle::ConstantTimeEq;

pub const MAX_BODY: usize = 2 * 1024 * 1024;
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Clone)]
pub struct Token(String);

impl Token {
    pub fn parse(value: String) -> color_eyre::eyre::Result<Self> {
        let value = value.trim_end_matches(['\r', '\n']);
        color_eyre::eyre::ensure!(
            value.len() >= 32 && value.len() <= 512 && value.bytes().all(|b| b.is_ascii_graphic()),
            "token must contain 32..512 printable ASCII characters without spaces"
        );
        Ok(Self(value.to_owned()))
    }
    pub fn read(path: &Path) -> color_eyre::eyre::Result<Self> {
        Self::parse(std::fs::read_to_string(path)?)
    }
    pub fn matches(&self, headers: &HeaderMap) -> bool {
        if headers.get_all("authorization").iter().count() != 1 {
            return false;
        }
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|v| bool::from(self.0.as_bytes().ct_eq(v.as_bytes())))
    }
    pub fn same_as(&self, other: &Self) -> bool {
        bool::from(self.0.as_bytes().ct_eq(other.0.as_bytes()))
    }
    pub fn apply(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.bearer_auth(&self.0)
    }
}

#[derive(Debug)]
pub struct ApiError(pub StatusCode, pub &'static str);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, retry) = match self.1 {
            "unsupported operation" => ("unsupported_operation", "refresh_catalog"),
            "capability not permitted" => ("capability_not_permitted", "never"),
            "host not permitted" | "job targets another host" => ("host_not_permitted", "never"),
            "unit not permitted" | "service not permitted" => ("unit_not_readable", "never"),
            "unit is not manageable" | "service is not manageable" => {
                ("unit_not_manageable", "never")
            }
            "unit kind is not manageable; mutations require .service" => {
                ("unit_kind_not_manageable", "never")
            }
            "logs not permitted" => ("logs_not_permitted", "never"),
            "repository not permitted" => ("repository_not_permitted", "never"),
            "deployment not permitted" => ("deployment_not_permitted", "never"),
            "execution profiles require a host" => ("execution_profile_host_required", "never"),
            "invalid observation unit name" => ("invalid_unit_name", "never"),
            "idempotency key conflicts with another request" => ("idempotency_conflict", "never"),
            "job revision changed" | "change revision changed" => ("revision_conflict", "refresh"),
            "deployment baseline changed" | "deployment plan expired" => {
                ("stale_baseline", "replan")
            }
            "catalog revision changed" | "invalid cursor" => ("cursor_invalid", "restart_listing"),
            "change is owned by a deployment workflow" => ("workflow_conflict", "observe"),
            _ => match self.0 {
                StatusCode::UNAUTHORIZED => ("unauthenticated", "never"),
                StatusCode::FORBIDDEN => ("forbidden", "never"),
                StatusCode::NOT_FOUND => ("not_found", "never"),
                StatusCode::CONFLICT => ("state_conflict", "refresh"),
                StatusCode::GONE => ("cursor_expired", "restart_listing"),
                StatusCode::TOO_MANY_REQUESTS => ("busy", "backoff"),
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                    ("invalid_request", "never")
                }
                _ => ("unavailable", "observe_before_retry"),
            },
        };
        (
            self.0,
            Json(serde_json::json!({"error": self.1, "code": code, "retry": retry})),
        )
            .into_response()
    }
}
pub type ApiResult<T> = Result<Json<T>, ApiError>;

#[derive(Debug)]
pub struct UpstreamHttpError {
    status: StatusCode,
    code: Option<String>,
    retry: Option<String>,
}

impl UpstreamHttpError {
    pub fn new(status: StatusCode) -> Self {
        Self {
            status,
            code: None,
            retry: None,
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }
    pub fn retry(&self) -> Option<&str> {
        self.retry.as_deref()
    }

    fn with_public_body(mut self, bytes: &[u8]) -> Self {
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
            let code = value.get("code").and_then(serde_json::Value::as_str);
            let retry = value.get("retry").and_then(serde_json::Value::as_str);
            if let (Some(code), Some(retry)) = (code, retry)
                && matches!(
                    code,
                    "unsupported_operation"
                        | "idempotency_conflict"
                        | "revision_conflict"
                        | "stale_baseline"
                        | "cursor_invalid"
                        | "workflow_conflict"
                        | "unauthenticated"
                        | "forbidden"
                        | "not_found"
                        | "state_conflict"
                        | "cursor_expired"
                        | "busy"
                        | "invalid_request"
                        | "unit_kind_not_manageable"
                        | "unavailable"
                )
                && matches!(
                    retry,
                    "refresh_catalog"
                        | "never"
                        | "refresh"
                        | "replan"
                        | "restart_listing"
                        | "observe"
                        | "backoff"
                        | "observe_before_retry"
                )
            {
                self.code = Some(code.into());
                self.retry = Some(retry.into());
            }
        }
        self
    }
}

impl fmt::Display for UpstreamHttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "upstream request failed with HTTP {}",
            self.status
        )?;
        if let (Some(code), Some(retry)) = (&self.code, &self.retry) {
            write!(formatter, " code={code} retry={retry}")?;
        }
        Ok(())
    }
}

impl std::error::Error for UpstreamHttpError {}

#[derive(Debug)]
pub struct ExecutorRejected {
    pub code: String,
    pub message: String,
}

impl fmt::Display for ExecutorRejected {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "executor rejected request ({}): {}",
            self.code, self.message
        )
    }
}

impl std::error::Error for ExecutorRejected {}

pub fn upstream_status(error: &color_eyre::Report) -> Option<StatusCode> {
    error
        .downcast_ref::<UpstreamHttpError>()
        .map(UpstreamHttpError::status)
}

pub fn client() -> color_eyre::eyre::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()?)
}

pub fn validate_url(value: &str) -> color_eyre::eyre::Result<reqwest::Url> {
    let url = reqwest::Url::parse(value)?;
    color_eyre::eyre::ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "invalid HTTP endpoint"
    );
    Ok(url)
}

pub fn validate_listen(addr: SocketAddr) -> color_eyre::eyre::Result<()> {
    color_eyre::eyre::ensure!(
        !addr.ip().is_unspecified(),
        "bind an explicit loopback or private network address"
    );
    Ok(())
}

pub async fn read_json<T: serde::de::DeserializeOwned>(
    request: reqwest::RequestBuilder,
) -> color_eyre::eyre::Result<T> {
    let mut response = request.send().await?;
    if !response.status().is_success() {
        let error = UpstreamHttpError::new(response.status());
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() + chunk.len() > 4096 {
                return Err(error.into());
            }
            bytes.extend_from_slice(&chunk);
        }
        return Err(error.with_public_body(&bytes).into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        color_eyre::eyre::ensure!(
            bytes.len() + chunk.len() <= MAX_BODY,
            "upstream response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(unix)]
pub async fn executor_request(
    socket: &Path,
    request: &crate::ExecutorRequest,
) -> color_eyre::eyre::Result<crate::ExecutorResponse> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let mut stream = tokio::net::UnixStream::connect(socket).await?;
    let mut bytes = serde_json::to_vec(request)?;
    color_eyre::eyre::ensure!(bytes.len() < MAX_BODY, "executor request too large");
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    BufReader::new(stream)
        .take(MAX_BODY as u64 + 1)
        .read_until(b'\n', &mut response)
        .await?;
    color_eyre::eyre::ensure!(response.len() <= MAX_BODY, "executor response too large");
    match serde_json::from_slice(&response)? {
        crate::ExecutorWireResponse::Ok { response } => Ok(*response),
        crate::ExecutorWireResponse::Error { code, message } => {
            Err(ExecutorRejected { code, message }.into())
        }
    }
}

pub async fn shutdown() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Serialize)]
pub struct Health {
    pub status: &'static str,
    pub version: &'static str,
}
pub async fn health() -> Json<Health> {
    Json(Health {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mutation_kind_error_crosses_transport_without_upstream_text() {
        let error = UpstreamHttpError::new(StatusCode::BAD_REQUEST).with_public_body(
            br#"{"code":"unit_kind_not_manageable","retry":"never","error":"private upstream text"}"#,
        );
        assert_eq!(error.code(), Some("unit_kind_not_manageable"));
        assert_eq!(error.retry(), Some("never"));
        assert!(!error.to_string().contains("private upstream text"));
    }

    #[test]
    fn bearer_auth_rejects_missing_wrong_and_duplicate_headers() {
        let token = Token::parse("a".repeat(32)).unwrap();
        let mut headers = HeaderMap::new();
        assert!(!token.matches(&headers));
        headers.insert(
            "authorization",
            format!("Bearer {}", "b".repeat(32)).parse().unwrap(),
        );
        assert!(!token.matches(&headers));
        headers.insert(
            "authorization",
            format!("Bearer {}", "a".repeat(32)).parse().unwrap(),
        );
        assert!(token.matches(&headers));
        headers.append("authorization", "Bearer wrong".parse().unwrap());
        assert!(!token.matches(&headers));
        assert!(Token::parse(String::new()).is_err());
        assert!(validate_url("http://user:secret@localhost").is_err());
    }
}
