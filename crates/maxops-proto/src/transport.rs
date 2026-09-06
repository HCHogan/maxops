//! Shared bounded HTTP transport. Never include tokens or upstream bodies in errors.
use axum::{
    Json,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::{net::SocketAddr, path::Path, time::Duration};
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

pub struct ApiError(pub StatusCode, pub &'static str);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"error": self.1}))).into_response()
    }
}
pub type ApiResult<T> = Result<Json<T>, ApiError>;

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
    color_eyre::eyre::ensure!(response.status().is_success(), "upstream request failed");
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
