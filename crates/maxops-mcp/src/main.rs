//! Thin MCP stdio adapter over the authenticated maxops HTTP API.
use clap::Parser;
use maxops_proto::{
    PROTOCOL_VERSION,
    transport::{self, MAX_BODY, Token},
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    io::{BufRead, Read, Write},
    path::PathBuf,
};

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const IDEMPOTENCY_ARGUMENT: &str = "_maxops_idempotency_key";

#[derive(Parser)]
#[command(
    version,
    about = "Expose one maxops principal's operation catalog over MCP stdio"
)]
struct Args {
    #[arg(long, env = "MAXOPS_URL", default_value = "http://127.0.0.1:9721")]
    url: String,
    #[arg(long, env = "MAXOPS_TOKEN_FILE")]
    token_file: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
struct Catalog {
    version: u16,
    operations: Vec<ServerOperation>,
}

#[derive(Clone, Debug, Deserialize)]
struct ServerOperation {
    name: String,
    summary: String,
    kind: String,
    read_only: bool,
    idempotency: String,
    params_schema: Value,
    response_schema: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    New,
    Initializing,
    Ready,
}

struct Server {
    url: String,
    token: Token,
    client: reqwest::Client,
    phase: Phase,
}

impl Server {
    fn new(url: String, token: Token) -> color_eyre::eyre::Result<Self> {
        transport::validate_url(&url)?;
        Ok(Self {
            url: url.trim_end_matches('/').to_owned(),
            token,
            client: transport::client()?,
            phase: Phase::New,
        })
    }

    async fn handle(&mut self, message: Value) -> Option<Value> {
        let Some(object) = message.as_object() else {
            return Some(rpc_error(Value::Null, -32600, "Invalid request"));
        };
        let id = object.get("id").cloned();
        if id
            .as_ref()
            .is_some_and(|id| !matches!(id, Value::String(_) | Value::Number(_)))
        {
            return Some(rpc_error(Value::Null, -32600, "Invalid request ID"));
        }
        if object.get("jsonrpc") != Some(&Value::String("2.0".into())) {
            return Some(rpc_error(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid request",
            ));
        }
        let Some(method) = object.get("method").and_then(Value::as_str) else {
            return Some(rpc_error(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid request",
            ));
        };

        if id.is_none() {
            if method == "notifications/initialized" && self.phase == Phase::Initializing {
                self.phase = Phase::Ready;
            }
            return None;
        }
        let id = id.expect("request ID checked");
        Some(match method {
            "initialize" => self.initialize(id, object.get("params")),
            "ping" if self.phase != Phase::New => rpc_result(id, json!({})),
            "tools/list" if self.phase == Phase::Ready => match self.tools().await {
                Ok(tools) => rpc_result(id, json!({"tools": tools})),
                Err(()) => rpc_error(id, -32603, "Unable to read maxops operation catalog"),
            },
            "tools/call" if self.phase == Phase::Ready => {
                match self.call_tool(object.get("params")).await {
                    Ok(result) => rpc_result(id, result),
                    Err(CallError::InvalidParams) => {
                        rpc_error(id, -32602, "Invalid tool parameters")
                    }
                    Err(CallError::Catalog) => {
                        rpc_error(id, -32603, "Unable to read maxops operation catalog")
                    }
                }
            }
            "tools/list" | "tools/call" => {
                rpc_error(id, -32002, "Server initialization is incomplete")
            }
            _ => rpc_error(id, -32601, "Method not found"),
        })
    }

    fn initialize(&mut self, id: Value, params: Option<&Value>) -> Value {
        if self.phase != Phase::New {
            return rpc_error(id, -32600, "Server is already initialized");
        }
        if params
            .and_then(|value| value.get("protocolVersion"))
            .and_then(Value::as_str)
            .is_none()
        {
            return rpc_error(id, -32602, "Missing protocol version");
        }
        self.phase = Phase::Initializing;
        rpc_result(
            id,
            json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {
                    "name": "maxops",
                    "title": "maxops fleet operations",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "instructions": "Tools are the operations permitted to this maxops credential. Durable job submissions require _maxops_idempotency_key and return a job handle to poll with jobs.status. Re-observe state before every mutation because humans and other fleet tools can change hosts and repositories."
            }),
        )
    }

    async fn catalog(&self) -> Result<Catalog, ()> {
        let catalog: Catalog = transport::read_json(
            self.token
                .apply(self.client.get(format!("{}/v1/operations", self.url))),
        )
        .await
        .map_err(|_| ())?;
        if catalog.version == 0 || catalog.version > PROTOCOL_VERSION {
            return Err(());
        }
        Ok(catalog)
    }

    async fn tools(&self) -> Result<Vec<Value>, ()> {
        self.catalog()
            .await?
            .operations
            .iter()
            .map(tool_definition)
            .collect()
    }

    async fn call_tool(&self, params: Option<&Value>) -> Result<Value, CallError> {
        let name = params
            .and_then(|value| value.get("name"))
            .and_then(Value::as_str)
            .ok_or(CallError::InvalidParams)?;
        let mut arguments = match params.and_then(|value| value.get("arguments")) {
            None => Map::new(),
            Some(Value::Object(arguments)) => arguments.clone(),
            Some(_) => return Err(CallError::InvalidParams),
        };
        let catalog = self.catalog().await.map_err(|()| CallError::Catalog)?;
        let Some(operation) = catalog
            .operations
            .iter()
            .find(|operation| operation.name == name)
        else {
            return Ok(tool_error("Tool is not permitted by the maxops principal"));
        };
        let idempotency_key = if operation.idempotency == "required" {
            let Some(Value::String(value)) = arguments.remove(IDEMPOTENCY_ARGUMENT) else {
                return Err(CallError::InvalidParams);
            };
            if value.is_empty()
                || value.len() > 128
                || !value.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(CallError::InvalidParams);
            }
            Some(value)
        } else {
            if arguments.contains_key(IDEMPOTENCY_ARGUMENT) {
                return Err(CallError::InvalidParams);
            }
            None
        };
        let mut request = self
            .token
            .apply(self.client.post(format!("{}/v1/execute", self.url)))
            .json(&json!({"op": operation.name, "params": arguments}));
        if let Some(key) = idempotency_key {
            request = request.header("idempotency-key", key);
        }
        let response: Value = match transport::read_json(request).await {
            Ok(value) => value,
            Err(error) => {
                let message = transport::upstream_status(&error).map_or_else(
                    || "maxops hub request failed".to_owned(),
                    |status| format!("maxops hub returned HTTP {}", status.as_u16()),
                );
                return Ok(tool_error(&message));
            }
        };
        let text = serde_json::to_string_pretty(&response).map_err(|_| CallError::Catalog)?;
        Ok(json!({
            "content": [{"type": "text", "text": text}],
            "structuredContent": response,
            "isError": false
        }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallError {
    InvalidParams,
    Catalog,
}

fn tool_definition(operation: &ServerOperation) -> Result<Value, ()> {
    let mut input = operation.params_schema.clone();
    let mut output = operation.response_schema.clone();
    if !input.is_object() || !output.is_object() {
        return Err(());
    }
    let output_type = output.get("type").and_then(Value::as_str);
    if output_type.is_some_and(|kind| kind != "object") {
        return Err(());
    }
    if output_type.is_none() {
        output
            .as_object_mut()
            .expect("object checked")
            .insert("type".into(), Value::String("object".into()));
    }
    if operation.idempotency == "required" {
        let properties = input
            .get_mut("properties")
            .and_then(Value::as_object_mut)
            .ok_or(())?;
        properties.insert(
            IDEMPOTENCY_ARGUMENT.into(),
            json!({
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "description": "Stable retry key for this logical maxops job submission"
            }),
        );
        let required = input
            .as_object_mut()
            .expect("object checked")
            .entry("required")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or(())?;
        required.push(Value::String(IDEMPOTENCY_ARGUMENT.into()));
    } else if operation.idempotency != "none" {
        return Err(());
    }
    if !matches!(
        operation.kind.as_str(),
        "observation" | "job_submission" | "job_control"
    ) {
        return Err(());
    }
    Ok(json!({
        "name": operation.name,
        "title": operation.name,
        "description": operation.summary,
        "inputSchema": input,
        "outputSchema": output,
        "annotations": {
            "readOnlyHint": operation.read_only,
            "destructiveHint": !operation.read_only,
            "idempotentHint": operation.idempotency == "required",
            "openWorldHint": true
        }
    }))
}

fn tool_error(message: &str) -> Value {
    json!({"content": [{"type": "text", "text": message}], "isError": true})
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    color_eyre::install()?;
    let args = Args::parse();
    let token = Token::read(&args.token_file)?;
    let mut server = Server::new(args.url, token)?;
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    loop {
        let mut line = Vec::new();
        let count = input
            .by_ref()
            .take(MAX_BODY as u64 + 1)
            .read_until(b'\n', &mut line)?;
        if count == 0 {
            break;
        }
        if line.len() > MAX_BODY {
            serde_json::to_writer(
                &mut output,
                &rpc_error(Value::Null, -32700, "MCP message exceeds 2 MiB"),
            )?;
            output.write_all(b"\n")?;
            output.flush()?;
            break;
        }
        let response = match serde_json::from_slice(&line) {
            Ok(message) => server.handle(message).await,
            Err(_) => Some(rpc_error(Value::Null, -32700, "Parse error")),
        };
        if let Some(response) = response {
            serde_json::to_writer(&mut output, &response)?;
            output.write_all(b"\n")?;
            output.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::State,
        http::HeaderMap,
        routing::{get, post},
    };
    use std::sync::{Arc, Mutex};

    const TOKEN: &str = "mcp-test-token-aaaaaaaaaaaaaaaaaa";
    type RecordedCalls = Arc<Mutex<Vec<(Value, Option<String>)>>>;

    #[derive(Clone, Default)]
    struct FakeState {
        calls: RecordedCalls,
    }

    fn catalog() -> Value {
        json!({
            "version": PROTOCOL_VERSION,
            "operations": [
                {
                    "name": "host.facts",
                    "summary": "facts",
                    "capability": "host:read",
                    "kind": "observation",
                    "read_only": true,
                    "minimum_protocol_version": 1,
                    "idempotency": "none",
                    "params_schema": {"type":"object","properties":{"host":{"type":"string"}},"required":["host"],"additionalProperties":false},
                    "response_schema": {"type":"object"}
                },
                {
                    "name": "exec.run",
                    "summary": "execute",
                    "capability": "exec:run",
                    "kind": "job_submission",
                    "read_only": false,
                    "minimum_protocol_version": 2,
                    "idempotency": "required",
                    "params_schema": {"type":"object","properties":{"host":{"type":"string"}},"required":["host"],"additionalProperties":false},
                    "response_schema": {"type":"object"}
                }
            ]
        })
    }

    async fn fake_catalog(headers: HeaderMap) -> Json<Value> {
        assert_eq!(headers["authorization"], format!("Bearer {TOKEN}"));
        Json(catalog())
    }

    async fn fake_execute(
        State(state): State<FakeState>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        assert_eq!(headers["authorization"], format!("Bearer {TOKEN}"));
        state.calls.lock().unwrap().push((
            body,
            headers
                .get("idempotency-key")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        ));
        Json(json!({"job_id":"018f0000-0000-7000-8000-000000000000","state":"queued","revision":0}))
    }

    async fn stub() -> (String, FakeState, tokio::task::JoinHandle<()>) {
        let state = FakeState::default();
        let app = Router::new()
            .route("/v1/operations", get(fake_catalog))
            .route("/v1/execute", post(fake_execute))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{address}"), state, task)
    }

    async fn ready_server(url: String) -> Server {
        let mut server = Server::new(url, Token::parse(TOKEN.into()).unwrap()).unwrap();
        let initialized = server
            .handle(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":MCP_PROTOCOL_VERSION,"capabilities":{},"clientInfo":{"name":"test","version":"1"}}}))
            .await
            .unwrap();
        assert_eq!(
            initialized["result"]["protocolVersion"],
            MCP_PROTOCOL_VERSION
        );
        assert!(
            server
                .handle(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
                .await
                .is_none()
        );
        server
    }

    #[tokio::test]
    async fn list_uses_the_principal_scoped_server_catalog() {
        let (url, _, task) = stub().await;
        let mut server = ready_server(url).await;
        let response = server
            .handle(json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}))
            .await
            .unwrap();
        let tools = response["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0]["name"], "host.facts");
        assert_eq!(tools[0]["annotations"]["readOnlyHint"], true);
        assert!(
            tools[1]["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&Value::String(IDEMPOTENCY_ARGUMENT.into()))
        );
        task.abort();
    }

    #[tokio::test]
    async fn call_preserves_identity_and_translates_the_idempotency_key() {
        let (url, state, task) = stub().await;
        let mut server = ready_server(url).await;
        let response = server
            .handle(json!({
                "jsonrpc":"2.0",
                "id":3,
                "method":"tools/call",
                "params":{"name":"exec.run","arguments":{"host":"alpha",(IDEMPOTENCY_ARGUMENT):"repair-1"}}
            }))
            .await
            .unwrap();
        assert_eq!(response["result"]["isError"], false);
        let calls = state.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].0,
            json!({"op":"exec.run","params":{"host":"alpha"}})
        );
        assert_eq!(calls[0].1.as_deref(), Some("repair-1"));
        task.abort();
    }

    #[tokio::test]
    async fn call_refuses_tools_absent_from_the_server_catalog() {
        let (url, state, task) = stub().await;
        let mut server = ready_server(url).await;
        let response = server
            .handle(json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"units.stop","arguments":{}}}))
            .await
            .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(state.calls.lock().unwrap().is_empty());
        task.abort();
    }
}
