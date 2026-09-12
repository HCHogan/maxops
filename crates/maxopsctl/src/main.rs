use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use clap::{Arg, ArgAction, ArgMatches, Command};
use maxops_proto::{
    IdempotencyRequirement, JobHandle, JobId, JobLogsParams, JobLogsResponse, JobRecord, JobState,
    OperationKind, PROTOCOL_VERSION, Request, operations,
    transport::{self, Token},
};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    path::Path,
    time::Duration,
};

const MAX_PARAMS_BYTES: u64 = 1024 * 1024;

fn cli() -> Command {
    let mut command = Command::new("maxopsctl")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Query a maxops hub; operation names and parameters come from the shared registry")
        .arg(
            Arg::new("url")
                .long("url")
                .env("MAXOPS_URL")
                .default_value("http://127.0.0.1:9721")
                .global(true),
        )
        .arg(
            Arg::new("token-file")
                .long("token-file")
                .env("MAXOPS_TOKEN_FILE")
                .global(true),
        )
        .subcommand_required(true)
        .subcommand(Command::new("operations").about("List operations allowed by the server"))
        .subcommand(
            Command::new("schema")
                .about("Print the local versioned operation catalog; no connection required"),
        );
    for operation in operations() {
        let mut subcommand = Command::new(operation.name)
            .about(operation.summary)
            .arg(
                Arg::new("params-file")
                    .long("params-file")
                    .value_name("PATH")
                    .conflicts_with("params-stdin")
                    .help("Read the complete params JSON object from a file"),
            )
            .arg(
                Arg::new("params-stdin")
                    .long("params-stdin")
                    .action(ArgAction::SetTrue)
                    .help("Read the complete params JSON object from stdin"),
            );
        if operation.kind == OperationKind::JobSubmission {
            subcommand = subcommand
                .arg(
                    Arg::new("idempotency-key")
                        .long("idempotency-key")
                        .required(true)
                        .help("Stable retry key for this logical job submission"),
                )
                .arg(
                    Arg::new("wait")
                        .long("wait")
                        .action(ArgAction::SetTrue)
                        .help("Wait for a terminal job state"),
                )
                .arg(
                    Arg::new("follow")
                        .long("follow")
                        .requires("wait")
                        .action(ArgAction::SetTrue)
                        .help("Stream decoded stdout and stderr while waiting"),
                );
        }
        let schema = serde_json::to_value(operation.params_schema).expect("serializable schema");
        if let Some(properties) = schema["properties"].as_object() {
            for (name, property) in properties {
                let required = schema["required"]
                    .as_array()
                    .is_some_and(|items| items.iter().any(|item| item == name));
                let Some(kind) = scalar_type(property) else {
                    continue;
                };
                let mut arg = Arg::new(name.clone())
                    .long(name.replace('_', "-"))
                    .conflicts_with_all(["params-file", "params-stdin"]);
                if required {
                    arg = arg.required_unless_present_any(["params-file", "params-stdin"]);
                }
                if kind == "integer" {
                    arg = arg.value_parser(clap::value_parser!(u64));
                }
                subcommand = subcommand.arg(arg);
            }
        }
        command = command.subcommand(subcommand);
    }
    command
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    color_eyre::install()?;
    let matches = cli().get_matches();
    let (operation, args) = matches.subcommand().expect("required subcommand");
    if operation == "schema" {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &json!({"version": PROTOCOL_VERSION, "operations": operations()}),
            )?
        );
        return Ok(());
    }
    let token_path = matches
        .get_one::<String>("token-file")
        .ok_or_else(|| color_eyre::eyre::eyre!("--token-file or MAXOPS_TOKEN_FILE is required"))?;
    let token = Token::read(Path::new(token_path))?;
    let url = matches
        .get_one::<String>("url")
        .expect("default URL")
        .trim_end_matches('/');
    transport::validate_url(url)?;
    let client = transport::client()?;
    if operation == "operations" {
        let response: Value =
            transport::read_json(token.apply(client.get(format!("{url}/v1/operations")))).await?;
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let definition = operations()
        .into_iter()
        .find(|definition| definition.name == operation)
        .expect("registered operation");
    let params = operation_params(operation, args)?;
    let request: Request = serde_json::from_value(json!({"op": operation, "params": params}))?;
    match &request {
        Request::UnitsLogs(params) => {
            params.validate().map_err(color_eyre::eyre::Report::msg)?;
        }
        Request::UnitsStart(params)
        | Request::UnitsStop(params)
        | Request::UnitsRestart(params)
        | Request::UnitsReload(params) => {
            params.validate().map_err(color_eyre::eyre::Report::msg)?;
        }
        Request::ExecRun(params) => {
            params.validate().map_err(color_eyre::eyre::Report::msg)?;
        }
        _ => {}
    }
    let mut outbound = token
        .apply(client.post(format!("{url}/v1/execute")))
        .json(&request);
    if definition.idempotency == IdempotencyRequirement::Required {
        outbound = outbound.header(
            "idempotency-key",
            args.get_one::<String>("idempotency-key")
                .expect("required idempotency key"),
        );
    }
    let response: Value = transport::read_json(outbound).await?;
    if definition.kind == OperationKind::JobSubmission && args.get_flag("wait") {
        let handle: JobHandle = serde_json::from_value(response)?;
        let follow = args.get_flag("follow");
        let terminal = wait_for_job(&client, &token, url, handle.job_id, follow).await?;
        if follow {
            eprintln!("{}", serde_json::to_string(&terminal)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&terminal)?);
        }
        match terminal.handle.state {
            JobState::Succeeded => {}
            JobState::Failed => std::process::exit(10),
            JobState::OutcomeUnknown => std::process::exit(11),
            JobState::Cancelled => std::process::exit(12),
            JobState::TimedOut => std::process::exit(13),
            _ => unreachable!("wait returned a non-terminal job"),
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&response)?);
    }
    Ok(())
}

async fn wait_for_job(
    client: &reqwest::Client,
    token: &Token,
    url: &str,
    job_id: JobId,
    follow: bool,
) -> color_eyre::eyre::Result<JobRecord> {
    let mut revision = None;
    let mut stdout_offset = 0;
    let mut stderr_offset = 0;
    loop {
        if follow {
            let logs = fetch_logs(
                client,
                token,
                url,
                JobLogsParams {
                    job_id: job_id.clone(),
                    stdout_offset,
                    stderr_offset,
                    limit: 64 * 1024,
                },
            )
            .await?;
            std::io::stdout().write_all(&BASE64.decode(logs.stdout_base64)?)?;
            std::io::stdout().flush()?;
            std::io::stderr().write_all(&BASE64.decode(logs.stderr_base64)?)?;
            std::io::stderr().flush()?;
            stdout_offset = logs.next_stdout_offset;
            stderr_offset = logs.next_stderr_offset;
        }
        let request = Request::JobsWait(maxops_proto::JobWaitParams {
            job_id: Some(job_id.clone()),
            idempotency_key: None,
            after_revision: revision,
            timeout_seconds: 10,
        });
        let waited: Value = match transport::read_json(
            token
                .apply(client.post(format!("{url}/v1/execute")))
                .json(&request),
        )
        .await
        {
            Ok(job) => job,
            Err(error) if retryable_wait_error(&error) => {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let handle: JobHandle = serde_json::from_value(waited["job"]["handle"].clone())?;
        revision = Some(handle.revision);
        if handle.state.is_terminal() {
            let job: JobRecord = transport::read_json(
                token
                    .apply(client.post(format!("{url}/v1/execute")))
                    .json(&Request::JobsStatus(job_id.clone().into())),
            )
            .await?;
            if follow {
                let logs = fetch_logs(
                    client,
                    token,
                    url,
                    JobLogsParams {
                        job_id: job_id.clone(),
                        stdout_offset,
                        stderr_offset,
                        limit: 64 * 1024,
                    },
                )
                .await?;
                std::io::stdout().write_all(&BASE64.decode(logs.stdout_base64)?)?;
                std::io::stderr().write_all(&BASE64.decode(logs.stderr_base64)?)?;
            }
            return Ok(job);
        }
    }
}

fn retryable_wait_error(error: &color_eyre::Report) -> bool {
    transport::upstream_status(error) == Some(reqwest::StatusCode::CONFLICT)
}

async fn fetch_logs(
    client: &reqwest::Client,
    token: &Token,
    url: &str,
    params: JobLogsParams,
) -> color_eyre::eyre::Result<JobLogsResponse> {
    transport::read_json(
        token
            .apply(client.post(format!("{url}/v1/execute")))
            .json(&Request::JobsLogs(params.into())),
    )
    .await
}

fn operation_params(operation: &str, args: &ArgMatches) -> color_eyre::eyre::Result<Value> {
    if let Some(path) = args.get_one::<String>("params-file") {
        let bytes = read_params(std::fs::File::open(path)?)?;
        return parse_params(&bytes);
    }
    if args.get_flag("params-stdin") {
        let bytes = read_params(std::io::stdin().lock())?;
        return parse_params(&bytes);
    }

    let definition = operations()
        .into_iter()
        .find(|item| item.name == operation)
        .expect("registered operation");
    let schema = serde_json::to_value(definition.params_schema)?;
    let mut params = serde_json::Map::new();
    if let Some(properties) = schema["properties"].as_object() {
        for (name, property) in properties {
            match scalar_type(property) {
                Some("integer") => {
                    if let Some(value) = args.get_one::<u64>(name) {
                        params.insert(name.clone(), json!(value));
                    }
                }
                Some("string") => {
                    if let Some(value) = args.get_one::<String>(name) {
                        params.insert(name.clone(), json!(value));
                    }
                }
                _ => {}
            }
        }
    }
    Ok(Value::Object(params))
}

fn scalar_type(property: &Value) -> Option<&str> {
    match property.get("type")? {
        Value::String(kind) if matches!(kind.as_str(), "string" | "integer") => Some(kind),
        Value::Array(kinds) if kinds.len() == 2 && kinds.iter().any(|kind| kind == "null") => {
            kinds.iter().find_map(|kind| match kind.as_str() {
                Some(kind @ ("string" | "integer")) => Some(kind),
                _ => None,
            })
        }
        _ => None,
    }
}

fn read_params(reader: impl Read) -> color_eyre::eyre::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(MAX_PARAMS_BYTES + 1).read_to_end(&mut bytes)?;
    color_eyre::eyre::ensure!(
        bytes.len() as u64 <= MAX_PARAMS_BYTES,
        "params JSON exceeds 1 MiB"
    );
    Ok(bytes)
}

fn parse_params(bytes: &[u8]) -> color_eyre::eyre::Result<Value> {
    let value: Value = serde_json::from_slice(bytes)?;
    color_eyre::eyre::ensure!(value.is_object(), "params JSON must be an object");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cli_is_generated_with_required_and_integer_parameters() {
        cli().debug_assert();
        assert!(
            cli()
                .try_get_matches_from(["maxopsctl", "host.facts"])
                .is_err()
        );
        assert!(
            cli()
                .try_get_matches_from([
                    "maxopsctl",
                    "units.logs",
                    "--host",
                    "a",
                    "--unit",
                    "a.service",
                    "--lines",
                    "invalid"
                ])
                .is_err()
        );
        assert!(
            cli()
                .try_get_matches_from(["maxopsctl", "units.failed"])
                .is_ok()
        );
        assert!(
            cli()
                .try_get_matches_from(["maxopsctl", "units.restart"])
                .is_err()
        );
        assert!(
            cli()
                .try_get_matches_from(["maxopsctl", "host.facts", "--params-stdin"])
                .is_ok()
        );
        assert!(
            cli()
                .try_get_matches_from(["maxopsctl", "host.facts", "--params-stdin", "--host", "a"])
                .is_err()
        );
    }

    #[test]
    fn complete_params_input_requires_a_json_object() {
        assert_eq!(parse_params(br#"{"host":"a"}"#).unwrap()["host"], "a");
        assert!(parse_params(br#"["a"]"#).is_err());
        assert!(parse_params(b"not-json").is_err());
    }

    #[test]
    fn optional_schema_unions_do_not_become_invalid_short_flags() {
        let matches = cli()
            .try_get_matches_from(["maxopsctl", "units.failed", "--host", "alpha"])
            .unwrap();
        let (operation, args) = matches.subcommand().unwrap();
        assert_eq!(
            operation_params(operation, args).unwrap(),
            json!({"host":"alpha"})
        );
    }

    #[test]
    fn params_file_supports_nested_values_and_replaces_short_flags() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("params.json");
        std::fs::write(&path, br#"{"host":"a","nested":{"items":[1,2]}}"#).unwrap();
        let matches = cli()
            .try_get_matches_from([
                "maxopsctl",
                "host.facts",
                "--params-file",
                path.to_str().unwrap(),
            ])
            .unwrap();
        let (operation, args) = matches.subcommand().unwrap();
        let params = operation_params(operation, args).unwrap();
        assert_eq!(params["nested"]["items"][1], 2);
        assert!(
            serde_json::from_value::<Request>(json!({"op":operation,"params":params})).is_err(),
            "the protocol type still rejects fields not in the selected operation"
        );
    }

    #[test]
    fn wait_only_retries_projection_conflicts() {
        use maxops_proto::transport::UpstreamHttpError;

        fn report(status: reqwest::StatusCode) -> color_eyre::Report {
            UpstreamHttpError::new(status).into()
        }

        assert!(retryable_wait_error(&report(reqwest::StatusCode::CONFLICT)));
        assert!(!retryable_wait_error(&report(
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        )));
    }
}
