use clap::{Arg, Command};
use maxops_proto::{
    Request, operations,
    transport::{self, Token},
};
use serde_json::{Value, json};
use std::path::Path;

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
        let mut subcommand = Command::new(operation.name).about(operation.summary);
        let schema = serde_json::to_value(operation.params_schema).expect("serializable schema");
        if let Some(properties) = schema["properties"].as_object() {
            for (name, property) in properties {
                let required = schema["required"]
                    .as_array()
                    .is_some_and(|items| items.iter().any(|item| item == name));
                let mut arg = Arg::new(name.clone())
                    .long(name.replace('_', "-"))
                    .required(required);
                if property["type"] == "integer" {
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
            serde_json::to_string_pretty(&json!({"version": 1, "operations": operations()}))?
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
    let request = if operation == "operations" {
        token.apply(client.get(format!("{url}/v1/operations")))
    } else {
        let definition = operations()
            .into_iter()
            .find(|item| item.name == operation)
            .expect("registered operation");
        let schema = serde_json::to_value(definition.params_schema)?;
        let mut params = serde_json::Map::new();
        if let Some(properties) = schema["properties"].as_object() {
            for (name, property) in properties {
                if property["type"] == "integer" {
                    if let Some(value) = args.get_one::<u64>(name) {
                        params.insert(name.clone(), json!(value));
                    }
                } else if let Some(value) = args.get_one::<String>(name) {
                    params.insert(name.clone(), json!(value));
                }
            }
        }
        let request: Request = serde_json::from_value(json!({"op": operation, "params": params}))?;
        if let Request::UnitsLogs(params) = &request {
            params.validate().map_err(color_eyre::eyre::Report::msg)?;
        }
        token
            .apply(client.post(format!("{url}/v1/execute")))
            .json(&request)
    };
    let response: Value = transport::read_json(request).await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
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
    }
}
