use clap::Parser;
use maxops_executor::{RunnerSpec, run_spec, write_result_atomic};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Internal maxops durable job wrapper")]
struct Args {
    #[arg(long, conflicts_with = "credential")]
    spec: Option<PathBuf>,
    #[arg(long, conflicts_with = "spec")]
    credential: Option<String>,
    #[arg(long)]
    output_directory: PathBuf,
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    color_eyre::install()?;
    let args = Args::parse();
    let spec_path = match (args.spec, args.credential) {
        (Some(path), None) => path,
        (None, Some(name)) => {
            let directory = std::env::var_os("CREDENTIALS_DIRECTORY")
                .ok_or_else(|| color_eyre::eyre::eyre!("missing CREDENTIALS_DIRECTORY"))?;
            PathBuf::from(directory).join(name)
        }
        _ => return Err(color_eyre::eyre::eyre!("one spec source is required")),
    };
    let spec: RunnerSpec = serde_json::from_slice(&tokio::fs::read(spec_path).await?)?;
    let result = run_spec(&spec, &args.output_directory).await?;
    write_result_atomic(&args.output_directory.join("result.json"), &result).await?;
    if result.success {
        Ok(())
    } else {
        std::process::exit(result.exit_code.unwrap_or(1));
    }
}
