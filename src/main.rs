use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod client;
mod commands;
mod config;
mod token_cache;

#[derive(Parser)]
#[command(name = "justice-link")]
#[command(about = "Salesforce data extraction tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List all Salesforce objects visible to the authenticated user
    List,
    /// Describe the fields of a single Salesforce object
    Describe {
        /// API name of the Salesforce object (e.g. Account, Contact)
        object: String,
    },
    /// Export a Salesforce object to a Parquet file
    Export {
        /// API name of the Salesforce object (e.g. Account, Contact)
        object: Option<String>,
        /// Limit the number of rows fetched (defaults to all)
        #[arg(short, long)]
        limit: Option<usize>,
        /// Output path — can be a file or a directory. Defaults to {object}.parquet in CWD.
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Comma-separated list of fields to export. Defaults to all supported fields.
        #[arg(short, long, value_delimiter = ',')]
        fields: Option<Vec<String>>,
        /// Path to a TOML config file for batch export
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// Validate objects and fields without exporting data
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    let sf = client::build_client().await?;

    match cli.command {
        Commands::List => commands::list::run(&sf).await?,
        Commands::Describe { object } => commands::describe::run(&sf, &object).await?,
        Commands::Export {
            object,
            limit,
            output,
            fields,
            config,
            dry_run,
        } => {
            if let Some(config_path) = config {
                commands::export::run_batch(&sf, &config_path, dry_run).await?;
            } else if let Some(object) = object {
                commands::export::run(&sf, &object, limit, output, fields, dry_run).await?;
            } else {
                return Err(anyhow::anyhow!(
                    "Either an object name or --config must be provided. \
                     See 'justice-link export --help' for usage."
                ));
            }
        }
    }

    Ok(())
}
