pub mod aws;
pub mod commands;
pub mod engine;
pub mod state;
pub mod transform;
pub mod ui;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// DynamoDB Migration Tool - migrate data between DynamoDB tables with
/// template transformations, state management, and rollback support.
#[derive(Parser)]
#[command(
    name = "ddbm",
    version = env!("CARGO_PKG_VERSION"),
    about = "DynamoDB Migration Tool",
    long_about = "A powerful CLI for migrating data between DynamoDB tables with\n\
                   template transformations, state management, and rollback support.",
    after_help = "EXAMPLES:\n\
                    # Start an interactive migration\n\
                    ddbm migrate\n\n\
                    # Copy all data from users-old to users-new (non-interactive)\n\
                    ddbm migrate --source users-old --target users-new --passthrough\n\n\
                    # Migrate with custom mappings and exclusions\n\
                    ddbm migrate -s users -t users-v2 -m mappings.json -e \"temp_id,raw_data\"\n\n\
                    # Undo a specific migration\n\
                    ddbm undo --id migration_20240428_120000"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Directory for state files (migration_state.json, undo_state.json)
    #[arg(long, global = true, default_value = ".")]
    pub state_dir: PathBuf,

    /// Log file path
    #[arg(long, global = true, default_value = "migration.log")]
    pub log_file: PathBuf,

    /// Enable verbose console logging
    #[arg(short, long, global = true)]
    pub verbose: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start a new migration (interactive by default)
    #[command(after_help = "EXAMPLES:\n\
                      # Interactive mode\n\
                      ddbm migrate\n\n\
                      # Passthrough mode (copy everything)\n\n\
                      ddbm migrate -s SourceTable -t TargetTable -p\n\n\
                      # Specific mappings from JSON\n\n\
                      ddbm migrate -s SourceTable -t TargetTable -m mappings.json")]
    Migrate {
        /// Source DynamoDB table name (non-interactive mode)
        #[arg(short, long)]
        source: Option<String>,

        /// Target DynamoDB table name (non-interactive mode)
        #[arg(short, long)]
        target: Option<String>,

        /// Path to JSON mappings file (non-interactive mode)
        #[arg(short, long)]
        mappings: Option<PathBuf>,

        /// Use passthrough mode - copy all attributes (non-interactive mode)
        #[arg(short, long)]
        passthrough: bool,

        /// Comma-separated columns to exclude in passthrough mode
        #[arg(short, long)]
        exclude: Option<String>,
    },

    /// Resume an existing migration
    #[command(after_help = "EXAMPLES:\n\
                      # Interactively select a migration to resume\n\
                      ddbm resume\n\n\
                      # Resume a specific migration job\n\
                      ddbm resume --id migration_20240428_120000")]
    Resume {
        /// Migration ID to resume (interactive if omitted)
        #[arg(short, long)]
        id: Option<String>,
    },

    /// Undo a completed migration (rollback)
    #[command(after_help = "EXAMPLES:\n\
                      # Interactively select a migration to undo\n\
                      ddbm undo\n\n\
                      # Undo a specific migration job\n\
                      ddbm undo --id migration_20240428_120000")]
    Undo {
        /// Migration ID to undo (interactive if omitted)
        #[arg(short, long)]
        id: Option<String>,
    },

    /// List all migration jobs and their current status
    List,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    init_logging(&cli.log_file, cli.verbose)?;

    // Ensure state directory exists
    if !cli.state_dir.exists() {
        std::fs::create_dir_all(&cli.state_dir)?;
    }

    let state_dir = cli.state_dir.canonicalize()?;

    match cli.command {
        Commands::Migrate {
            source,
            target,
            mappings,
            passthrough,
            exclude,
        } => {
            let client = aws::client::create_client().await?;

            if source.is_some() && target.is_some() {
                // Non-interactive mode
                commands::migrate::run_non_interactive(
                    &client,
                    &state_dir,
                    source.as_deref().unwrap(),
                    target.as_deref().unwrap(),
                    mappings.as_deref(),
                    passthrough,
                    exclude.as_deref(),
                )
                .await?;
            } else if source.is_some() || target.is_some() {
                anyhow::bail!(
                    "Both --source and --target must be provided for non-interactive mode"
                );
            } else {
                // Interactive mode
                commands::migrate::run_interactive(&client, &state_dir).await?;
            }
        }

        Commands::Resume { id } => {
            let client = aws::client::create_client().await?;
            commands::resume::run(&client, &state_dir, id.as_deref()).await?;
        }

        Commands::Undo { id } => {
            let client = aws::client::create_client().await?;
            commands::undo::run(&client, &state_dir, id.as_deref()).await?;
        }

        Commands::List => {
            commands::list::run(&state_dir)?;
        }
    }

    Ok(())
}

/// Initialize tracing subscriber with file and optional console output.
fn init_logging(log_file: &std::path::Path, verbose: bool) -> Result<()> {
    let log_dir = log_file
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let log_filename = log_file
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("migration.log"));

    let file_appender = tracing_appender::rolling::never(log_dir, log_filename);

    let file_layer = fmt::layer()
        .with_writer(file_appender)
        .with_ansi(false)
        .with_target(false);

    let filter = if verbose {
        EnvFilter::new("ddbm=debug")
    } else {
        EnvFilter::new("ddbm=info")
    };

    let registry = tracing_subscriber::registry().with(filter).with(file_layer);

    if verbose {
        let console_layer = fmt::layer().with_target(false).with_level(true).compact();
        registry.with(console_layer).init();
    } else {
        registry.init();
    }

    Ok(())
}
