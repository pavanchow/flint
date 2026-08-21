use clap::{Parser, Subcommand};
use flint::server::{self, Config};
use flint::Store;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "flint", version, about = "An embedded, single-binary key-value store: a persistent append-only log with an in-memory index.")]
struct Cli {
    /// Data directory (created if absent).
    #[arg(long, default_value = "flint-data", global = true)]
    dir: String,
    /// Segment size in bytes before the active log rolls over.
    #[arg(long, default_value_t = 64 * 1024 * 1024, global = true)]
    seg_cap: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve the store over a TCP line protocol.
    Serve {
        #[arg(long, default_value_t = 6380)]
        port: u16,
        /// fsync every write before replying (power-loss durable, slower).
        #[arg(long)]
        fsync: bool,
    },
    /// Run as an MCP server over stdio so an agent can use Flint as scratch memory.
    Mcp,
    /// Set a key to a value, then exit.
    Set { key: String, value: String },
    /// Get a key and print its value, then exit. Exit 1 if absent.
    Get { key: String },
    /// Delete a key, then exit.
    Del { key: String },
    /// Compact the store on disk, then exit.
    Compact,
    /// Print the number of live keys.
    Len,
}

fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let store = Store::open(&cli.dir, cli.seg_cap)?;
    match cli.cmd {
        Cmd::Serve { port, fsync } => {
            store.set_sync_writes(fsync);
            let store = Arc::new(store);
            server::serve(
                store,
                Config { port, compact_at_segments: 8, compact_interval: Duration::from_secs(30) },
            )?;
        }
        Cmd::Set { key, value } => {
            store.set(key.as_bytes(), value.as_bytes())?;
            store.flush()?;
            println!("OK");
        }
        Cmd::Get { key } => match store.get(key.as_bytes())? {
            Some(v) => {
                use std::io::Write;
                std::io::stdout().write_all(&v)?;
                println!();
            }
            None => std::process::exit(1),
        },
        Cmd::Del { key } => {
            let hit = store.delete(key.as_bytes())?;
            store.flush()?;
            println!("{}", if hit { 1 } else { 0 });
        }
        Cmd::Compact => {
            store.compact()?;
            println!("OK");
        }
        Cmd::Mcp => flint::mcp::serve_mcp(Arc::new(store))?,
        Cmd::Len => println!("{}", store.len()),
    }
    Ok(())
}
