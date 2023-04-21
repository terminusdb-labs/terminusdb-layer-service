use clap::Parser;

mod manager;
mod server;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    primary: String,
    #[arg(short, long)]
    local: String,
    #[arg(short, long)]
    upload: String,
    #[arg(short, long)]
    scratch: String,
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    server::serve(
        args.primary,
        args.local,
        args.upload,
        args.scratch,
        args.port,
    )
    .await?;

    Ok(())
}
