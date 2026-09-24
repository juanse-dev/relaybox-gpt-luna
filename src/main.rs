use std::env;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let database_url =
        env::var("RELAYBOX_DATABASE_URL").unwrap_or_else(|_| "sqlite://relaybox.db".to_owned());
    let bind = env::var("RELAYBOX_BIND").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let options = SqliteConnectOptions::from_str(&database_url)?.create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    let app = relaybox::app(pool).await?;
    tracing::info!(address = %listener.local_addr()?, "Relaybox listening");
    axum::serve(listener, app).await?;
    Ok(())
}
