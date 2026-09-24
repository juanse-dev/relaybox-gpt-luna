use std::str::FromStr;
use std::{env, env::VarError};

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let database_url = configured("RELAYBOX_DATABASE_URL", "sqlite://relaybox.db")?;
    let bind = configured("RELAYBOX_BIND", "127.0.0.1:3000")?;
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

fn configured(name: &str, default: &str) -> Result<String, VarError> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => Err(error),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{env, ffi::OsString, os::unix::ffi::OsStringExt};

    use super::configured;

    #[test]
    fn non_unicode_database_url_fails_configuration() {
        const NAME: &str = "RELAYBOX_DATABASE_URL";
        let previous = env::var_os(NAME);
        env::set_var(NAME, OsString::from_vec(vec![0xff]));

        let result = configured(NAME, "sqlite://fallback.db");

        match previous {
            Some(value) => env::set_var(NAME, value),
            None => env::remove_var(NAME),
        }
        assert!(matches!(result, Err(env::VarError::NotUnicode(_))));
    }
}
