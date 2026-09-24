pub mod api;
pub mod application;
pub mod domain;
pub mod infrastructure;

use std::sync::Arc;

use application::DeliveryService;
use axum::Router;
use infrastructure::SqliteDeliveryRepository;
use sqlx::SqlitePool;

pub async fn app(pool: SqlitePool) -> Result<Router, sqlx::migrate::MigrateError> {
    sqlx::migrate!().run(&pool).await?;
    let repository = Arc::new(SqliteDeliveryRepository::new(pool));
    Ok(api::router(Arc::new(DeliveryService::new(repository))))
}
