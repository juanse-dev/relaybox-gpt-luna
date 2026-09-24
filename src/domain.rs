use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize)]
pub struct Delivery {
    pub id: Uuid,
    pub status: String,
    pub attempts: i64,
    pub target_url: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct NewDelivery {
    pub idempotency_key: String,
    pub target_url: String,
    pub payload: Value,
}

#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error("idempotency conflict")]
    Conflict,
    #[error("repository failure: {0}")]
    Failure(#[from] sqlx::Error),
}
