use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::{
    application::DeliveryRepository,
    domain::{Delivery, NewDelivery, RepositoryError},
};

pub struct SqliteDeliveryRepository {
    pool: SqlitePool,
}

impl SqliteDeliveryRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn by_key(&self, key: &str) -> Result<Delivery, sqlx::Error> {
        let row = sqlx::query("SELECT id, status, attempts, target_url, payload, created_at FROM deliveries WHERE idempotency_key = ?")
            .bind(key).fetch_one(&self.pool).await?;
        decode(row)
    }
}

#[async_trait]
impl DeliveryRepository for SqliteDeliveryRepository {
    async fn enqueue(&self, new: NewDelivery) -> Result<(Delivery, bool), RepositoryError> {
        let id = Uuid::new_v4();
        let created_at = Utc::now();
        let payload =
            serde_json::to_string(&new.payload).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let result = sqlx::query("INSERT INTO deliveries (id, idempotency_key, status, attempts, target_url, payload, created_at) VALUES (?, ?, 'pending', 0, ?, ?, ?)")
            .bind(id.to_string()).bind(&new.idempotency_key).bind(&new.target_url).bind(payload).bind(created_at.to_rfc3339())
            .execute(&self.pool).await;
        match result {
            Ok(_) => Ok((
                Delivery {
                    id,
                    status: "pending".into(),
                    attempts: 0,
                    target_url: new.target_url,
                    payload: new.payload,
                    created_at,
                },
                true,
            )),
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
                let existing = self.by_key(&new.idempotency_key).await?;
                if existing.target_url == new.target_url && existing.payload == new.payload {
                    Ok((existing, false))
                } else {
                    Err(RepositoryError::Conflict)
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn get(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError> {
        let row = sqlx::query("SELECT id, status, attempts, target_url, payload, created_at FROM deliveries WHERE id = ?")
            .bind(id.to_string()).fetch_optional(&self.pool).await?;
        row.map(decode).transpose().map_err(Into::into)
    }
}

fn decode(row: sqlx::sqlite::SqliteRow) -> Result<Delivery, sqlx::Error> {
    let id: String = row.try_get("id")?;
    let payload: String = row.try_get("payload")?;
    let created_at: String = row.try_get("created_at")?;
    Ok(Delivery {
        id: Uuid::parse_str(&id).map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
        status: row.try_get("status")?,
        attempts: row.try_get("attempts")?,
        target_url: row.try_get("target_url")?,
        payload: serde_json::from_str::<Value>(&payload)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
        created_at: DateTime::parse_from_rfc3339(&created_at)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?
            .with_timezone(&Utc),
    })
}
