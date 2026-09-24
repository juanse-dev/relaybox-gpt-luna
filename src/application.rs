use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::domain::{Delivery, NewDelivery};

#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("idempotency conflict")]
    Conflict,
    #[error("persistence operation failed: {0}")]
    Persistence(String),
}

#[derive(Debug)]
pub enum EnqueueOutcome {
    Inserted { id: Uuid, created_at: DateTime<Utc> },
    Existing(Delivery),
}

#[async_trait]
pub trait DeliveryRepository: Send + Sync {
    async fn enqueue(&self, delivery: &NewDelivery) -> Result<EnqueueOutcome, ApplicationError>;
    async fn get(&self, id: Uuid) -> Result<Option<Delivery>, ApplicationError>;
}

#[derive(Clone)]
pub struct DeliveryService {
    repository: Arc<dyn DeliveryRepository>,
}

impl DeliveryService {
    pub fn new(repository: Arc<dyn DeliveryRepository>) -> Self {
        Self { repository }
    }

    pub async fn enqueue(
        &self,
        idempotency_key: String,
        target_url: String,
        payload: Value,
    ) -> Result<(Delivery, bool), ApplicationError> {
        let new_delivery = NewDelivery {
            idempotency_key,
            target_url,
            payload,
        };
        let outcome = self.repository.enqueue(&new_delivery).await?;
        match outcome {
            EnqueueOutcome::Inserted { id, created_at } => Ok((
                Delivery {
                    id,
                    status: "pending".into(),
                    attempts: 0,
                    target_url: new_delivery.target_url,
                    payload: new_delivery.payload,
                    created_at,
                },
                true,
            )),
            EnqueueOutcome::Existing(delivery)
                if delivery.target_url == new_delivery.target_url
                    && delivery.payload == new_delivery.payload =>
            {
                Ok((delivery, false))
            }
            EnqueueOutcome::Existing(_) => Err(ApplicationError::Conflict),
        }
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Delivery>, ApplicationError> {
        self.repository.get(id).await
    }
}
