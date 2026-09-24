use std::sync::Arc;

use async_trait::async_trait;
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
    Inserted(Delivery),
    Existing(Delivery),
}

#[async_trait]
pub trait DeliveryRepository: Send + Sync {
    async fn enqueue(&self, delivery: NewDelivery) -> Result<EnqueueOutcome, ApplicationError>;
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
        let outcome = self
            .repository
            .enqueue(NewDelivery {
                idempotency_key,
                target_url: target_url.clone(),
                payload: payload.clone(),
            })
            .await?;
        match outcome {
            EnqueueOutcome::Inserted(delivery) => Ok((delivery, true)),
            EnqueueOutcome::Existing(delivery)
                if delivery.target_url == target_url && delivery.payload == payload =>
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
