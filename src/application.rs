use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::domain::{Delivery, NewDelivery, RepositoryError};

#[async_trait]
pub trait DeliveryRepository: Send + Sync {
    async fn enqueue(&self, delivery: NewDelivery) -> Result<(Delivery, bool), RepositoryError>;
    async fn get(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError>;
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
    ) -> Result<(Delivery, bool), RepositoryError> {
        self.repository
            .enqueue(NewDelivery {
                idempotency_key,
                target_url,
                payload,
            })
            .await
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError> {
        self.repository.get(id).await
    }
}
