use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use url::Url;
use uuid::Uuid;

use crate::{application::DeliveryService, domain::RepositoryError};

#[derive(Deserialize)]
struct EnqueueRequest {
    target_url: String,
    payload: Value,
}

pub fn router(service: Arc<DeliveryService>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/deliveries", post(enqueue))
        .route("/v1/deliveries/:id", get(get_delivery))
        .with_state(service)
}

async fn health() -> Json<Value> {
    Json(json!({"status":"ok"}))
}

async fn enqueue(
    State(service): State<Arc<DeliveryService>>,
    headers: HeaderMap,
    body: Result<Json<EnqueueRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let key = match headers.get("Idempotency-Key").and_then(|v| v.to_str().ok()) {
        Some(value) => value.trim_matches(|c: char| c.is_ascii_whitespace()),
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_idempotency_key",
                "Idempotency-Key is required",
            )
        }
    };
    if key.is_empty() || key.len() > 128 {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "Idempotency-Key must be 1 to 128 bytes after trimming",
        );
    }
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Request body must be valid JSON",
            )
        }
    };
    match Url::parse(&request.target_url) {
        Ok(url) if matches!(url.scheme(), "http" | "https") && url.host().is_some() => (),
        _ => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_target_url",
                "target_url must be an absolute HTTP or HTTPS URL with a host",
            )
        }
    }
    match service
        .enqueue(key.to_owned(), request.target_url, request.payload)
        .await
    {
        Ok((delivery, created)) => (
            if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            Json(delivery),
        )
            .into_response(),
        Err(RepositoryError::Conflict) => error(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "Idempotency-Key was already used with different content",
        ),
        Err(RepositoryError::Failure(e)) => {
            tracing::error!(error = %e, "delivery persistence failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "An internal error occurred",
            )
        }
    }
}

async fn get_delivery(
    State(service): State<Arc<DeliveryService>>,
    Path(id): Path<String>,
) -> Response {
    let id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => {
            return error(
                StatusCode::NOT_FOUND,
                "delivery_not_found",
                "Delivery not found",
            )
        }
    };
    match service.get(id).await {
        Ok(Some(delivery)) => Json(delivery).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "delivery_not_found",
            "Delivery not found",
        ),
        Err(RepositoryError::Failure(e)) => {
            tracing::error!(error = %e, "delivery query failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "An internal error occurred",
            )
        }
        Err(RepositoryError::Conflict) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "An internal error occurred",
        ),
    }
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error":{"code":code,"message":message}})),
    )
        .into_response()
}
