use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, PathRejection},
        Path, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use url::Url;
use uuid::Uuid;

use crate::application::{ApplicationError, DeliveryService};

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
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let key = match headers
        .get("Idempotency-Key")
        .and_then(|value| std::str::from_utf8(value.as_bytes()).ok())
    {
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
    let body = match body {
        Ok(body) => body,
        Err(rejection) => {
            let status = rejection.status();
            return error(status, "invalid_request", "Request body could not be read");
        }
    };
    let mut request: Value = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Request body must be valid JSON",
            )
        }
    };
    let Some(target_url) = request
        .get("target_url")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_target_url",
            "target_url must be an absolute HTTP or HTTPS URL with a host",
        );
    };
    let Some(payload) = request
        .as_object_mut()
        .and_then(|object| object.remove("payload"))
    else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "payload is required",
        );
    };
    match Url::parse(&target_url) {
        Ok(url) if matches!(url.scheme(), "http" | "https") && url.host().is_some() => (),
        _ => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_target_url",
                "target_url must be an absolute HTTP or HTTPS URL with a host",
            )
        }
    }
    match service.enqueue(key.to_owned(), target_url, payload).await {
        Ok((delivery, created)) => (
            if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            Json(delivery),
        )
            .into_response(),
        Err(ApplicationError::Conflict) => error(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "Idempotency-Key was already used with different content",
        ),
        Err(ApplicationError::Persistence(detail)) => {
            tracing::error!(%detail, "delivery persistence failed");
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
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let Path(id) = match path {
        Ok(path) => path,
        Err(_) => {
            return error(
                StatusCode::NOT_FOUND,
                "delivery_not_found",
                "Delivery not found",
            )
        }
    };
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
        Err(ApplicationError::Persistence(detail)) => {
            tracing::error!(%detail, "delivery query failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "An internal error occurred",
            )
        }
        Err(ApplicationError::Conflict) => error(
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
