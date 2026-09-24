use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use tower::ServiceExt;

async fn setup() -> (SqlitePool, axum::Router) {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let app = relaybox::app(pool.clone()).await.unwrap();
    (pool, app)
}

async fn request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    key: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn enqueue_replay_conflict_and_query_are_durable() {
    let (pool, app) = setup().await;
    let input = json!({"target_url":"https://example.test/webhooks","payload":{"a":1,"b":2}});
    let (status, created) = request(
        &app,
        "POST",
        "/v1/deliveries",
        Some("  key-1\t"),
        input.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["status"], "pending");
    assert_eq!(created["attempts"], 0);
    let reordered = json!({"target_url":"https://example.test/webhooks","payload":{"b":2,"a":1}});
    let (status, replay) = request(&app, "POST", "/v1/deliveries", Some("key-1"), reordered).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["id"], created["id"]);

    let (status, conflict) = request(
        &app,
        "POST",
        "/v1/deliveries",
        Some("key-1"),
        json!({"target_url":"https://other.test/","payload":{"a":1}}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["error"]["code"], "idempotency_conflict");

    drop(app);
    let restarted = relaybox::app(pool).await.unwrap();
    let (status, queried) = request(
        &restarted,
        "GET",
        &format!("/v1/deliveries/{}", created["id"].as_str().unwrap()),
        None,
        json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(queried, created);
}

#[tokio::test]
async fn validates_requests_and_handles_concurrent_same_key() {
    let (_, app) = setup().await;
    let (status, _) = request(
        &app,
        "POST",
        "/v1/deliveries",
        None,
        json!({"target_url":"https://example.test","payload":null}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, invalid_url) = request(
        &app,
        "POST",
        "/v1/deliveries",
        Some("k"),
        json!({"target_url":"ftp://example.test","payload":null}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(invalid_url["error"]["code"], "invalid_target_url");
    let (status, _) = request(&app, "GET", "/v1/deliveries/not-a-uuid", None, json!(null)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let app = std::sync::Arc::new(app);
    let mut requests = Vec::new();
    for _ in 0..8 {
        let app = app.clone();
        requests.push(tokio::spawn(async move {
            request(
                &app,
                "POST",
                "/v1/deliveries",
                Some("racing-key"),
                json!({"target_url":"https://example.test","payload":[1,true]}),
            )
            .await
        }));
    }
    let mut ids = Vec::new();
    let mut created = 0;
    for task in requests {
        let (status, value) = task.await.unwrap();
        assert!(status == StatusCode::CREATED || status == StatusCode::OK);
        if status == StatusCode::CREATED {
            created += 1;
        }
        ids.push(value["id"].clone());
    }
    assert_eq!(created, 1);
    assert!(ids.iter().all(|id| id == &ids[0]));
}
