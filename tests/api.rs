use axum::{
    body::Body,
    http::{HeaderValue, Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use std::str::FromStr;
use tower::ServiceExt;

async fn setup() -> (SqlitePool, axum::Router, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("relaybox-{}.db", uuid::Uuid::new_v4()));
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .unwrap();
    let app = relaybox::app(pool.clone()).await.unwrap();
    (pool, app, path)
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
    let path = std::env::temp_dir().join(format!("relaybox-{}.db", uuid::Uuid::new_v4()));
    let database_url = format!("sqlite://{}", path.display());
    let options = SqliteConnectOptions::from_str(&database_url)
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options.clone())
        .await
        .unwrap();
    let app = relaybox::app(pool.clone()).await.unwrap();
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
    pool.close().await;
    let restarted_pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .unwrap();
    let restarted = relaybox::app(restarted_pool.clone()).await.unwrap();
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
    restarted_pool.close().await;
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn preserves_large_json_numbers_and_utf8_idempotency_keys() {
    let (pool, app, path) = setup().await;
    let enqueue = |number: &str| {
        Request::builder()
            .method("POST")
            .uri("/v1/deliveries")
            .header("content-type", "application/json")
            .header(
                "Idempotency-Key",
                HeaderValue::from_bytes("café".as_bytes()).unwrap(),
            )
            .body(Body::from(format!(
                "{{\"target_url\":\"https://example.test/\",\"payload\":{number}}}"
            )))
            .unwrap()
    };
    let created = app
        .clone()
        .oneshot(enqueue("18446744073709551616"))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let bytes = created.into_body().collect().await.unwrap().to_bytes();
    let delivery: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(delivery["payload"].to_string(), "18446744073709551616");

    let conflict = app
        .clone()
        .oneshot(enqueue("18446744073709551617"))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    drop(app);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn validates_requests_and_handles_concurrent_same_key() {
    let (pool, app, path) = setup().await;
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
    let (status, missing_host) = request(
        &app,
        "POST",
        "/v1/deliveries",
        Some("missing-host"),
        json!({"target_url":"https:///path","payload":null}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(missing_host["error"]["code"], "invalid_target_url");
    let malformed_json = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/deliveries")
                .header("content-type", "application/json")
                .header("Idempotency-Key", "malformed-json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed_json.status(), StatusCode::BAD_REQUEST);
    assert_eq!(invalid_url["error"]["code"], "invalid_target_url");
    let (status, _) = request(
        &app,
        "POST",
        "/v1/deliveries",
        Some("typed-url"),
        json!({"target_url":123,"payload":null}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = request(&app, "GET", "/v1/deliveries/not-a-uuid", None, json!(null)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let malformed_path = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/deliveries/%FF")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed_path.status(), StatusCode::NOT_FOUND);
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

    let oversized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/deliveries")
                .header("content-type", "application/json")
                .header("Idempotency-Key", "oversized")
                .body(Body::from(vec![b' '; 2 * 1024 * 1024 + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        oversized.headers().get("content-type").unwrap(),
        "application/json"
    );
    let error: Value =
        serde_json::from_slice(&oversized.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(error["error"]["code"], "invalid_request");

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
    drop(app);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}
