use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use dfc_aivcs::{AivcsClient, MockAivcsClient, ReviewDecisionPayload};
use dfc_core::{
    CorrelateRequest, CorrelationKind, CorrelationRecord, DfcError, InboundAivcsEvent,
    InboundHitlEvent, ReplayRequest, RollbackRequest, TenantContext, SCHEMA_VERSION,
};
use dfc_data_fabric::{DataFabricClient, EventIngestService, IngestOutcome, MockDataFabricClient};
use dfc_hitl::{ReviewBundleAssembler, ReviewDecisionRequest, ReviewDecisionResponse};
use dfc_replay::{AuditContext, ReplayBridge};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::info;

mod config;

use config::ServerConfig;

#[derive(Clone)]
struct AppState {
    git_sha: &'static str,
    public_fqdn: String,
    public_url: String,
    data_fabric: Arc<MockDataFabricClient>,
    ingest: Arc<EventIngestService<MockDataFabricClient>>,
    aivcs: Arc<MockAivcsClient>,
}

#[derive(Serialize)]
struct VersionResponse {
    service: &'static str,
    schema_version: &'static str,
    git_sha: &'static str,
    fqdn: String,
    public_url: String,
}

#[derive(Serialize)]
struct ReadinessResponse {
    ready: bool,
    mode: &'static str,
}

#[derive(Serialize, Deserialize)]
struct IngestResponse {
    event: dfc_core::DfcEvent,
}

#[derive(Serialize)]
struct StagedIngestResponse {
    staged: bool,
    event_id: String,
    pending: Vec<dfc_core::PendingCorrelation>,
}

#[derive(Serialize, Deserialize)]
struct ErrorBody {
    error: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("dfc=info".parse()?),
        )
        .init();

    let cfg = ServerConfig::from_env();
    info!(
        port = cfg.port,
        fqdn = %cfg.public_fqdn,
        "dfc-server starting"
    );

    let data_fabric = Arc::new(MockDataFabricClient::default());
    let ingest = Arc::new(EventIngestService::new(data_fabric.clone()));

    let state = AppState {
        git_sha: option_env!("GIT_SHA").unwrap_or("dev"),
        public_fqdn: cfg.public_fqdn.clone(),
        public_url: cfg.public_url(),
        data_fabric,
        ingest,
        aivcs: Arc::new(MockAivcsClient),
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/version", get(version))
        .route("/v1/correlate", post(correlate_create))
        .route("/v1/correlate/{kind}/{id}", get(correlate_get))
        .route("/v1/events/aivcs", post(events_aivcs))
        .route("/v1/events/hitl", post(events_hitl))
        .route("/v1/hitl/reviews/{review_id}", get(hitl_review_get))
        .route(
            "/v1/hitl/reviews/{review_id}/decision",
            post(hitl_review_decision),
        )
        .route("/v1/replay/request", post(replay_request))
        .route("/v1/rollback/request", post(rollback_request))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(cfg.bind_addr()).await?;
    info!(addr = %cfg.bind_addr(), "listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    let _ = state.data_fabric.as_ref();
    Json(ReadinessResponse {
        ready: true,
        mode: "mock-upstreams",
    })
}

async fn version(State(state): State<AppState>) -> Json<VersionResponse> {
    Json(VersionResponse {
        service: "dfc",
        schema_version: SCHEMA_VERSION,
        git_sha: state.git_sha,
        fqdn: state.public_fqdn.clone(),
        public_url: state.public_url.clone(),
    })
}

fn tenant_from_headers(headers: &HeaderMap) -> Result<TenantContext, ApiError> {
    headers
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| TenantContext::new(s.to_string()))
        .ok_or_else(|| ApiError::BadRequest("X-Tenant-Id header is required".into()))
}

fn validate_route_id<'a>(s: &'a str, field_name: &str) -> Result<&'a str, ApiError> {
    if s.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == ':' || c == '-') {
        Ok(s)
    } else {
        Err(ApiError::BadRequest(format!(
            "{field_name} contains forbidden characters (allowed: alphanumeric, '.', '_', ':', '-')"
        )))
    }
}

fn actor_from_headers(headers: &HeaderMap) -> String {
    headers
        .get("x-actor")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("system")
        .to_string()
}

async fn correlate_create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut body): Json<CorrelateRequest>,
) -> Result<Json<CorrelationRecord>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    if body.tenant_id.is_empty() {
        body.tenant_id = tenant.tenant_id.clone();
    }
    tenant.ensure(&body.tenant_id)?;

    if body.tenant_id.trim().is_empty() {
        return Err(ApiError::BadRequest("tenant_id is required".into()));
    }

    if let Some(run_id) = &body.data_fabric_run_id {
        validate_route_id(run_id, "run_id")?;
    }
    if let Some(task_id) = &body.data_fabric_task_id {
        validate_route_id(task_id, "task_id")?;
    }

    body.validate().map_err(ApiError::from)?;

    let record = CorrelationRecord::from(body);
    state
        .data_fabric
        .store_correlation(&record)
        .await
        .map_err(ApiError::from)?;

    reconcile_record(&state, &record).await?;

    Ok(Json(record))
}

async fn reconcile_record(state: &AppState, record: &CorrelationRecord) -> Result<(), ApiError> {
    if let Some(run_id) = &record.data_fabric_run_id {
        state
            .ingest
            .reconcile_correlation(&record.tenant_id, "run", run_id)
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(snapshot_id) = &record.aivcs_snapshot_id {
        state
            .ingest
            .reconcile_correlation(&record.tenant_id, "snapshot", snapshot_id)
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(review_id) = record.links.get("review_id").and_then(|v| v.as_str()) {
        state
            .ingest
            .reconcile_correlation(&record.tenant_id, "review", review_id)
            .await
            .map_err(ApiError::from)?;
    }
    Ok(())
}

async fn correlate_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
) -> Result<Json<CorrelationRecord>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    validate_route_id(&kind, "kind")?;
    validate_route_id(&id, "id")?;

    if CorrelationKind::parse(&kind).is_none() {
        return Err(ApiError::BadRequest(format!("unknown kind: {kind}")));
    }

    let record = state
        .data_fabric
        .get_correlation(&tenant.tenant_id, &kind, &id)
        .await
        .map_err(ApiError::from)?;
    tenant.ensure(&record.tenant_id)?;
    Ok(Json(record))
}

async fn events_aivcs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<InboundAivcsEvent>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    tenant.ensure(&body.tenant_id)?;

    if body.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest("idempotency_key is required".into()));
    }
    validate_route_id(&body.idempotency_key, "idempotency_key")?;

    if let Some(run_id) = &body.run_id {
        validate_route_id(run_id, "run_id")?;
    }

    let outcome = state
        .ingest
        .ingest_aivcs(body)
        .await
        .map_err(ApiError::from)?;
    Ok(ingest_outcome_response(outcome))
}

async fn events_hitl(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<InboundHitlEvent>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    tenant.ensure(&body.tenant_id)?;

    if body.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest("idempotency_key is required".into()));
    }
    validate_route_id(&body.idempotency_key, "idempotency_key")?;

    let outcome = state
        .ingest
        .ingest_hitl(body)
        .await
        .map_err(ApiError::from)?;
    Ok(ingest_outcome_response(outcome))
}

fn ingest_outcome_response(outcome: IngestOutcome) -> Response {
    match outcome {
        IngestOutcome::Ingested(event) => {
            (StatusCode::OK, Json(IngestResponse { event: *event })).into_response()
        }
        IngestOutcome::Staged { event_id, pending } => (
            StatusCode::ACCEPTED,
            Json(StagedIngestResponse {
                staged: true,
                event_id: event_id.0,
                pending,
            }),
        )
            .into_response(),
    }
}

async fn hitl_review_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(review_id): Path<String>,
) -> Result<Response, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    validate_route_id(&review_id, "review_id")?;
    let assembler = ReviewBundleAssembler::new(state.data_fabric.clone(), state.aivcs.clone());
    let bundle = assembler
        .assemble(&tenant.tenant_id, &review_id)
        .await
        .map_err(ApiError::from)?;
    tenant.ensure(&bundle.tenant_id)?;

    let etag = bundle.etag();
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == etag)
    {
        return Ok((StatusCode::NOT_MODIFIED, [(header::ETAG, etag)], ()).into_response());
    }

    Ok((StatusCode::OK, [(header::ETAG, etag)], Json(bundle)).into_response())
}

async fn hitl_review_decision(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(review_id): Path<String>,
    Json(body): Json<ReviewDecisionRequest>,
) -> Result<Json<ReviewDecisionResponse>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    validate_route_id(&review_id, "review_id")?;

    if body.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest("idempotency_key is required".into()));
    }
    validate_route_id(&body.idempotency_key, "idempotency_key")?;

    let correlation = state
        .data_fabric
        .get_correlation(&tenant.tenant_id, "review", &review_id)
        .await
        .map_err(ApiError::from)?;

    let mut event = dfc_core::DfcEvent::new(
        body.decision.data_fabric_event_type(),
        tenant.tenant_id.clone(),
        body.idempotency_key.clone(),
        dfc_core::SourceSystem::AivcsHumanInTheLoop,
    );
    event.run_id = correlation.data_fabric_run_id.clone();
    event.task_id = correlation.data_fabric_task_id.clone();
    event.metadata = serde_json::json!({
        "review_id": review_id,
        "reviewer": body.reviewer.as_deref().unwrap_or("unknown"),
        "comment": body.comment,
        "decision": body.decision.as_str(),
    });

    let stored = state
        .data_fabric
        .ingest_event(&event)
        .await
        .map_err(ApiError::from)?;

    let _ = state
        .data_fabric
        .bump_review_revision(&tenant.tenant_id, &review_id)
        .await
        .map_err(ApiError::from)?;

    let aivcs_result = state
        .aivcs
        .submit_review_decision(&ReviewDecisionPayload {
            tenant_id: tenant.tenant_id.clone(),
            review_id: review_id.clone(),
            decision: body.decision.as_str().into(),
            comment: body.comment.clone(),
            idempotency_key: body.idempotency_key,
            run_id: correlation.data_fabric_run_id,
            task_id: correlation.data_fabric_task_id,
        })
        .await
        .map_err(ApiError::from)?;

    Ok(Json(ReviewDecisionResponse {
        review_id,
        data_fabric_event_id: stored
            .data_fabric_event_id
            .unwrap_or_else(|| stored.event_id.0.clone()),
        aivcs_operation_id: aivcs_result.operation_id,
    }))
}

async fn replay_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ReplayRequest>,
) -> Result<Json<dfc_core::ReplayResponse>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    tenant.ensure(&body.tenant_id)?;

    validate_route_id(&body.run_id, "run_id")?;
    validate_route_id(&body.idempotency_key, "idempotency_key")?;

    let bridge = ReplayBridge::new(state.data_fabric.clone(), state.aivcs.clone());
    let response = bridge
        .handle_replay(AuditContext::new(actor_from_headers(&headers), None), body)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(response))
}

async fn rollback_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<RollbackRequest>,
) -> Result<Json<dfc_core::RollbackResponse>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    tenant.ensure(&body.tenant_id)?;

    validate_route_id(&body.branch_id, "branch_id")?;
    validate_route_id(&body.idempotency_key, "idempotency_key")?;

    let bridge = ReplayBridge::new(state.data_fabric.clone(), state.aivcs.clone());
    let response = bridge
        .handle_rollback(AuditContext::new(actor_from_headers(&headers), None), body)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(response))
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Forbidden(String),
    Conflict(String),
    Upstream(String),
}

impl From<DfcError> for ApiError {
    fn from(err: DfcError) -> Self {
        match err {
            DfcError::Validation(msg) => Self::BadRequest(msg),
            DfcError::TenantMismatch { .. } => Self::Forbidden("tenant access denied".into()),
            DfcError::NotFound(msg) => Self::NotFound(msg),
            DfcError::Conflict(msg) => Self::Conflict(msg),
            DfcError::Upstream { system, message } => {
                Self::Upstream(format!("{system}: {message}"))
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            Self::Forbidden(msg) => (StatusCode::FORBIDDEN, msg),
            Self::Conflict(msg) => (StatusCode::CONFLICT, msg),
            Self::Upstream(msg) => (StatusCode::BAD_GATEWAY, msg),
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use dfc_hitl::HitlReviewBundle;
    use tower::ServiceExt;

    fn app() -> Router {
        let data_fabric = Arc::new(MockDataFabricClient::default());
        let ingest = Arc::new(EventIngestService::new(data_fabric.clone()));
        let state = AppState {
            git_sha: "test",
            public_fqdn: config::DEFAULT_PUBLIC_FQDN.into(),
            public_url: format!("https://{}", config::DEFAULT_PUBLIC_FQDN),
            data_fabric,
            ingest,
            aivcs: Arc::new(MockAivcsClient),
        };
        Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/version", get(version))
            .route("/v1/correlate", post(correlate_create))
            .route("/v1/correlate/{kind}/{id}", get(correlate_get))
            .route("/v1/events/aivcs", post(events_aivcs))
            .route("/v1/events/hitl", post(events_hitl))
            .route("/v1/hitl/reviews/{review_id}", get(hitl_review_get))
            .route(
                "/v1/hitl/reviews/{review_id}/decision",
                post(hitl_review_decision),
            )
            .route("/v1/replay/request", post(replay_request))
            .route("/v1/rollback/request", post(rollback_request))
            .with_state(state)
    }

    async fn post_json(
        app: &Router,
        path: &str,
        tenant: &str,
        body: serde_json::Value,
    ) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::post(path)
                    .header("x-tenant-id", tenant)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn health_and_version() {
        let app = app();

        let resp = app
            .clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = app
            .oneshot(Request::get("/v1/version").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn aivcs_event_requires_idempotency_key() {
        let app = app();
        let resp = post_json(
            &app,
            "/v1/events/aivcs",
            "tenant-a",
            serde_json::json!({
                "event_type": "aivcs.snapshot.created",
                "tenant_id": "tenant-a",
                "idempotency_key": "   "
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn aivcs_duplicate_idempotency_returns_same_event_id() {
        let app = app();
        let body = serde_json::json!({
            "event_type": "aivcs.snapshot.created",
            "tenant_id": "tenant-a",
            "idempotency_key": "snap-key-1",
            "aivcs_ref": "aivcs:snapshot:snap_1",
            "run_id": "run-1"
        });

        let first = post_json(&app, "/v1/events/aivcs", "tenant-a", body.clone()).await;
        assert_eq!(first.status(), StatusCode::OK);
        let first_body: IngestResponse = serde_json::from_slice(
            &axum::body::to_bytes(first.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        let second = post_json(&app, "/v1/events/aivcs", "tenant-a", body).await;
        assert_eq!(second.status(), StatusCode::OK);
        let second_body: IngestResponse = serde_json::from_slice(
            &axum::body::to_bytes(second.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(first_body.event.event_id, second_body.event.event_id);
    }

    #[tokio::test]
    async fn branch_created_before_snapshot_is_staged_then_reconciled() {
        let app = app();
        let branch = serde_json::json!({
            "event_type": "aivcs.branch.created",
            "tenant_id": "tenant-a",
            "idempotency_key": "branch-key-1",
            "metadata": { "parent_snapshot_id": "snap_parent" }
        });
        let staged = post_json(&app, "/v1/events/aivcs", "tenant-a", branch.clone()).await;
        assert_eq!(staged.status(), StatusCode::ACCEPTED);

        let duplicate = post_json(&app, "/v1/events/aivcs", "tenant-a", branch).await;
        assert_eq!(duplicate.status(), StatusCode::ACCEPTED);

        let snapshot = serde_json::json!({
            "event_type": "aivcs.snapshot.created",
            "tenant_id": "tenant-a",
            "idempotency_key": "snap-key-parent",
            "aivcs_ref": "aivcs:snapshot:snap_parent",
            "run_id": "run-1"
        });
        let ingested = post_json(&app, "/v1/events/aivcs", "tenant-a", snapshot).await;
        assert_eq!(ingested.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn hitl_event_enriches_run_id_from_review_correlation() {
        let app = app();
        let correlate = serde_json::json!({
            "tenant_id": "tenant-a",
            "data_fabric_run_id": "run-123",
            "metadata": { "review_id": "rev-1" }
        });
        let correlate_resp = post_json(&app, "/v1/correlate", "tenant-a", correlate).await;
        assert_eq!(correlate_resp.status(), StatusCode::OK);

        let hitl = serde_json::json!({
            "event_type": "hitl.review.opened",
            "tenant_id": "tenant-a",
            "idempotency_key": "hitl-key-1",
            "review_id": "rev-1"
        });
        let resp = post_json(&app, "/v1/events/hitl", "tenant-a", hitl).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: IngestResponse = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body.event.run_id.as_deref(), Some("run-123"));
        assert_eq!(
            body.event.source_system,
            dfc_core::SourceSystem::AivcsHumanInTheLoop
        );
    }

    async fn get_request(app: &Router, path: &str, tenant: &str) -> axum::response::Response {
        app.clone()
            .oneshot(
                Request::get(path)
                    .header("x-tenant-id", tenant)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_correlate_lifecycle_us_c1_c2_c3() {
        let app = app();

        let body = serde_json::json!({
            "tenant_id": "tenant-a",
            "kind": "branch",
            "source_system": "github",
            "source_id": "feature-abc",
            "target_system": "aivcs",
            "target_id": "branch-abc",
            "metadata": { "content_hash": "hash123" }
        });

        // 1. Create correlation mapping (US-C1)
        let resp = post_json(&app, "/v1/correlate", "tenant-a", body.clone()).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let record: CorrelationRecord = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        // Stable content-addressed derivation (US-C3)
        let expected_id = dfc_core::CorrelationId::from_content_hash("tenant-a", "hash123");
        assert_eq!(record.correlation_id, expected_id);
        assert_eq!(record.tenant_id, "tenant-a");
        assert_eq!(record.kind, Some(CorrelationKind::Branch));
        assert_eq!(record.source_id, Some("feature-abc".to_string()));
        assert_eq!(record.target_id, Some("branch-abc".to_string()));

        // Deduplication/retries are stable
        let resp2 = post_json(&app, "/v1/correlate", "tenant-a", body).await;
        assert_eq!(resp2.status(), StatusCode::OK);
        let record2: CorrelationRecord = serde_json::from_slice(
            &axum::body::to_bytes(resp2.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(record2.correlation_id, expected_id);

        // 2. Resolve correlation (US-C2)
        // Resolve using source_id
        let resp_resolve1 = get_request(&app, "/v1/correlate/branch/feature-abc", "tenant-a").await;
        assert_eq!(resp_resolve1.status(), StatusCode::OK);
        let record_res1: CorrelationRecord = serde_json::from_slice(
            &axum::body::to_bytes(resp_resolve1.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(record_res1.correlation_id, expected_id);

        // Resolve using target_id
        let resp_resolve2 = get_request(&app, "/v1/correlate/branch/branch-abc", "tenant-a").await;
        assert_eq!(resp_resolve2.status(), StatusCode::OK);

        // Resolve under a different tenant -> 403 Forbidden (US-X1 tenant isolation)
        let resp_resolve3 = get_request(&app, "/v1/correlate/branch/feature-abc", "tenant-b").await;
        assert_eq!(resp_resolve3.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn cross_tenant_lookup_forbidden_and_audited() {
        use std::sync::Arc;
        // Prepare a MockDataFabricClient that already has a correlation for tenant-b
        let data_fabric = Arc::new(MockDataFabricClient::default());
        let req = dfc_core::CorrelateRequest {
            tenant_id: "tenant-b".into(),
            repo: None,
            kind: None,
            source_system: None,
            source_id: None,
            target_system: None,
            target_id: None,
            data_fabric_run_id: Some("run-123".into()),
            data_fabric_task_id: None,
            aivcs_snapshot_id: None,
            aivcs_branch: None,
            metadata: serde_json::json!({}),
        };
        let record = dfc_core::CorrelationRecord::from(req);
        // store under tenant-b
        data_fabric.store_correlation(&record).await.unwrap();

        let ingest = Arc::new(EventIngestService::new(data_fabric.clone()));
        let state = AppState {
            git_sha: "test",
            public_fqdn: config::DEFAULT_PUBLIC_FQDN.into(),
            public_url: format!("https://{}", config::DEFAULT_PUBLIC_FQDN),
            data_fabric: data_fabric.clone(),
            ingest,
            aivcs: Arc::new(MockAivcsClient),
        };
        let app = Router::new()
            .route("/v1/correlate/{kind}/{id}", get(correlate_get))
            .with_state(state);

        let resp = app
            .clone()
            .oneshot(
                Request::get("/v1/correlate/run/run-123")
                    .header("x-tenant-id", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_correlate_409_conflict() {
        let app = app();

        let body1 = serde_json::json!({
            "tenant_id": "tenant-a",
            "kind": "pr",
            "source_system": "github",
            "source_id": "pr-100",
            "target_system": "aivcs",
            "target_id": "pr-200"
        });

        let resp1 = post_json(&app, "/v1/correlate", "tenant-a", body1).await;
        assert_eq!(resp1.status(), StatusCode::OK);

        // Try to map same PR ID to a different tenant
        let body2 = serde_json::json!({
            "tenant_id": "tenant-b",
            "kind": "pr",
            "source_system": "github",
            "source_id": "pr-100",
            "target_system": "aivcs",
            "target_id": "pr-300"
        });

        let resp2 = post_json(&app, "/v1/correlate", "tenant-b", body2).await;
        assert_eq!(resp2.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn correlate_rejects_empty_mapping() {
        let app = app();
        let body = serde_json::json!({
            "tenant_id": "tenant-a"
        });
        let resp = post_json(&app, "/v1/correlate", "tenant-a", body).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    async fn correlate_review(app: &Router, review_id: &str) {
        let correlate = serde_json::json!({
            "tenant_id": "tenant-a",
            "data_fabric_run_id": "run-123",
            "data_fabric_task_id": "task-456",
            "metadata": { "review_id": review_id }
        });
        let resp = post_json(app, "/v1/correlate", "tenant-a", correlate).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn hitl_review_bundle_lifecycle() {
        let app = app();
        correlate_review(&app, "rev-e4").await;

        let resp = get_request(&app, "/v1/hitl/reviews/rev-e4", "tenant-a").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("etag").and_then(|v| v.to_str().ok()),
            Some("\"rev-1\"")
        );

        let bundle: HitlReviewBundle = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(bundle.review_id, "rev-e4");
        assert_eq!(bundle.tenant_id, "tenant-a");
        assert!(bundle.correlation_id.is_some());
        assert_eq!(bundle.run_id.as_deref(), Some("run-123"));
        assert_eq!(bundle.task_id.as_deref(), Some("task-456"));
        assert_eq!(bundle.revision, 1);
        assert!(bundle.diff_ref.is_some());
        assert!(bundle.validation_result.is_some());
    }

    #[tokio::test]
    async fn hitl_review_404_unknown_or_cross_tenant() {
        let app = app();
        correlate_review(&app, "rev-secret").await;

        let unknown = get_request(&app, "/v1/hitl/reviews/missing", "tenant-a").await;
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

        let cross_tenant = get_request(&app, "/v1/hitl/reviews/rev-secret", "tenant-b").await;
        assert_eq!(cross_tenant.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn hitl_review_304_when_etag_matches() {
        let app = app();
        correlate_review(&app, "rev-etag").await;

        let first = get_request(&app, "/v1/hitl/reviews/rev-etag", "tenant-a").await;
        assert_eq!(first.status(), StatusCode::OK);
        let etag = first
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_string();

        let cached = app
            .clone()
            .oneshot(
                Request::get("/v1/hitl/reviews/rev-etag")
                    .header("x-tenant-id", "tenant-a")
                    .header("if-none-match", etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cached.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            cached.headers().get("etag").and_then(|v| v.to_str().ok()),
            Some("\"rev-1\"")
        );
    }

    #[tokio::test]
    async fn hitl_decision_fanout_writes_human_event_and_calls_aivcs() {
        let app = app();
        correlate_review(&app, "rev-decision").await;

        let decision = serde_json::json!({
            "decision": "approved",
            "comment": "looks good",
            "idempotency_key": "decision-key-1"
        });
        let resp = post_json(
            &app,
            "/v1/hitl/reviews/rev-decision/decision",
            "tenant-a",
            decision,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body: ReviewDecisionResponse = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body.review_id, "rev-decision");
        assert!(!body.data_fabric_event_id.is_empty());
        assert!(body.aivcs_operation_id.starts_with("aivcs_op_"));

        let refreshed = get_request(&app, "/v1/hitl/reviews/rev-decision", "tenant-a").await;
        assert_eq!(refreshed.status(), StatusCode::OK);
        let etag = refreshed
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_string();
        let bundle: HitlReviewBundle = serde_json::from_slice(
            &axum::body::to_bytes(refreshed.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(bundle.revision, 2);
        assert_eq!(etag, "\"rev-2\"");
    }

    async fn correlate_run(app: &Router, run_id: &str, snapshot_id: &str) {
        let correlate = serde_json::json!({
            "tenant_id": "tenant-a",
            "data_fabric_run_id": run_id,
            "aivcs_snapshot_id": snapshot_id
        });
        let resp = post_json(app, "/v1/correlate", "tenant-a", correlate).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    async fn correlate_branch(app: &Router, branch_id: &str) {
        let correlate = serde_json::json!({
            "tenant_id": "tenant-a",
            "kind": "branch",
            "source_system": "aivcs",
            "source_id": branch_id,
            "target_system": "aivcs",
            "target_id": branch_id,
            "aivcs_branch": branch_id
        });
        let resp = post_json(app, "/v1/correlate", "tenant-a", correlate).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn replay_request_resolves_lineage_and_audits_events() {
        let app = app();
        correlate_run(&app, "run-e5", "snap_base").await;

        let body = serde_json::json!({
            "tenant_id": "tenant-a",
            "run_id": "run-e5",
            "target_snapshot_id": "snap_target",
            "idempotency_key": "replay-e5-1"
        });
        let resp = app
            .clone()
            .oneshot(
                Request::post("/v1/replay/request")
                    .header("x-tenant-id", "tenant-a")
                    .header("x-actor", "operator-1")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let replay: dfc_core::ReplayResponse = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(replay.replay_id.starts_with("replay_"));
        assert_eq!(replay.snapshot_ids, vec!["snap_base", "snap_target"]);
        assert!(replay.data_fabric_event_id.is_some());

        let duplicate = post_json(
            &app,
            "/v1/replay/request",
            "tenant-a",
            serde_json::json!({
                "tenant_id": "tenant-a",
                "run_id": "run-e5",
                "target_snapshot_id": "snap_target",
                "idempotency_key": "replay-e5-1"
            }),
        )
        .await;
        assert_eq!(duplicate.status(), StatusCode::OK);
        let replay2: dfc_core::ReplayResponse = serde_json::from_slice(
            &axum::body::to_bytes(duplicate.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(replay2.replay_id, replay.replay_id);
    }

    #[tokio::test]
    async fn rollback_request_records_audit_trail_before_aivcs() {
        let app = app();
        correlate_branch(&app, "branch-e5").await;

        let body = serde_json::json!({
            "tenant_id": "tenant-a",
            "branch_id": "branch-e5",
            "target_snapshot_id": "snap_rollback",
            "reason": "manual operator rollback",
            "idempotency_key": "rollback-e5-1"
        });
        let resp = app
            .clone()
            .oneshot(
                Request::post("/v1/rollback/request")
                    .header("x-tenant-id", "tenant-a")
                    .header("x-actor", "operator-1")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let rollback: dfc_core::RollbackResponse = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(rollback.rollback_id.starts_with("rollback_"));
        assert!(rollback.data_fabric_event_id.is_some());
    }

    #[tokio::test]
    async fn replay_request_rejects_empty_run_id() {
        let app = app();
        let resp = post_json(
            &app,
            "/v1/replay/request",
            "tenant-a",
            serde_json::json!({
                "tenant_id": "tenant-a",
                "run_id": "   ",
                "idempotency_key": "bad-replay"
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn correlate_get_rejects_invalid_charset_in_id() {
        let app = app();
        let resp = get_request(&app, "/v1/correlate/run/run-with@symbol", "tenant-a").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: ErrorBody = serde_json::from_slice(&body).unwrap();
        assert!(error.error.contains("forbidden characters"));
    }

    #[tokio::test]
    async fn hitl_review_get_rejects_invalid_charset_in_review_id() {
        let app = app();
        let resp = get_request(&app, "/v1/hitl/reviews/rev-with*star", "tenant-a").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: ErrorBody = serde_json::from_slice(&body).unwrap();
        assert!(error.error.contains("forbidden characters"));
    }

    #[tokio::test]
    async fn events_aivcs_rejects_invalid_charset_in_idempotency_key() {
        let app = app();
        let resp = post_json(
            &app,
            "/v1/events/aivcs",
            "tenant-a",
            serde_json::json!({
                "event_type": "aivcs.snapshot.created",
                "tenant_id": "tenant-a",
                "idempotency_key": "key-with-@-sign",
                "aivcs_ref": "aivcs:snapshot:snap_1",
                "run_id": "run-1"
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: ErrorBody = serde_json::from_slice(&body).unwrap();
        assert!(error.error.contains("forbidden characters"));
    }

    #[tokio::test]
    async fn valid_charset_ids_accepted() {
        let app = app();
        let body = serde_json::json!({
            "tenant_id": "tenant-a",
            "kind": "branch",
            "source_system": "github",
            "source_id": "feature-abc_123.v2",
            "target_system": "aivcs",
            "target_id": "branch:abc-def_123.v2",
        });
        let resp = post_json(&app, "/v1/correlate", "tenant-a", body).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = get_request(&app, "/v1/correlate/branch/feature-abc_123.v2", "tenant-a").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
