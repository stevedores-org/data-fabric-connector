use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use dfc_aivcs::{AivcsClient, MockAivcsClient};
use dfc_core::{
    CorrelateRequest, CorrelationKind, CorrelationRecord, DfcError, InboundAivcsEvent,
    InboundHitlEvent, ReplayRequest, RollbackRequest, SCHEMA_VERSION, TenantContext,
};
use dfc_data_fabric::{DataFabricClient, MockDataFabricClient};
use dfc_hitl::{
    HitlReviewBundle, ReviewBundleAssembler, ReviewDecision, ReviewDecisionRequest,
    ReviewDecisionResponse,
};
use serde::Serialize;
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

#[derive(Serialize)]
struct IngestResponse {
    event: dfc_core::DfcEvent,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("dfc=info".parse()?),
        )
        .init();

    let cfg = ServerConfig::from_env();
    info!(
        port = cfg.port,
        fqdn = %cfg.public_fqdn,
        "dfc-server starting"
    );

    let state = AppState {
        git_sha: option_env!("GIT_SHA").unwrap_or("dev"),
        public_fqdn: cfg.public_fqdn.clone(),
        public_url: cfg.public_url(),
        data_fabric: Arc::new(MockDataFabricClient::default()),
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

    let record = CorrelationRecord::from(body);
    state
        .data_fabric
        .store_correlation(&record)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(record))
}

async fn correlate_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
) -> Result<Json<CorrelationRecord>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
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
) -> Result<Json<IngestResponse>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    tenant.ensure(&body.tenant_id)?;

    if body.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest("idempotency_key is required".into()));
    }

    let event = body.into_dfc_event();
    event.validate().map_err(ApiError::from)?;
    let stored = state
        .data_fabric
        .ingest_event(&event)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(IngestResponse { event: stored }))
}

async fn events_hitl(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<InboundHitlEvent>,
) -> Result<Json<IngestResponse>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    tenant.ensure(&body.tenant_id)?;

    if body.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest("idempotency_key is required".into()));
    }

    let event = body.into_dfc_event();
    event.validate().map_err(ApiError::from)?;
    let stored = state
        .data_fabric
        .ingest_event(&event)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(IngestResponse { event: stored }))
}

async fn hitl_review_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(review_id): Path<String>,
) -> Result<Json<HitlReviewBundle>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    let assembler = ReviewBundleAssembler::new(state.data_fabric.clone());
    let bundle = assembler
        .assemble(&tenant.tenant_id, &review_id)
        .await
        .map_err(ApiError::from)?;
    tenant.ensure(&bundle.tenant_id)?;
    Ok(Json(bundle))
}

async fn hitl_review_decision(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(review_id): Path<String>,
    Json(body): Json<ReviewDecisionRequest>,
) -> Result<Json<ReviewDecisionResponse>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;

    if body.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest("idempotency_key is required".into()));
    }

    let event_type = match body.decision {
        ReviewDecision::Approved => "hitl.review.approved",
        ReviewDecision::Rejected => "hitl.review.rejected",
        ReviewDecision::RequestedChanges => "hitl.review.requested_changes",
        ReviewDecision::Escalated => "hitl.review.escalated",
    };

    let mut event = dfc_core::DfcEvent::new(
        event_type,
        tenant.tenant_id.clone(),
        body.idempotency_key,
        dfc_core::SourceSystem::AivcsHumanInTheLoop,
    );
    event.metadata = serde_json::json!({
        "review_id": review_id,
        "reviewer": body.reviewer,
        "reason": body.reason,
        "constraints": body.constraints,
    });

    let stored = state
        .data_fabric
        .ingest_event(&event)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(ReviewDecisionResponse {
        review_id,
        data_fabric_event_id: stored
            .data_fabric_event_id
            .unwrap_or_else(|| stored.event_id.0.clone()),
        aivcs_operation_id: "aivcs_op_stub".into(),
    }))
}

async fn replay_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ReplayRequest>,
) -> Result<Json<dfc_core::ReplayResponse>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    tenant.ensure(&body.tenant_id)?;

    if body.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest("idempotency_key is required".into()));
    }

    let mut requested = dfc_core::DfcEvent::new(
        "aivcs.replay.requested",
        body.tenant_id.clone(),
        format!("{}:requested", body.idempotency_key),
        dfc_core::SourceSystem::AivcsApi,
    );
    requested.run_id = Some(body.run_id.clone());
    requested.task_id = body.task_id.clone();
    state
        .data_fabric
        .ingest_event(&requested)
        .await
        .map_err(ApiError::from)?;

    let response = state
        .aivcs
        .request_replay(&body)
        .await
        .map_err(ApiError::from)?;

    let mut completed = dfc_core::DfcEvent::new(
        "aivcs.replay.completed",
        body.tenant_id,
        format!("{}:completed", body.idempotency_key),
        dfc_core::SourceSystem::AivcsApi,
    );
    completed.run_id = Some(body.run_id);
    state
        .data_fabric
        .ingest_event(&completed)
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

    if body.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest("idempotency_key is required".into()));
    }

    let response = state
        .aivcs
        .request_rollback(&body)
        .await
        .map_err(ApiError::from)?;

    let mut event = dfc_core::DfcEvent::new(
        "aivcs.rollback.requested",
        body.tenant_id,
        body.idempotency_key,
        dfc_core::SourceSystem::AivcsApi,
    );
    event.metadata = serde_json::json!({
        "branch_id": body.branch_id,
        "target_snapshot_id": body.target_snapshot_id,
        "reason": body.reason,
    });
    state
        .data_fabric
        .ingest_event(&event)
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
    use tower::ServiceExt;

    fn app() -> Router {
        let state = AppState {
            git_sha: "test",
            public_fqdn: config::DEFAULT_PUBLIC_FQDN.into(),
            public_url: format!("https://{}", config::DEFAULT_PUBLIC_FQDN),
            data_fabric: Arc::new(MockDataFabricClient::default()),
            aivcs: Arc::new(MockAivcsClient),
        };
        Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/version", get(version))
            .with_state(state)
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
}
