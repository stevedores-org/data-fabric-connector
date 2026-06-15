use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, Response},
    middleware::Next,
};
use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    resource::Resource,
    runtime,
    trace::{RandomIdGenerator, Sampler, TracerProvider},
};
use tracing::Instrument;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init_tracing() -> anyhow::Result<()> {
    let env_filter =
        EnvFilter::from_default_env().add_directive("dfc=info".parse().expect("valid directive"));

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
        return Ok(());
    }

    global::set_text_map_propagator(TraceContextPropagator::new());

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")?;
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "dfc-server".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(3))
        .build()?;

    let provider = TracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            service_name,
        )]))
        .with_batch_exporter(exporter, runtime::Tokio)
        .build();

    global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer("dfc-server");

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    Ok(())
}

pub async fn trace_context_middleware(req: Request<Body>, next: Next) -> Response<Body> {
    let correlation_id = req
        .headers()
        .get("x-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    let span = tracing::info_span!(
        "http_request",
        method = %req.method(),
        uri = %req.uri(),
        correlation_id = tracing::field::Empty,
    );

    if let Some(correlation_id) = correlation_id.as_deref() {
        span.record("correlation_id", correlation_id);
    }

    async move { next.run(req).await }.instrument(span).await
}
