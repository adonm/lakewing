//! One logging/tracing subscriber; OTLP uses gRPC (normally port 4317).
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub struct Telemetry(Option<SdkTracerProvider>);

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(provider) = &self.0 {
            if let Err(error) = provider.shutdown() {
                eprintln!("trace shutdown: {error}");
            }
        }
    }
}

pub fn init(endpoint: Option<&str>) -> anyhow::Result<Telemetry> {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let provider = endpoint
        .map(|endpoint| -> anyhow::Result<_> {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()?;
            Ok(SdkTracerProvider::builder()
                .with_resource(Resource::builder().with_service_name("lakewing").build())
                .with_batch_exporter(exporter)
                .build())
        })
        .transpose()?;
    let layer = provider
        .as_ref()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("lakewing")));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(layer)
        .try_init()?;
    Ok(Telemetry(provider))
}
