//! Standalone staged_publish demonstration; optional first argument is the data directory.
mod support;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::demo("staged_publish").await
}
