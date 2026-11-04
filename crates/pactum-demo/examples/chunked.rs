//! Standalone chunked demonstration; optional first argument is the data directory.
mod support;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    support::demo("chunked").await
}
