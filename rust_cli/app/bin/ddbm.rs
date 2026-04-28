#[tokio::main]
async fn main() -> anyhow::Result<()> {
    ddbm::run().await
}
