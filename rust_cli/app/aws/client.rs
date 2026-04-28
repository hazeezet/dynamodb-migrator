/// AWS DynamoDB client setup.
///
/// Creates a shared client from the standard AWS credential chain
/// (environment variables → ~/.aws/credentials → IAM role).
use anyhow::Result;
use aws_sdk_dynamodb::Client;

/// Create a DynamoDB client using the default AWS config chain.
pub async fn create_client() -> Result<Client> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    // Validate we can resolve credentials early
    let credentials_provider = config.credentials_provider();
    if credentials_provider.is_none() {
        anyhow::bail!(
            "AWS credentials not found. Please configure credentials via:\n\
             - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)\n\
             - AWS CLI (aws configure)\n\
             - IAM instance role"
        );
    }

    let client = Client::new(&config);
    Ok(client)
}
