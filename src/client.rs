use anyhow::Result;
use cirrus::auth::AuthSession;
use cirrus::{auth, Cirrus};
use std::sync::Arc;

use crate::token_cache::PersistentRefreshTokenAuth;

/// Builds and returns an authenticated Salesforce client from environment variables.
pub async fn build_client() -> Result<Cirrus> {
    dotenvy::dotenv()?;

    let instance_url = std::env::var("SF_INSTANCE_URL")?;
    let consumer_key = std::env::var("SF_CONSUMER_KEY")?;
    let consumer_secret = std::env::var("SF_CONSUMER_SECRET")?;
    let refresh_token = std::env::var("SF_REFRESH_TOKEN")?;

    let inner = auth::RefreshTokenAuth::builder()
        .consumer_key(consumer_key)
        .consumer_secret(consumer_secret)
        .refresh_token(refresh_token)
        .instance_url(instance_url)
        .build()?;

    let persistent_auth = PersistentRefreshTokenAuth::new(Arc::new(inner))?;

    // Pre-warm the token cache so that subsequent API calls reuse the same
    // access token instead of triggering multiple OAuth requests.
    let _ = persistent_auth.access_token().await?;

    let sf = Cirrus::builder()
        .auth(Arc::new(persistent_auth))
        .build()?;

    Ok(sf)
}
