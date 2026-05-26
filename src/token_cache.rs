use async_trait::async_trait;
use cirrus::auth::{AuthSession, RefreshTokenAuth};
use cirrus::CirrusResult;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

/// TTL for cached access tokens in seconds.
/// Salesforce access tokens typically last 1-2 hours. We cache for 90 minutes
/// to dramatically reduce OAuth endpoint hits, while still being well within
/// the typical token lifetime. If a token does expire early, the API returns
/// 401 INVALID_SESSION_ID, cirrus calls invalidate(), and we refresh.
const CACHE_TTL_SECS: u64 = 90 * 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedToken {
    access_token: String,
    expires_at: u64, // Unix timestamp (seconds)
}

/// Wraps [`RefreshTokenAuth`] with a persistent on-disk token cache so that
/// repeated CLI invocations don't hammer the Salesforce OAuth endpoint.
pub struct PersistentRefreshTokenAuth {
    inner: Arc<RefreshTokenAuth>,
    cache_path: PathBuf,
    mem_cache: RwLock<Option<CachedToken>>,
}

impl PersistentRefreshTokenAuth {
    pub fn new(inner: Arc<RefreshTokenAuth>) -> anyhow::Result<Self> {
        let cache_dir = Self::cache_dir()?;
        std::fs::create_dir_all(&cache_dir)?;
        let cache_path = cache_dir.join("token.json");

        Ok(Self {
            inner,
            cache_path,
            mem_cache: RwLock::new(None),
        })
    }

    fn cache_dir() -> anyhow::Result<PathBuf> {
        // Try standard cache dirs
        if let Ok(home) = std::env::var("HOME") {
            return Ok(PathBuf::from(home).join(".cache").join("justice-link"));
        }
        if let Ok(userprofile) = std::env::var("USERPROFILE") {
            return Ok(PathBuf::from(userprofile)
                .join("AppData")
                .join("Local")
                .join("justice-link")
                .join("cache"));
        }
        // Fallback to temp dir
        Ok(std::env::temp_dir().join("justice-link"))
    }

    async fn load_disk_cache(&self) -> Option<CachedToken> {
        let data = tokio::fs::read_to_string(&self.cache_path).await.ok()?;
        let token: CachedToken = serde_json::from_str(&data).ok()?;
        if self.is_valid(&token) {
            Some(token)
        } else {
            None
        }
    }

    async fn save_disk_cache(&self, token: &CachedToken) {
        if let Ok(data) = serde_json::to_string_pretty(token) {
            let _ = tokio::fs::write(&self.cache_path, data).await;
        }
    }

    fn is_valid(&self, token: &CachedToken) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        token.expires_at > now
    }

    fn now_plus_ttl(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + CACHE_TTL_SECS
    }
}

#[async_trait]
impl AuthSession for PersistentRefreshTokenAuth {
    async fn access_token(&self) -> CirrusResult<Cow<'_, str>> {
        // 1. Fast path: in-memory cache
        {
            let guard = self.mem_cache.read().await;
            if let Some(token) = guard.as_ref() {
                if self.is_valid(token) {
                    return Ok(Cow::Owned(token.access_token.clone()));
                }
            }
        }

        // 2. Medium path: load from disk
        if let Some(token) = self.load_disk_cache().await {
            let mut guard = self.mem_cache.write().await;
            *guard = Some(token.clone());
            return Ok(Cow::Owned(token.access_token));
        }

        // 3. Slow path: hit Salesforce OAuth endpoint
        let token_str = self.inner.access_token().await?;
        let access_token = token_str.to_string();

        let cached = CachedToken {
            access_token: access_token.clone(),
            expires_at: self.now_plus_ttl(),
        };

        self.save_disk_cache(&cached).await;

        let mut guard = self.mem_cache.write().await;
        *guard = Some(cached);

        Ok(Cow::Owned(access_token))
    }

    fn instance_url(&self) -> &str {
        self.inner.instance_url()
    }

    async fn invalidate(&self, stale_token: &str) {
        // Clear our caches if the stale token matches what we have
        let mut guard = self.mem_cache.write().await;
        if let Some(token) = guard.as_ref() {
            if token.access_token == stale_token {
                *guard = None;
                let _ = tokio::fs::remove_file(&self.cache_path).await;
            }
        }
        drop(guard);
        self.inner.invalidate(stale_token).await;
    }
}
