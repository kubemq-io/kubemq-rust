use std::sync::Arc;
use std::sync::RwLock;

#[derive(Clone)]
pub(crate) struct AuthInterceptor {
    token: Arc<RwLock<Option<String>>>,
}

impl AuthInterceptor {
    pub(crate) fn new(token: Arc<RwLock<Option<String>>>) -> Self {
        Self { token }
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        // Use std::sync::RwLock (not tokio) since Interceptor::call is synchronous.
        // read() blocks briefly if the write lock is held during token refresh,
        // which is safe because the write critical section is extremely short.
        let guard = self
            .token
            .read()
            .map_err(|_| tonic::Status::internal("token lock poisoned"))?;
        if let Some(ref token) = *guard {
            request.metadata_mut().insert(
                "authorization",
                token
                    .parse()
                    .map_err(|_| tonic::Status::internal("invalid token"))?,
            );
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::service::Interceptor;

    #[test]
    fn test_auth_interceptor_no_token() {
        let token = Arc::new(RwLock::new(None));
        let mut interceptor = AuthInterceptor::new(token);
        let request = tonic::Request::new(());
        let result = interceptor.call(request);
        assert!(result.is_ok());
        let req = result.unwrap();
        // Should not have authorization header
        assert!(req.metadata().get("authorization").is_none());
    }

    #[test]
    fn test_auth_interceptor_with_token() {
        let token = Arc::new(RwLock::new(Some("my-secret-token".to_string())));
        let mut interceptor = AuthInterceptor::new(token);
        let request = tonic::Request::new(());
        let result = interceptor.call(request);
        assert!(result.is_ok());
        let req = result.unwrap();
        let auth = req.metadata().get("authorization").unwrap();
        assert_eq!(auth.to_str().unwrap(), "my-secret-token");
    }

    #[test]
    fn test_auth_interceptor_clone() {
        let token = Arc::new(RwLock::new(Some("token".to_string())));
        let interceptor = AuthInterceptor::new(token);
        let _cloned = interceptor.clone();
    }

    #[test]
    fn test_auth_interceptor_token_update() {
        let token = Arc::new(RwLock::new(Some("old-token".to_string())));
        let mut interceptor = AuthInterceptor::new(token.clone());

        // First call with old token
        let request = tonic::Request::new(());
        let result = interceptor.call(request).unwrap();
        assert_eq!(
            result
                .metadata()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "old-token"
        );

        // Update token
        *token.write().unwrap() = Some("new-token".to_string());

        // Second call with new token
        let request = tonic::Request::new(());
        let result = interceptor.call(request).unwrap();
        assert_eq!(
            result
                .metadata()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "new-token"
        );
    }

    #[test]
    fn test_auth_interceptor_token_removed() {
        let token = Arc::new(RwLock::new(Some("token".to_string())));
        let mut interceptor = AuthInterceptor::new(token.clone());

        // Remove token
        *token.write().unwrap() = None;

        let request = tonic::Request::new(());
        let result = interceptor.call(request).unwrap();
        assert!(result.metadata().get("authorization").is_none());
    }
}
