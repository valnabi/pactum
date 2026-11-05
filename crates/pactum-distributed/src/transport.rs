//! Versioned remote participant protocol. Transport errors always mean uncertainty.
use crate::{Domain, FencedResource, Grant, Operation, Reply};
use async_trait::async_trait;
use pactum_core::{
    Diagnostic, ErrorCode, ErrorContext, ParticipantError, ParticipantId, TransactionId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Supported protocol version. Unknown versions fail before dispatch.
pub const VERSION: u32 = 2;
fn now() -> Result<u64, TransportError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(TransportError::from_source)?;
    u64::try_from(elapsed.as_millis()).map_err(TransportError::from_source)
}
/// Versioned request; transaction and immutable operation binding survive retries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Protocol version.
    pub version: u32,
    /// SHA-256 identity over version, grant, resource, transaction and operation.
    /// Deadline is attempt-specific and deliberately excluded.
    pub identity: Vec<u8>,
    /// Exact fencing grant, validated at the resource effect boundary.
    pub grant: Grant,
    /// Durable resource identity, not merely a network address.
    pub resource: ParticipantId,
    /// Durable transaction idempotency key.
    pub transaction: TransactionId,
    /// Immutable operation arguments.
    pub operation: Operation,
    /// Unix milliseconds; server refuses requests already expired at dispatch.
    pub deadline_millis: u64,
}
impl Request {
    /// Construct an attempt. Identity remains stable when only the deadline changes.
    pub fn new(
        grant: Grant,
        resource: ParticipantId,
        transaction: TransactionId,
        operation: Operation,
        deadline_millis: u64,
    ) -> Result<Self, TransportError> {
        let mut request = Self {
            version: VERSION,
            identity: Vec::new(),
            grant,
            resource,
            transaction,
            operation,
            deadline_millis,
        };
        request.identity = request.expected_identity()?;
        Ok(request)
    }
    /// Correlation fields only; excludes payload, prepared token and owner secrets.
    pub fn context(&self) -> ErrorContext {
        ErrorContext {
            transaction: Some(self.transaction.as_str().into()),
            participant: Some(self.resource.as_str().into()),
            phase: Some(self.operation.phase().into()),
            epoch: Some(self.grant.epoch),
            request: Some(
                self.identity
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
            ),
            ..ErrorContext::default()
        }
    }
    fn expected_identity(&self) -> Result<Vec<u8>, TransportError> {
        let binding = serde_json::to_vec(&(
            self.version,
            &self.grant,
            &self.resource,
            &self.transaction,
            &self.operation,
        ))
        .map_err(TransportError::from_source)?;
        Ok(Sha256::digest(binding).to_vec())
    }
}
/// Correlated response. A failed exchange does not imply the operation failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    /// Protocol version.
    pub version: u32,
    /// Echoed immutable request identity.
    pub identity: Vec<u8>,
    /// Echoed durable resource identity.
    pub resource: ParticipantId,
    /// Echoed durable transaction identity.
    pub transaction: TransactionId,
    /// Durable reply, or resource/ownership uncertainty. Never synthesize a no vote.
    pub result: Result<Reply, RemoteFailure>,
}
/// Version 2 wire failure. Source messages and payloads never cross this boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, snafu::Snafu)]
#[serde(deny_unknown_fields)]
#[snafu(display("remote operation failed: {code}"))]
pub struct RemoteFailure {
    /// Stable server-side code, independent of its Display implementation.
    pub code: ErrorCode,
    /// Allowlisted correlation identifiers.
    pub context: ErrorContext,
}
/// Exchange failure. Effects may already be durable, including after timeout.
#[derive(Clone, Debug, PartialEq, Eq, snafu::Snafu)]
#[snafu(transparent)]
pub struct TransportError {
    source: pactum_errors::Diagnostic,
}
impl TransportError {
    /// Construct a safe legacy diagnostic; completion remains conservative.
    pub fn message(detail: String) -> Self {
        Self::from_diagnostic(pactum_errors::Diagnostic::message(
            pactum_errors::ErrorCode::TransportUnknown,
            detail,
        ))
    }
    /// Preserve a typed boundary source using the conservative default code.
    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::classified(pactum_errors::ErrorCode::TransportUnknown, source)
    }
    /// Preserve a typed boundary source with an explicit machine-readable code.
    pub fn classified(
        code: pactum_errors::ErrorCode,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::from_diagnostic(pactum_errors::Diagnostic::caused(code, source))
    }
    /// Wrap an existing structured diagnostic without losing its classification.
    pub fn from_diagnostic(source: pactum_errors::Diagnostic) -> Self {
        Self { source }
    }
    /// Add allowlisted boundary identifiers, preserving prior metadata and sources.
    pub fn with_context(self, context: pactum_errors::ErrorContext) -> Self {
        Self {
            source: self.source.with_context(context),
        }
    }
    /// Inspect the structured diagnostic.
    pub fn diagnostic(&self) -> &pactum_errors::Diagnostic {
        &self.source
    }
}
impl std::ops::Deref for TransportError {
    type Target = pactum_errors::Diagnostic;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

/// Adapter for an authenticated, bounded remote exchange. No automatic retry here:
/// the core RetryPolicy supplies bounded identical-operation retries.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Send an attempt. Cancellation may drop the future after remote effects.
    async fn exchange(&self, request: Request) -> Result<Response, TransportError>;
}
/// Reference server endpoint. Network adapters may serialize Request/Response.
/// The resource remains responsible for atomic fencing and durable deduplication.
pub struct Endpoint {
    resource: Arc<dyn FencedResource>,
}
impl Endpoint {
    /// Bind to one stable fenced resource.
    pub fn new(resource: Arc<dyn FencedResource>) -> Self {
        Self { resource }
    }
    /// Validate envelope and deadline before dispatch. Deadline never revokes effects
    /// already executing; grant checks at the resource boundary provide fencing.
    pub async fn handle(&self, request: Request) -> Result<Response, TransportError> {
        let size = match &request.operation {
            Operation::Prepare(p) | Operation::Commit(p) => p.len(),
            Operation::Abort(p) => p.as_ref().map_or(0, Vec::len),
            Operation::Status => 0,
        };
        if request.version != VERSION
            || request.identity != request.expected_identity()?
            || request.resource != self.resource.id()
            || request.grant.domain != self.resource.domain()
            || size > 1024 * 1024
        {
            return Err(TransportError::from_diagnostic(
                Diagnostic::message(
                    ErrorCode::ProtocolMismatch,
                    "invalid protocol envelope".into(),
                )
                .with_context(request.context()),
            ));
        }
        if now()? >= request.deadline_millis {
            return Err(TransportError::from_diagnostic(
                Diagnostic::message(ErrorCode::Timeout, "request deadline expired".into())
                    .with_context(request.context()),
            ));
        }
        let context = request.context();
        let result = self
            .resource
            .call(&request.grant, &request.transaction, request.operation)
            .await
            .map_err(|e| RemoteFailure {
                code: e.code(),
                context,
            });
        Ok(Response {
            version: VERSION,
            identity: request.identity,
            resource: request.resource,
            transaction: request.transaction,
            result,
        })
    }
}
/// Remote fenced resource client; only the server-side contract makes it fenced.
pub struct RemoteResource {
    id: ParticipantId,
    domain: Domain,
    transport: Arc<dyn Transport>,
    timeout: Duration,
}
impl RemoteResource {
    /// Configure a positive per-attempt timeout. Core retries preserve request identity.
    pub fn new(
        id: ParticipantId,
        domain: Domain,
        transport: Arc<dyn Transport>,
        timeout: Duration,
    ) -> Result<Self, TransportError> {
        if timeout.as_millis() == 0 {
            return Err(TransportError::message(
                "timeout must be at least one millisecond".into(),
            ));
        }
        Ok(Self {
            id,
            domain,
            transport,
            timeout,
        })
    }
}
#[async_trait]
impl FencedResource for RemoteResource {
    fn id(&self) -> ParticipantId {
        self.id.clone()
    }
    fn domain(&self) -> Domain {
        self.domain.clone()
    }
    async fn call(
        &self,
        grant: &Grant,
        tx: &TransactionId,
        operation: Operation,
    ) -> Result<Reply, ParticipantError> {
        let context = ErrorContext {
            transaction: Some(tx.as_str().into()),
            participant: Some(self.id.as_str().into()),
            phase: Some(operation.phase().into()),
            epoch: Some(grant.epoch),
            ..ErrorContext::default()
        };
        let request = (|| {
            let millis =
                u64::try_from(self.timeout.as_millis()).map_err(TransportError::from_source)?;
            let deadline = now()?
                .checked_add(millis)
                .ok_or_else(|| TransportError::message("deadline overflow".into()))?;
            Request::new(
                grant.clone(),
                self.id.clone(),
                tx.clone(),
                operation,
                deadline,
            )
        })()
        .map_err(|e: TransportError| {
            ParticipantError::classified(e.code(), e).with_context(context)
        })?;
        let context = request.context();
        let run = async {
            let expected = request.identity.clone();
            let request_context = request.context();
            let response = self.transport.exchange(request).await.map_err(|e| {
                let e = e.with_context(request_context.clone());
                e.trace();
                e
            })?;
            if response.version != VERSION
                || response.identity != expected
                || response.resource != self.id
                || &response.transaction != tx
            {
                return Err(TransportError::from_diagnostic(
                    Diagnostic::message(
                        ErrorCode::ProtocolMismatch,
                        "uncorrelated remote response".into(),
                    )
                    .with_context(request_context),
                ));
            }
            response
                .result
                .map_err(|e| TransportError::classified(e.code, e).with_context(request_context))
        };
        match tokio::time::timeout(self.timeout, run).await {
            Ok(result) => result.map_err(|e| {
                let prior = e.context().clone();
                ParticipantError::classified(e.code(), e)
                    .with_context(prior)
                    .with_context(context.clone())
            }),
            Err(e) => {
                Err(ParticipantError::classified(ErrorCode::Timeout, e).with_context(context))
            }
        }
    }
}
/// In-process reference transport with partition and lost-response injection.
/// No network dependency or authentication policy is imposed on Pactum core.
pub struct Loopback {
    endpoint: Endpoint,
    partitioned: AtomicBool,
    lost_replies: AtomicUsize,
}
impl Loopback {
    /// Connect to a fenced server resource.
    pub fn new(resource: Arc<dyn FencedResource>) -> Self {
        Self {
            endpoint: Endpoint::new(resource),
            partitioned: AtomicBool::new(false),
            lost_replies: AtomicUsize::new(0),
        }
    }
    /// Simulate disconnection before dispatch.
    pub fn set_partitioned(&self, partitioned: bool) {
        self.partitioned.store(partitioned, Ordering::SeqCst);
    }
    /// Drop the next N replies after dispatch, preserving any durable effects.
    pub fn lose_replies(&self, count: usize) {
        self.lost_replies.store(count, Ordering::SeqCst);
    }
}
#[async_trait]
impl Transport for Loopback {
    async fn exchange(&self, request: Request) -> Result<Response, TransportError> {
        if self.partitioned.load(Ordering::SeqCst) {
            return Err(TransportError::from_diagnostic(
                Diagnostic::message(ErrorCode::Unavailable, "transport unavailable".into())
                    .with_context(request.context()),
            ));
        }
        // Exercise the actual versioned serialization boundary.
        let bytes = serde_json::to_vec(&request).map_err(TransportError::from_source)?;
        let request = serde_json::from_slice(&bytes).map_err(TransportError::from_source)?;
        let response = self.endpoint.handle(request).await?;
        if self
            .lost_replies
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(TransportError::message(
                "reply lost after dispatch; completion uncertain".into(),
            ));
        }
        let bytes = serde_json::to_vec(&response).map_err(TransportError::from_source)?;
        serde_json::from_slice(&bytes).map_err(TransportError::from_source)
    }
}
