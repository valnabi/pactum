//! Versioned remote participant protocol. Transport errors always mean uncertainty.
use crate::{Domain, FencedResource, Grant, Operation, Reply};
use async_trait::async_trait;
use pactum_core::{ParticipantError, ParticipantId, TransactionId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Supported protocol version. Unknown versions fail before dispatch.
pub const VERSION: u32 = 1;
fn now() -> Result<u64, TransportError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| TransportError(e.to_string()))?;
    u64::try_from(elapsed.as_millis()).map_err(|e| TransportError(e.to_string()))
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
    fn expected_identity(&self) -> Result<Vec<u8>, TransportError> {
        let binding = serde_json::to_vec(&(
            self.version,
            &self.grant,
            &self.resource,
            &self.transaction,
            &self.operation,
        ))
        .map_err(|e| TransportError(e.to_string()))?;
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
    pub result: Result<Reply, String>,
}
/// Exchange failure. Effects may already be durable, including after timeout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportError(pub String);
impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for TransportError {}
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
            return Err(TransportError(
                "invalid protocol, identity, resource, domain or message size".into(),
            ));
        }
        if now()? >= request.deadline_millis {
            return Err(TransportError(
                "request deadline expired; completion uncertain".into(),
            ));
        }
        let result = self
            .resource
            .call(&request.grant, &request.transaction, request.operation)
            .await
            .map_err(|e| e.to_string());
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
            return Err(TransportError(
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
        let run = async {
            let millis = u64::try_from(self.timeout.as_millis())
                .map_err(|e| TransportError(e.to_string()))?;
            let deadline = now()?
                .checked_add(millis)
                .ok_or_else(|| TransportError("deadline overflow".into()))?;
            let request = Request::new(
                grant.clone(),
                self.id.clone(),
                tx.clone(),
                operation,
                deadline,
            )?;
            let expected = request.identity.clone();
            let response = self.transport.exchange(request).await?;
            if response.version != VERSION
                || response.identity != expected
                || response.resource != self.id
                || &response.transaction != tx
            {
                return Err(TransportError("uncorrelated remote response".into()));
            }
            response.result.map_err(TransportError)
        };
        match tokio::time::timeout(self.timeout, run).await {
            Ok(result) => {
                result.map_err(|e| ParticipantError(format!("remote completion uncertain: {e}")))
            }
            Err(_) => Err(ParticipantError(
                "remote deadline exceeded; completion uncertain".into(),
            )),
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
            return Err(TransportError("partition; completion uncertain".into()));
        }
        // Exercise the actual versioned serialization boundary.
        let bytes = serde_json::to_vec(&request).map_err(|e| TransportError(e.to_string()))?;
        let request = serde_json::from_slice(&bytes).map_err(|e| TransportError(e.to_string()))?;
        let response = self.endpoint.handle(request).await?;
        if self
            .lost_replies
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(TransportError(
                "reply lost after dispatch; completion uncertain".into(),
            ));
        }
        let bytes = serde_json::to_vec(&response).map_err(|e| TransportError(e.to_string()))?;
        serde_json::from_slice(&bytes).map_err(|e| TransportError(e.to_string()))
    }
}
