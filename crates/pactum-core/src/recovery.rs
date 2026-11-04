//! Optional single-owner recovery runner. Protocol journals remain the only
//! transaction authority; the dedicated scheduling journal contains no decisions.
use crate::{
    Cancellation, Coordinator, Error, Outcome, Status, TransactionId,
    execution::{OperationStatus, Progress},
};
use async_trait::async_trait;
use pactum_journal::Journal;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, mpsc, oneshot};

/// Existing protocol status, preserving mode-specific guarantees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Work {
    /// Two-phase transaction.
    Transaction(Status),
    /// Atomic, Chunked or StagedPublish operation.
    Operation(OperationStatus),
}
impl Work {
    /// Stable key within the service's dedicated namespace.
    pub fn key(&self) -> &str {
        match self {
            Self::Transaction(s) => s.transaction.as_str(),
            Self::Operation(s) => s.operation.as_str(),
        }
    }
    /// Whether protocol work is complete, including definite rejection.
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Transaction(s) => s.outcome != Outcome::InDoubt,
            Self::Operation(s) => s.progress.is_terminal(),
        }
    }
    /// Whether protocol-level cancellation paused a chunked batch.
    pub fn is_paused(&self) -> bool {
        matches!(self, Self::Operation(OperationStatus { progress: Progress::Chunked(s), .. }) if s.paused)
    }
    /// Latest local resource failure, which may be lost on process restart.
    pub fn last_error(&self) -> Option<&str> {
        match self {
            Self::Transaction(s) => s.last_error.as_deref(),
            Self::Operation(s) => s.last_error.as_deref(),
        }
    }
}

/// Adapter to existing recovery APIs. Implementations must preserve journal
/// barriers, permanent receipts and cancellation safety when futures are dropped.
#[async_trait]
pub trait RecoveryTarget: Send {
    /// Read indexed pending work without dispatching resource operations.
    fn pending_work(&self) -> Result<Vec<Work>, Error>;
    /// Last acknowledged protocol journal position, including replay.
    fn replay_position(&self) -> u64;
    /// Reconcile one existing key; never introduce a new request or decision rule.
    async fn recover_one(&mut self, key: &str, cancellation: &Cancellation) -> Result<Work, Error>;
    /// Explicitly resume protocol-level pauses. Scheduling pause is independent.
    fn resume_work(&mut self, _key: &str) -> Result<(), Error> {
        Ok(())
    }
}
#[async_trait]
impl<J: Journal> RecoveryTarget for Coordinator<J> {
    fn pending_work(&self) -> Result<Vec<Work>, Error> {
        self.healthy()?;
        self.pending
            .iter()
            .map(|id| self.status(id).map(|s| Work::Transaction(s.unwrap())))
            .collect()
    }
    fn replay_position(&self) -> u64 {
        self.durable_sequence
    }
    async fn recover_one(&mut self, key: &str, cancellation: &Cancellation) -> Result<Work, Error> {
        self.healthy()?;
        let id = TransactionId::new(key)?;
        if !self.states.contains_key(&id) {
            return Err(Error::Invalid("unknown transaction".into()));
        }
        self.recovery_passes += 1;
        self.reconcile(&id, cancellation)
            .await
            .map(Work::Transaction)
    }
}

/// Runner bounds. Resource operation timeouts remain the target's responsibility.
#[derive(Clone, Debug)]
pub struct Config {
    /// Positive polling interval; wake requests coalesce and bypass this delay.
    pub poll_interval: Duration,
    /// Positive initial retry delay.
    pub initial_backoff: Duration,
    /// Maximum retry delay, including jitter.
    pub max_backoff: Duration,
    /// Maximum keys attempted before checking commands and yielding.
    pub max_per_tick: usize,
    /// Bounded operator command queue capacity.
    pub command_capacity: usize,
    /// Independent jitter seed; supply a distinct value per deployment.
    pub jitter_seed: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            max_per_tick: 32,
            command_capacity: 32,
            jitter_seed: now(),
        }
    }
}
/// Durable scheduling classification; never a transaction decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reason {
    /// Last attempt left unresolved resource work.
    Transient,
    /// Operator suspended automatic dispatch.
    Paused,
    /// Invalid configuration requires operator intervention.
    Permanent(String),
    /// Operator requested another attempt.
    Retry,
    /// A native chunked batch needs explicit resume.
    ProtocolPaused,
}
/// Persisted timing and operator intent for one pending key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    /// Attempts reserved durably before dispatch, including interrupted attempts.
    pub attempts: u64,
    /// Unix milliseconds of last reserved attempt.
    pub last_attempt_ms: Option<u64>,
    /// Earliest next attempt, Unix milliseconds.
    pub next_attempt_ms: u64,
    /// First observation time for lag measurement.
    pub first_seen_ms: u64,
    /// Scheduling or protocol reason.
    pub reason: Reason,
    /// Last error retained across restart.
    pub last_error: Option<String>,
}
impl Schedule {
    fn new(time: u64) -> Self {
        Self {
            attempts: 0,
            last_attempt_ms: None,
            next_attempt_ms: time,
            first_seen_ms: time,
            reason: Reason::Retry,
            last_error: None,
        }
    }
    fn runnable(&self) -> bool {
        matches!(self.reason, Reason::Retry | Reason::Transient)
    }
}
/// One pending protocol status and its durable schedule, if observed before.
#[derive(Clone, Debug)]
pub struct Pending {
    /// Existing authoritative status.
    pub work: Work,
    /// Absent for newly discovered work before the first scheduling write.
    pub schedule: Option<Schedule>,
}
/// Process-local counters; durable attempt counts are available per key.
#[derive(Clone, Debug, Default)]
pub struct Metrics {
    /// Recovery attempts dispatched in this process.
    pub attempts: u64,
    /// Repeated attempts dispatched in this process.
    pub retries: u64,
    /// Terminal statuses observed after dispatch.
    pub completed: u64,
    /// Pending protocol work, including pauses.
    pub pending: usize,
    /// Age of oldest observed pending work.
    pub oldest_pending_ms: u64,
    /// Commands successfully persisted (audit records are in the schedule journal).
    pub controls: u64,
}
/// Query result; errors require reopening rather than returning stale health.
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// All pending/in-doubt work with operator-readable reasons.
    pub pending: Vec<Pending>,
    /// Last acknowledged protocol replay/barrier position.
    pub replay_position: u64,
    /// Last acknowledged scheduling journal position.
    pub schedule_position: u64,
    /// Process-local observability counters.
    pub metrics: Metrics,
}
/// Safe scheduling controls. None can change a durable commit/abort decision.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Control {
    /// Suspend future automatic dispatch; never undo in-flight work.
    Pause,
    /// Clear scheduling and native chunk pauses, then schedule immediately.
    Resume,
    /// Schedule immediately, retaining a native chunk pause.
    Retry,
}
/// Structured scheduling trace emitted after its audit record is durable.
/// Export through the application's tracing/metrics system. A trace never
/// grants permission to change a protocol decision.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trace {
    /// Stable pending key.
    pub key: String,
    /// Acknowledged scheduling state, or None when removed after completion.
    pub schedule: Option<Schedule>,
    /// Attempt, outcome or operator action recorded in the audit journal.
    pub action: String,
}
type Observer = Arc<dyn Fn(&Trace) + Send + Sync>;
enum Command {
    Query(oneshot::Sender<Result<Snapshot, Error>>),
    Control(String, Control, oneshot::Sender<Result<(), Error>>),
}
/// Cloneable wake, query and administrative control endpoint.
/// Authentication/authorization and audit export are the embedding application's responsibility.
pub struct Handle<T> {
    target: Arc<Mutex<T>>,
    commands: mpsc::Sender<Command>,
    wake: Arc<tokio::sync::Notify>,
    stop: Cancellation,
}
impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
            commands: self.commands.clone(),
            wake: self.wake.clone(),
            stop: self.stop.clone(),
        }
    }
}
impl<T> Handle<T> {
    /// Access the same exclusive protocol owner to submit work or register a
    /// returning participant. Release the guard before awaiting handle commands.
    pub fn target(&self) -> &Arc<Mutex<T>> {
        &self.target
    }
    /// Wake discovery early. Existing backoff deadlines and pauses are respected.
    pub fn wake(&self) {
        self.wake.notify_one();
    }
    /// Stop promptly by dropping a cooperative in-flight recovery future.
    /// Synchronous journal/backend calls must be bounded by their implementation.
    pub fn stop(&self) {
        self.stop.cancel();
    }
    /// Query healthy authoritative pending state, applying bounded queue backpressure.
    pub async fn query(&self) -> Result<Snapshot, Error> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Query(tx))
            .await
            .map_err(|_| stopped())?;
        rx.await.map_err(|_| stopped())?
    }
    /// Persist operator intent before acknowledging it. Pause takes effect after
    /// any already dispatched attempt; it is not a transaction cancellation.
    pub async fn control(&self, key: impl Into<String>, control: Control) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Control(key.into(), control, tx))
            .await
            .map_err(|_| stopped())?;
        rx.await.map_err(|_| stopped())?
    }
}
/// Optional runner, owning exactly one target and a separate single-owner journal.
/// Keep the schedule journal bound to the same namespace and enforce its owner
/// lock just as for the target journal. No distributed lease/fencing is supplied.
pub struct Runner<T, S> {
    target: Arc<Mutex<T>>,
    journal: S,
    schedules: BTreeMap<String, Schedule>,
    sequence: u64,
    config: Config,
    rng: u64,
    metrics: Metrics,
    cursor: Option<String>,
    poisoned: bool,
    observer: Option<Observer>,
}
impl<T: RecoveryTarget + 'static, S: Journal + 'static> Runner<T, S> {
    /// Validate/replay a dedicated schedule journal. `namespace` is a permanent
    /// application binding; never reuse it for another protocol journal/owner.
    pub fn open(target: T, mut journal: S, namespace: &str, config: Config) -> Result<Self, Error> {
        if namespace.is_empty()
            || config.poll_interval.is_zero()
            || config.initial_backoff.as_millis() == 0
            || config.max_backoff < config.initial_backoff
            || config.max_backoff.as_millis() > u64::MAX as u128
            || config.max_per_tick == 0
            || config.command_capacity == 0
            || config.command_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || std::time::Instant::now()
                .checked_add(config.poll_interval)
                .is_none()
        {
            return Err(Error::Invalid("invalid recovery configuration".into()));
        }
        if journal
            .checkpoint()
            .map_err(|e| Error::Corrupt(e.to_string()))?
            .is_some()
        {
            return Err(Error::Corrupt("schedule checkpoints unsupported".into()));
        }
        let mut sequence = 0u64;
        let mut schedules = BTreeMap::new();
        journal
            .visit(&mut |entry| {
                if entry.version != 20 || Some(entry.sequence) != sequence.checked_add(1) {
                    return Err(pactum_journal::JournalError(
                        "invalid recovery journal sequence/schema".into(),
                    ));
                }
                if sequence == 0 {
                    let binding: String = serde_json::from_slice(&entry.payload)
                        .map_err(|e| pactum_journal::JournalError(e.to_string()))?;
                    if binding != namespace {
                        return Err(pactum_journal::JournalError(
                            "recovery namespace mismatch".into(),
                        ));
                    }
                } else {
                    let event: Trace = serde_json::from_slice(&entry.payload)
                        .map_err(|e| pactum_journal::JournalError(e.to_string()))?;
                    TransactionId::new(&event.key)
                        .map_err(|e| pactum_journal::JournalError(e.to_string()))?;
                    if let Some(schedule) = event.schedule {
                        schedules.insert(event.key, schedule);
                    } else {
                        schedules.remove(&event.key);
                    }
                }
                sequence = entry.sequence;
                Ok(())
            })
            .map_err(|e| Error::Corrupt(e.to_string()))?;
        let rng = config.jitter_seed.max(1);
        let mut runner = Self {
            target: Arc::new(Mutex::new(target)),
            journal,
            schedules,
            sequence,
            config,
            rng,
            metrics: Metrics::default(),
            cursor: None,
            poisoned: false,
            observer: None,
        };
        if sequence == 0 {
            runner.write(
                &serde_json::to_vec(namespace).map_err(|e| Error::Invalid(e.to_string()))?,
            )?;
        }
        Ok(runner)
    }
    /// Attach a process-local structured trace sink. It must be bounded,
    /// nonblocking and nonpanicking; delivery is best effort across process death.
    /// The schedule journal retains the durable audit independently of this sink.
    pub fn observe(mut self, observer: impl Fn(&Trace) + Send + Sync + 'static) -> Self {
        self.observer = Some(Arc::new(observer));
        self
    }
    fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::InDoubt("reopen recovery schedule journal".into()));
        }
        self.poisoned = true;
        let seq = self
            .journal
            .append(20, bytes)
            .map_err(|e| Error::InDoubt(e.to_string()))?;
        let durable = self
            .journal
            .flush()
            .map_err(|e| Error::InDoubt(e.to_string()))?;
        if Some(seq) != self.sequence.checked_add(1) || durable != seq {
            return Err(Error::InDoubt("recovery barrier mismatch".into()));
        }
        self.sequence = seq;
        self.poisoned = false;
        Ok(())
    }
    fn save(&mut self, key: &str, schedule: Option<Schedule>, action: &str) -> Result<(), Error> {
        let event = Trace {
            key: key.into(),
            schedule,
            action: action.into(),
        };
        self.write(&serde_json::to_vec(&event).map_err(|e| Error::Invalid(e.to_string()))?)?;
        if let Some(observer) = &self.observer {
            observer(&event);
        }
        if let Some(s) = event.schedule {
            self.schedules.insert(key.into(), s);
        } else {
            self.schedules.remove(key);
        }
        Ok(())
    }
    fn delay(&mut self, attempts: u64) -> u64 {
        let cap = self.config.max_backoff.as_millis() as u64;
        let base = (self.config.initial_backoff.as_millis() as u64)
            .saturating_mul(1u64 << attempts.saturating_sub(1).min(63))
            .min(cap);
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        // Equal jitter: [base/2, base], with a positive minimum.
        (base / 2 + self.rng % (base - base / 2 + 1)).max(1)
    }
    /// Start one Tokio task. Moving the runner prevents repeated startup of the
    /// same owner. Await the join handle after `stop` before restarting or dropping.
    /// Errors stop dispatch; InDoubt requires dropping/reopening both owners.
    pub fn start(self) -> (Handle<T>, tokio::task::JoinHandle<Result<Self, Error>>) {
        let (commands, rx) = mpsc::channel(self.config.command_capacity);
        let handle = Handle {
            target: self.target.clone(),
            commands,
            wake: Arc::new(tokio::sync::Notify::new()),
            stop: Cancellation::default(),
        };
        let wake = handle.wake.clone();
        let stop = handle.stop.clone();
        let join = tokio::spawn(async move {
            let mut runner = self;
            tokio::select! {
                biased;
                _ = stop.cancelled() => {},
                result = runner.run(rx, wake, stop.clone()) => { result?; }
            }
            Ok(runner)
        });
        (handle, join)
    }
    async fn snapshot(&mut self) -> Result<Snapshot, Error> {
        let target = self.target.lock().await;
        let work = target.pending_work()?;
        let time = now();
        let mut metrics = self.metrics.clone();
        metrics.pending = work.len();
        metrics.oldest_pending_ms = work
            .iter()
            .filter_map(|w| self.schedules.get(w.key()))
            .map(|s| time.saturating_sub(s.first_seen_ms))
            .max()
            .unwrap_or(0);
        Ok(Snapshot {
            pending: work
                .into_iter()
                .map(|w| Pending {
                    schedule: self.schedules.get(w.key()).cloned(),
                    work: w,
                })
                .collect(),
            replay_position: target.replay_position(),
            schedule_position: self.sequence,
            metrics,
        })
    }
    async fn command(&mut self, command: Command) -> Result<(), Error> {
        match command {
            Command::Query(reply) => {
                let result = self.snapshot().await;
                let fatal = result.as_ref().err().cloned();
                let _ = reply.send(result);
                if let Some(e) = fatal {
                    return Err(e);
                }
            }
            Command::Control(key, control, reply) => {
                let result = self.apply_control(&key, &control).await;
                let fatal = result
                    .as_ref()
                    .err()
                    .filter(|e| !matches!(e, Error::Invalid(_)))
                    .cloned();
                let _ = reply.send(result);
                if let Some(e) = fatal {
                    return Err(e);
                }
            }
        }
        Ok(())
    }
    async fn apply_control(&mut self, key: &str, control: &Control) -> Result<(), Error> {
        let work = self.target.lock().await.pending_work()?;
        if !work.iter().any(|w| w.key() == key) {
            return Err(Error::Invalid("key is not pending".into()));
        }
        let mut schedule = self
            .schedules
            .get(key)
            .cloned()
            .unwrap_or_else(|| Schedule::new(now()));
        schedule.reason = if matches!(control, Control::Pause) {
            Reason::Paused
        } else {
            Reason::Retry
        };
        schedule.next_attempt_ms = now();
        // Persist scheduling intent first. A failed native resume remains paused,
        // and the operator can repeat it without changing a transaction decision.
        self.save(key, Some(schedule), &format!("operator:{control:?}"))?;
        if matches!(control, Control::Resume) {
            self.target.lock().await.resume_work(key)?;
        }
        self.metrics.controls += 1;
        Ok(())
    }
    async fn tick(&mut self, stop: &Cancellation) -> Result<(), Error> {
        let work = self.target.lock().await.pending_work()?;
        let mut keys: Vec<_> = work.iter().map(|w| w.key().to_owned()).collect();
        keys.sort();
        let stale: Vec<_> = self
            .schedules
            .keys()
            .filter(|key| keys.binary_search(key).is_err())
            .cloned()
            .collect();
        for key in stale {
            self.save(&key, None, "completed-externally")?;
        }
        if let Some(cursor) = &self.cursor {
            let pivot = keys.partition_point(|key| key <= cursor);
            keys.rotate_left(pivot);
        }
        let mut dispatched = 0;
        for key in keys {
            if stop.is_cancelled() || dispatched == self.config.max_per_tick {
                break;
            }
            let mut schedule = self
                .schedules
                .get(&key)
                .cloned()
                .unwrap_or_else(|| Schedule::new(now()));
            if !schedule.runnable() || schedule.next_attempt_ms > now() {
                continue;
            }
            self.cursor = Some(key.clone());
            schedule.attempts = schedule.attempts.saturating_add(1);
            schedule.last_attempt_ms = Some(now());
            let retry_delay = self.delay(schedule.attempts);
            schedule.next_attempt_ms = now().saturating_add(retry_delay);
            schedule.reason = Reason::Transient;
            self.save(&key, Some(schedule.clone()), "attempt")?;
            self.metrics.attempts += 1;
            self.metrics.retries += u64::from(schedule.attempts > 1);
            dispatched += 1;
            let target = self.target.clone();
            let result = tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(()),
                result = async { target.lock().await.recover_one(&key, &Cancellation::default()).await } => result,
            };
            match result {
                Ok(work) if work.is_terminal() => {
                    self.metrics.completed += 1;
                    self.save(&key, None, "completed")?;
                }
                Ok(work) => {
                    // Slow failed attempts still receive a full delay after returning.
                    schedule.next_attempt_ms = now().saturating_add(retry_delay);
                    if work.is_paused() {
                        schedule.reason = Reason::ProtocolPaused;
                    }
                    schedule.last_error = work.last_error().map(str::to_owned);
                    self.save(&key, Some(schedule), "unresolved")?;
                }
                Err(Error::Invalid(reason)) => {
                    schedule.reason = Reason::Permanent(reason.clone());
                    schedule.last_error = Some(reason);
                    self.save(&key, Some(schedule), "permanent")?;
                }
                Err(e) => return Err(e),
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }
    async fn run(
        &mut self,
        mut commands: mpsc::Receiver<Command>,
        wake: Arc<tokio::sync::Notify>,
        stop: Cancellation,
    ) -> Result<(), Error> {
        loop {
            if stop.is_cancelled() {
                return Ok(());
            }
            // Bound command processing too, so query floods cannot starve recovery.
            for _ in 0..self.config.command_capacity {
                match commands.try_recv() {
                    Ok(command) => self.command(command).await?,
                    Err(_) => break,
                }
            }
            self.tick(&stop).await?;
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(()),
                command = commands.recv() => match command { Some(c) => self.command(c).await?, None => return Ok(()) },
                _ = wake.notified() => {},
                _ = tokio::time::sleep(self.config.poll_interval) => {},
            }
        }
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn stopped() -> Error {
    Error::Invalid("recovery runner stopped; inspect join result".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{AtomicStatus, OperationId};
    use pactum_journal::{Entry, JournalError};
    use std::sync::{
        Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Clone, Default)]
    struct Memory {
        records: Arc<StdMutex<Vec<Entry>>>,
        fail: Arc<AtomicBool>,
    }
    impl Journal for Memory {
        fn append(&mut self, version: u32, payload: &[u8]) -> Result<u64, JournalError> {
            let mut records = self.records.lock().unwrap();
            let sequence = records.len() as u64 + 1;
            records.push(Entry {
                sequence,
                version,
                payload: payload.to_vec(),
            });
            Ok(sequence)
        }
        fn flush(&mut self) -> Result<u64, JournalError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(JournalError("lost barrier acknowledgement".into()));
            }
            Ok(self.records.lock().unwrap().len() as u64)
        }
        fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
            Ok(self.records.lock().unwrap().clone())
        }
    }
    #[derive(Clone)]
    struct Target {
        work: BTreeMap<String, Work>,
        calls: Arc<StdMutex<Vec<String>>>,
        invalid: bool,
        hang: bool,
        entered: Arc<tokio::sync::Notify>,
    }
    impl Target {
        fn new(keys: &[&str]) -> Self {
            Self {
                work: keys
                    .iter()
                    .map(|key| {
                        (
                            (*key).into(),
                            Work::Operation(OperationStatus {
                                operation: OperationId::new(*key).unwrap(),
                                progress: Progress::Atomic(AtomicStatus::Indeterminate),
                                last_error: Some("resource offline".into()),
                            }),
                        )
                    })
                    .collect(),
                calls: Arc::default(),
                invalid: false,
                hang: false,
                entered: Arc::default(),
            }
        }
    }
    #[async_trait]
    impl RecoveryTarget for Target {
        fn pending_work(&self) -> Result<Vec<Work>, Error> {
            Ok(self.work.values().cloned().collect())
        }
        fn replay_position(&self) -> u64 {
            42
        }
        async fn recover_one(&mut self, key: &str, _: &Cancellation) -> Result<Work, Error> {
            self.calls.lock().unwrap().push(key.into());
            self.entered.notify_one();
            if self.hang {
                std::future::pending::<()>().await;
            }
            if self.invalid {
                return Err(Error::Invalid("fix resource configuration".into()));
            }
            Ok(self.work[key].clone())
        }
    }
    fn config() -> Config {
        Config {
            max_per_tick: 1,
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_secs(1),
            poll_interval: Duration::from_millis(10),
            jitter_seed: 1,
            command_capacity: 1,
        }
    }
    #[tokio::test]
    async fn fairness_backoff_and_durable_attempts() {
        let target = Target::new(&["a", "b", "c"]);
        let calls = target.calls.clone();
        let memory = Memory::default();
        let mut runner = Runner::open(target.clone(), memory.clone(), "owner", config()).unwrap();
        let stop = Cancellation::default();
        for _ in 0..6 {
            for (key, mut schedule) in runner.schedules.clone() {
                schedule.next_attempt_ms = 0;
                runner.save(&key, Some(schedule), "test-due").unwrap();
            }
            runner.tick(&stop).await.unwrap();
        }
        assert_eq!(*calls.lock().unwrap(), ["a", "b", "c", "a", "b", "c"]);
        assert_eq!(runner.metrics.attempts, 6);
        assert_eq!(runner.metrics.retries, 3);
        for (key, mut schedule) in runner.schedules.clone() {
            schedule.next_attempt_ms = now() + 60_000;
            runner.save(&key, Some(schedule), "test-backoff").unwrap();
        }
        let schedules = runner.schedules.clone();
        drop(runner);
        let mut runner = Runner::open(target, memory, "owner", config()).unwrap();
        assert_eq!(runner.schedules, schedules);
        let time = now();
        runner.tick(&stop).await.unwrap();
        assert!(runner.schedules.values().all(|s| s.next_attempt_ms >= time));
        let mut distinct = std::collections::BTreeSet::new();
        for attempt in 1..100 {
            let delay = runner.delay(attempt);
            assert!((1..=1000).contains(&delay));
            if attempt > 10 {
                assert!(delay >= 500);
                distinct.insert(delay);
            }
        }
        assert!(distinct.len() > 1);
    }
    #[tokio::test]
    async fn controls_survive_restart_and_permanent_errors_need_operator() {
        let mut target = Target::new(&["a"]);
        target.invalid = true;
        let memory = Memory::default();
        let mut runner = Runner::open(target.clone(), memory.clone(), "owner", config()).unwrap();
        runner.tick(&Cancellation::default()).await.unwrap();
        assert!(matches!(runner.schedules["a"].reason, Reason::Permanent(_)));
        runner.tick(&Cancellation::default()).await.unwrap();
        assert_eq!(runner.metrics.attempts, 1);
        runner.apply_control("a", &Control::Pause).await.unwrap();
        drop(runner);
        let mut runner = Runner::open(target, memory.clone(), "owner", config()).unwrap();
        runner.tick(&Cancellation::default()).await.unwrap();
        assert_eq!(runner.metrics.attempts, 0);
        assert_eq!(runner.schedules["a"].reason, Reason::Paused);
        assert!(
            runner
                .apply_control("unknown", &Control::Retry)
                .await
                .is_err()
        );
        runner.apply_control("a", &Control::Retry).await.unwrap();
        runner.tick(&Cancellation::default()).await.unwrap();
        assert_eq!(runner.metrics.attempts, 1);
        assert!(
            memory
                .records
                .lock()
                .unwrap()
                .iter()
                .any(|e| String::from_utf8_lossy(&e.payload).contains("operator:Pause"))
        );
        assert!(Runner::open(Target::new(&[]), memory, "wrong-owner", config()).is_err());
    }
    #[tokio::test]
    async fn ambiguous_schedule_barrier_stops_before_resource_dispatch() {
        let target = Target::new(&["a"]);
        let calls = target.calls.clone();
        let memory = Memory::default();
        let mut runner = Runner::open(target.clone(), memory.clone(), "owner", config()).unwrap();
        memory.fail.store(true, Ordering::SeqCst);
        assert!(matches!(
            runner.tick(&Cancellation::default()).await,
            Err(Error::InDoubt(_))
        ));
        assert!(calls.lock().unwrap().is_empty());
        assert!(matches!(
            runner.tick(&Cancellation::default()).await,
            Err(Error::InDoubt(_))
        ));
        drop(runner);
        memory.fail.store(false, Ordering::SeqCst);
        let runner = Runner::open(target, memory, "owner", config()).unwrap();
        assert_eq!(runner.schedules["a"].attempts, 1);
        assert!(runner.schedules["a"].next_attempt_ms > 0);
    }
    #[tokio::test]
    async fn shutdown_interrupts_cooperative_attempt_and_command_queue_is_bounded() {
        let mut target = Target::new(&["a"]);
        target.hang = true;
        let entered = target.entered.clone();
        let runner = Runner::open(target, Memory::default(), "owner", config()).unwrap();
        let (handle, join) = runner.start();
        entered.notified().await;
        let (reply, _) = oneshot::channel();
        handle.commands.try_send(Command::Query(reply)).unwrap();
        let (reply, _) = oneshot::channel();
        assert!(matches!(
            handle.commands.try_send(Command::Query(reply)),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        handle.stop();
        let runner = tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(runner.schedules["a"].attempts, 1);
        assert!(handle.query().await.is_err());
        let (handle, join) = runner.start();
        assert_eq!(handle.query().await.unwrap().replay_position, 42);
        handle.stop();
        join.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn wake_discovers_work_and_queries_expose_reasons_and_positions() {
        let mut cfg = config();
        cfg.poll_interval = Duration::from_secs(3600);
        let runner = Runner::open(Target::new(&[]), Memory::default(), "owner", cfg).unwrap();
        let (handle, join) = runner.start();
        assert!(handle.query().await.unwrap().pending.is_empty());
        {
            let mut target = handle.target().lock().await;
            target.work = Target::new(&["new"]).work;
        }
        handle.wake();
        for _ in 0..20 {
            let snapshot = handle.query().await.unwrap();
            if snapshot.metrics.attempts == 1 {
                assert_eq!(snapshot.metrics.pending, 1);
                assert_eq!(
                    snapshot.pending[0]
                        .schedule
                        .as_ref()
                        .unwrap()
                        .last_error
                        .as_deref(),
                    Some("resource offline")
                );
                assert!(snapshot.schedule_position >= 3);
                break;
            }
            tokio::task::yield_now().await;
        }
        handle.control("new", Control::Pause).await.unwrap();
        assert_eq!(
            handle.query().await.unwrap().pending[0]
                .schedule
                .as_ref()
                .unwrap()
                .reason,
            Reason::Paused
        );
        handle.stop();
        join.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn shutdown_does_not_wait_for_application_target_lock() {
        let runner = Runner::open(Target::new(&[]), Memory::default(), "owner", config()).unwrap();
        let (handle, join) = runner.start();
        handle.query().await.unwrap();
        let guard = handle.target().lock().await;
        handle.wake();
        tokio::task::yield_now().await;
        handle.stop();
        let runner = tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(guard);
        drop(runner);
    }
    #[test]
    fn invalid_configuration_is_rejected_before_start() {
        for cfg in [
            Config {
                command_capacity: usize::MAX,
                ..config()
            },
            Config {
                poll_interval: Duration::MAX,
                ..config()
            },
            Config {
                initial_backoff: Duration::ZERO,
                ..config()
            },
            Config {
                max_per_tick: 0,
                ..config()
            },
        ] {
            assert!(matches!(
                Runner::open(Target::new(&[]), Memory::default(), "owner", cfg),
                Err(Error::Invalid(_))
            ));
        }
    }
}
