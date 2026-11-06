//! Deterministic simulation, bounded interleaving exploration, trace shrinking,
//! and SQLite / child-process crash cross-checking (Epic #70: #83, #84, #85, #86).

use async_trait::async_trait;
use pactum_core::{
    Cancellation, Coordinator, Enlistment, ErrorCode, Participant, ParticipantError, ParticipantId,
    ParticipantStatus, RetryPolicy, TransactionId, Vote,
    execution::{
        AtomicRequest, Backend, BackendError, ChunkedRequest, Engine, Execution, ExecutionPolicy,
        OperationId, PublishRequest, PublishResult, Request, UnitResult,
    },
    simulation::{
        BoundaryPhase, ExplorationConfig, FaultAction, FaultRule, Invariant, InvariantViolation,
        ScheduleAction, SimAuthority, SimClock, SimComponent, SimController, SimJournalStore,
        SimParticipantMessage, SimParticipantStore, SimSchedule, SimValue, SimWorkload,
        StateExplorer,
    },
};
use pactum_demo::DurableCounter;
use pactum_journal::{Checkpoint, Entry, Journal, JournalError, Retention, Sequence};
use pactum_sqlite::{SqliteBackend, SqliteJournal};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

fn fast_policy() -> RetryPolicy {
    RetryPolicy {
        attempts: 2,
        timeout: Duration::from_millis(100),
        backoff: Duration::ZERO,
    }
}

#[tokio::test]
async fn sim_clock_journal_participant_and_backend_fault_barriers_are_deterministic() {
    let clock = SimClock::new(1_000);
    assert_eq!(clock.now_millis().unwrap(), 1_000);
    assert_eq!(clock.advance_millis(250).unwrap(), 1_250);
    assert!(clock.set_millis(900).is_err());

    clock.inject_fault(Some(ErrorCode::Unavailable));
    assert_eq!(
        clock.now_millis().unwrap_err().code(),
        ErrorCode::Unavailable
    );
    assert_eq!(clock.now_millis().unwrap(), 1_250);

    let authority = SimAuthority::new("sim-domain", clock.clone()).unwrap();
    assert!(SimAuthority::new("", clock.clone()).is_err());
    let grant1 = authority.acquire("owner-a", 500).unwrap();
    assert_eq!(grant1.epoch, 1);
    assert_eq!(
        authority.acquire("owner-b", 500).unwrap_err().code(),
        ErrorCode::OwnershipBusy
    );
    authority.renew(&grant1, 1_000).unwrap();
    clock.advance_millis(1_000).unwrap();
    assert_eq!(
        authority.validate(&grant1).unwrap_err().code(),
        ErrorCode::StaleOwner
    );
    let grant2 = authority.acquire("owner-b", 500).unwrap();
    assert_eq!(grant2.epoch, 2);
    assert_eq!(
        authority.validate(&grant1).unwrap_err().code(),
        ErrorCode::StaleOwner
    );

    // Conformance & exclusive unfenced ownership on SimJournalStore.
    let controller = SimController::new(clock.clone(), Some(authority.clone()));
    let store = SimJournalStore::new("conformance-journal", controller.clone()).unwrap();
    assert!(SimJournalStore::new("", controller.clone()).is_err());
    let mut conformance_handle = store.open().unwrap();
    pactum_journal::check_conformance(&mut conformance_handle).unwrap();
    drop(conformance_handle);

    let mut held = store.open().unwrap();
    assert_eq!(
        store.open().unwrap_err().code(),
        ErrorCode::OwnershipBusy,
        "second unfenced handle must fail closed while first is active"
    );
    held.append(1, b"staged-before-crash").unwrap();
    store.crash_active_session();
    drop(held);
    controller.clear_crash();
    assert!(store.discarded_staged_records() >= 1);

    // Fenced journal ownership & corruption fail-closed checks.
    let mismatched_store = SimJournalStore::new("other-domain", controller.clone()).unwrap();
    assert_eq!(
        mismatched_store
            .open_fenced(grant2.clone())
            .unwrap_err()
            .code(),
        ErrorCode::InvalidInput
    );
    let fenced_store = SimJournalStore::new("sim-domain", controller.clone()).unwrap();
    assert_eq!(
        fenced_store.open_fenced(grant1.clone()).unwrap_err().code(),
        ErrorCode::StaleOwner
    );
    let mut fenced_journal = fenced_store.open_fenced(grant2.clone()).unwrap();
    fenced_journal.append(1, b"record-1").unwrap();
    fenced_journal.flush().unwrap();
    fenced_journal
        .compact(1, 1, b"cp-state", Retention::Archive)
        .unwrap();
    assert_eq!(fenced_store.archived_entries().len(), 1);

    fenced_store.inject_corruption("tampered checkpoint checksum");
    assert_eq!(
        fenced_journal.checkpoint().unwrap_err().code(),
        ErrorCode::CorruptHistory
    );

    // Participant duplicate delivery, delay, message reorder queue, and fencing.
    let pid = ParticipantId::new("a").unwrap();
    let p_store = SimParticipantStore::new(pid, controller.clone(), None);
    let tx1 = TransactionId::new("tx-1").unwrap();

    let stale_handle = p_store.fenced_handle(grant1);
    assert_eq!(
        stale_handle.prepare(&tx1, b"10").await.unwrap_err().code(),
        ErrorCode::StaleOwner
    );

    let live_handle = p_store.fenced_handle(grant2);
    // Inject delay + duplicate delivery on prepare.
    let next_step = controller.step_count() + 1;
    controller
        .add_rule(FaultRule {
            step: next_step,
            component: Some(SimComponent::Participant),
            operation: Some("prepare".into()),
            phase: BoundaryPhase::BeforeEffect,
            action: FaultAction::DelayMillis { millis: 50 },
        })
        .unwrap();
    controller
        .add_rule(FaultRule {
            step: next_step + 1,
            component: Some(SimComponent::Participant),
            operation: Some("prepare".into()),
            phase: BoundaryPhase::AfterEffect,
            action: FaultAction::DuplicateDelivery {
                extra_deliveries: 2,
            },
        })
        .unwrap();
    let before_ms = clock.current_millis();
    let vote1 = live_handle.prepare(&tx1, b"10").await.unwrap();
    assert_eq!(clock.current_millis(), before_ms + 50);
    let Vote::Prepared(token1) = vote1 else {
        panic!("expected prepared vote");
    };

    // Enqueue out-of-order messages and deliver deterministically; commit must apply only once.
    p_store.enqueue_message(SimParticipantMessage::Prepare {
        tx: tx1.clone(),
        payload: b"10".to_vec(),
    });
    p_store.enqueue_message(SimParticipantMessage::Commit {
        tx: tx1.clone(),
        token: token1.clone(),
    });
    p_store.enqueue_message(SimParticipantMessage::Commit {
        tx: tx1.clone(),
        token: token1,
    });
    let results = p_store.drain_reordered(42).await;
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(p_store.value(), 10);
    assert_eq!(p_store.commit_effect_count(&tx1), 1);
}

#[tokio::test]
async fn state_explorer_verifies_invariants_across_2pc_native_saga_and_fenced_failover() {
    let workloads = SimWorkload::canonical_suite();
    let config = ExplorationConfig {
        seed: 0x51A7_E070,
        max_schedules: 6,
        max_actions_per_schedule: 4,
        include_boundary_sweep: true,
    };
    let explorer = StateExplorer::new(config.clone()).unwrap();
    let report1 = explorer.explore(&workloads).await.unwrap();
    assert!(
        report1.failure.is_none(),
        "unexpected invariant failure: {:?}",
        report1.failure
    );
    assert!(report1.schedules_explored > 100);
    assert!(report1.total_crashes_recovered > 20);
    assert!(report1.total_stale_rejections > 0);

    // Verify deterministic reproducibility across identical exploration runs.
    let report2 = explorer.explore(&workloads).await.unwrap();
    assert_eq!(report1, report2);
}

#[tokio::test]
async fn failure_traces_serialize_replay_and_shrink_to_minimal_counterexample() {
    assert!(
        StateExplorer::new(ExplorationConfig {
            max_schedules: 0,
            ..ExplorationConfig::default()
        })
        .is_err()
    );
    assert!(
        StateExplorer::new(ExplorationConfig {
            max_actions_per_schedule: 99,
            ..ExplorationConfig::default()
        })
        .is_err()
    );
    assert!(SimSchedule::from_json("{invalid json").is_err());

    let explorer = StateExplorer::new(ExplorationConfig::default()).unwrap();
    let noisy_schedule = SimSchedule {
        seed: 999,
        workload: SimWorkload::TwoPhaseCommit {
            reject_second: false,
            cancel_before_decision: false,
            compact_after_recovery: false,
        },
        actions: vec![
            ScheduleAction::AdvanceClock { millis: 250 },
            ScheduleAction::ReorderParticipantMessages { seed: 77 },
            ScheduleAction::InjectFault {
                rule: FaultRule {
                    step: 1,
                    component: Some(SimComponent::Journal),
                    operation: Some("replay".into()),
                    phase: BoundaryPhase::BeforeEffect,
                    action: FaultAction::DelayMillis { millis: 40 },
                },
            },
            ScheduleAction::InjectFault {
                rule: FaultRule {
                    step: 0,
                    component: Some(SimComponent::Participant),
                    operation: Some("prepare".into()),
                    phase: BoundaryPhase::BeforeEffect,
                    action: FaultAction::DuplicateDelivery {
                        extra_deliveries: 4,
                    },
                },
            },
            ScheduleAction::CrashAndReopen,
            ScheduleAction::ExtraRecoveryPass,
        ],
    };

    // Round-trip JSON fixture serialization & deterministic replay check.
    let json = noisy_schedule.to_json().unwrap();
    let parsed = SimSchedule::from_json(&json).unwrap();
    assert_eq!(parsed, noisy_schedule);
    let trace_a = explorer.run_schedule(&parsed).await.unwrap();
    let trace_b = explorer.run_schedule(&noisy_schedule).await.unwrap();
    assert_eq!(trace_a, trace_b);
    assert!(trace_a.to_string().contains("TwoPhaseCommit"));

    // Shrink a 6-action trace using a synthetic predicate that triggers when duplicate
    // prepare delivery occurs on Participant::prepare.
    let shrunk = explorer
        .shrink_with(&noisy_schedule, |trace| {
            trace
                .events
                .iter()
                .find(|ev| {
                    ev.component == SimComponent::Participant
                        && ev.operation == "prepare"
                        && matches!(ev.fault, Some(FaultAction::DuplicateDelivery { .. }))
                })
                .map(|ev| InvariantViolation {
                    invariant: Invariant::ExactlyOnceEffects,
                    step: ev.step,
                    detail: "synthetic duplicate-prepare trigger".into(),
                })
        })
        .await
        .unwrap()
        .unwrap();

    assert_eq!(shrunk.original_schedule.actions.len(), 6);
    assert_eq!(shrunk.minimal_schedule.actions.len(), 1);
    assert_eq!(
        shrunk.minimal_schedule.actions[0],
        ScheduleAction::InjectFault {
            rule: FaultRule {
                step: 0,
                component: Some(SimComponent::Participant),
                operation: Some("prepare".into()),
                phase: BoundaryPhase::BeforeEffect,
                action: FaultAction::DuplicateDelivery {
                    extra_deliveries: 1,
                },
            },
        }
    );
    let rendered = shrunk.to_string();
    assert!(rendered.contains("reduced 6 -> 1 actions"));
    assert!(rendered.contains("ExactlyOnceEffects"));
}

/// Wrapper around `SqliteJournal` that shares the exact boundary step numbering and fault rules
/// of `SimController`, parking the child process when `FaultAction::CrashProcess` fires in
/// child-process mode so the parent can terminate it with `Child::kill` without running destructors.
struct ControlledSqliteJournal {
    inner: SqliteJournal,
    controller: SimController,
    crash_dir: Option<PathBuf>,
}

impl ControlledSqliteJournal {
    fn maybe_park_for_os_kill(&self, step: usize) {
        if let Some(dir) = &self.crash_dir {
            std::fs::write(dir.join("ready"), step.to_string()).unwrap();
            loop {
                std::thread::park();
            }
        }
    }

    fn before(&mut self, operation: &str) -> Result<usize, JournalError> {
        let (step, fault) = self.controller.enter_boundary(
            SimComponent::Journal,
            "sqlite-journal",
            operation,
            BoundaryPhase::BeforeEffect,
            None,
        );
        if let Some(action) = fault {
            match action {
                FaultAction::FailBeforeEffect { code } => {
                    return Err(JournalError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            code,
                            format!("sqlite journal {operation} failed before effect"),
                        ),
                    ));
                }
                FaultAction::CrashProcess => {
                    self.maybe_park_for_os_kill(step);
                    return Err(JournalError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            ErrorCode::DurabilityUnknown,
                            format!("sqlite journal {operation} crashed before effect"),
                        ),
                    ));
                }
                _ => {}
            }
        }
        Ok(step)
    }

    fn after(&mut self, operation: &str) -> Result<usize, JournalError> {
        let (step, fault) = self.controller.enter_boundary(
            SimComponent::Journal,
            "sqlite-journal",
            operation,
            BoundaryPhase::AfterEffect,
            None,
        );
        if let Some(action) = fault {
            match action {
                FaultAction::LoseAck { code } => {
                    return Err(JournalError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            code,
                            format!("sqlite journal {operation} lost ack"),
                        ),
                    ));
                }
                FaultAction::CrashProcess => {
                    self.maybe_park_for_os_kill(step);
                    return Err(JournalError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            ErrorCode::DurabilityUnknown,
                            format!("sqlite journal {operation} crashed after effect"),
                        ),
                    ));
                }
                _ => {}
            }
        }
        Ok(step)
    }
}

impl Journal for ControlledSqliteJournal {
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError> {
        self.before("append")?;
        let seq = self.inner.append(version, payload)?;
        self.after("append")?;
        Ok(seq)
    }

    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.before("flush")?;
        let seq = self.inner.flush()?;
        self.after("flush")?;
        Ok(seq)
    }

    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.before("replay")?;
        let entries = self.inner.replay()?;
        self.after("replay")?;
        Ok(entries)
    }

    fn checkpoint(&mut self) -> Result<Option<Checkpoint>, JournalError> {
        self.before("checkpoint")?;
        let cp = self.inner.checkpoint()?;
        self.after("checkpoint")?;
        Ok(cp)
    }

    fn compact(
        &mut self,
        watermark: Sequence,
        version: u32,
        payload: &[u8],
        retention: Retention,
    ) -> Result<(), JournalError> {
        self.before("compact")?;
        self.inner.compact(watermark, version, payload, retention)?;
        self.after("compact")?;
        Ok(())
    }
}

struct ControlledSqliteParticipant {
    resource: Arc<DurableCounter>,
    controller: SimController,
    crash_dir: Option<PathBuf>,
}

impl ControlledSqliteParticipant {
    fn maybe_park_for_os_kill(&self, step: usize) {
        if let Some(dir) = &self.crash_dir {
            std::fs::write(dir.join("ready"), step.to_string()).unwrap();
            loop {
                std::thread::park();
            }
        }
    }

    fn before(&self, operation: &str) -> Result<Option<FaultAction>, ParticipantError> {
        let target = self.resource.id().as_str().to_owned();
        let (step, fault) = self.controller.enter_boundary(
            SimComponent::Participant,
            &target,
            operation,
            BoundaryPhase::BeforeEffect,
            None,
        );
        if let Some(action) = &fault {
            match action {
                FaultAction::FailBeforeEffect { code } => {
                    return Err(ParticipantError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            *code,
                            format!("sqlite participant {operation} failed before effect"),
                        ),
                    ));
                }
                FaultAction::CrashProcess => {
                    self.maybe_park_for_os_kill(step);
                    return Err(ParticipantError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            ErrorCode::Unavailable,
                            format!("sqlite participant {operation} crashed before effect"),
                        ),
                    ));
                }
                _ => {}
            }
        }
        Ok(fault)
    }

    fn after(&self, operation: &str) -> Result<(), ParticipantError> {
        let target = self.resource.id().as_str().to_owned();
        let (step, fault) = self.controller.enter_boundary(
            SimComponent::Participant,
            &target,
            operation,
            BoundaryPhase::AfterEffect,
            None,
        );
        if let Some(action) = &fault {
            match action {
                FaultAction::LoseAck { code } => {
                    return Err(ParticipantError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            *code,
                            format!("sqlite participant {operation} lost ack"),
                        ),
                    ));
                }
                FaultAction::CrashProcess => {
                    self.maybe_park_for_os_kill(step);
                    return Err(ParticipantError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            ErrorCode::TransportUnknown,
                            format!("sqlite participant {operation} crashed after effect"),
                        ),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Participant for ControlledSqliteParticipant {
    fn id(&self) -> ParticipantId {
        self.resource.id()
    }

    async fn prepare(&self, tx: &TransactionId, payload: &[u8]) -> Result<Vote, ParticipantError> {
        let before = self.before("prepare")?;
        let vote = self.resource.prepare(tx, payload).await;
        if let Some(FaultAction::DuplicateDelivery { extra_deliveries }) = before {
            for _ in 0..extra_deliveries {
                let _ = self.resource.prepare(tx, payload).await;
            }
        }
        self.after("prepare")?;
        vote
    }

    async fn commit(&self, tx: &TransactionId, token: &[u8]) -> Result<(), ParticipantError> {
        let before = self.before("commit")?;
        let res = self.resource.commit(tx, token).await;
        if let Some(FaultAction::DuplicateDelivery { extra_deliveries }) = before {
            for _ in 0..extra_deliveries {
                let _ = self.resource.commit(tx, token).await;
            }
        }
        self.after("commit")?;
        res
    }

    async fn abort(
        &self,
        tx: &TransactionId,
        token: Option<&[u8]>,
    ) -> Result<(), ParticipantError> {
        let before = self.before("abort")?;
        let res = self.resource.abort(tx, token).await;
        if let Some(FaultAction::DuplicateDelivery { extra_deliveries }) = before {
            for _ in 0..extra_deliveries {
                let _ = self.resource.abort(tx, token).await;
            }
        }
        self.after("abort")?;
        res
    }

    async fn status(&self, tx: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        self.before("status")?;
        let res = self.resource.status(tx).await;
        self.after("status")?;
        res
    }
}

struct ControlledSqliteBackend {
    inner: SqliteBackend,
    controller: SimController,
}

impl ControlledSqliteBackend {
    fn before(&mut self, operation: &str) -> Result<Option<FaultAction>, BackendError> {
        let (_, fault) = self.controller.enter_boundary(
            SimComponent::Backend,
            "sqlite-backend",
            operation,
            BoundaryPhase::BeforeEffect,
            None,
        );
        if let Some(action) = &fault {
            match action {
                FaultAction::FailBeforeEffect { code } => {
                    return Err(BackendError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            *code,
                            format!("sqlite backend {operation} failed before effect"),
                        ),
                    ));
                }
                FaultAction::CrashProcess => {
                    return Err(BackendError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            ErrorCode::Unavailable,
                            format!("sqlite backend {operation} crashed before effect"),
                        ),
                    ));
                }
                _ => {}
            }
        }
        Ok(fault)
    }

    fn after(&mut self, operation: &str) -> Result<(), BackendError> {
        let (_, fault) = self.controller.enter_boundary(
            SimComponent::Backend,
            "sqlite-backend",
            operation,
            BoundaryPhase::AfterEffect,
            None,
        );
        if let Some(action) = &fault {
            match action {
                FaultAction::LoseAck { code } => {
                    return Err(BackendError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            *code,
                            format!("sqlite backend {operation} lost ack"),
                        ),
                    ));
                }
                FaultAction::CrashProcess => {
                    return Err(BackendError::from_diagnostic(
                        pactum_core::Diagnostic::message(
                            ErrorCode::AdapterUnknown,
                            format!("sqlite backend {operation} crashed after effect"),
                        ),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl Backend for ControlledSqliteBackend {
    fn identity(&self) -> String {
        self.inner.identity()
    }

    fn unit(
        &mut self,
        operation: &OperationId,
        request: &Request,
        chunk: Option<usize>,
    ) -> Result<UnitResult, BackendError> {
        let before = self.before("unit")?;
        let res = self.inner.unit(operation, request, chunk);
        if let Some(FaultAction::DuplicateDelivery { extra_deliveries }) = before {
            for _ in 0..extra_deliveries {
                let _ = self.inner.unit(operation, request, chunk);
            }
        }
        self.after("unit")?;
        res
    }

    fn stage(
        &mut self,
        operation: &OperationId,
        request: &PublishRequest,
    ) -> Result<(), BackendError> {
        let before = self.before("stage")?;
        let res = self.inner.stage(operation, request);
        if let Some(FaultAction::DuplicateDelivery { extra_deliveries }) = before {
            for _ in 0..extra_deliveries {
                let _ = self.inner.stage(operation, request);
            }
        }
        self.after("stage")?;
        res
    }

    fn validate(
        &mut self,
        operation: &OperationId,
        request: &PublishRequest,
    ) -> Result<bool, BackendError> {
        let before = self.before("validate")?;
        let res = self.inner.validate(operation, request);
        if let Some(FaultAction::DuplicateDelivery { extra_deliveries }) = before {
            for _ in 0..extra_deliveries {
                let _ = self.inner.validate(operation, request);
            }
        }
        self.after("validate")?;
        res
    }

    fn publish(
        &mut self,
        operation: &OperationId,
        request: &PublishRequest,
    ) -> Result<PublishResult, BackendError> {
        let before = self.before("publish")?;
        let res = self.inner.publish(operation, request);
        if let Some(FaultAction::DuplicateDelivery { extra_deliveries }) = before {
            for _ in 0..extra_deliveries {
                let _ = self.inner.publish(operation, request);
            }
        }
        self.after("publish")?;
        res
    }

    fn cleanup(
        &mut self,
        operation: &OperationId,
        request: &PublishRequest,
    ) -> Result<(), BackendError> {
        let before = self.before("cleanup")?;
        let res = self.inner.cleanup(operation, request);
        if let Some(FaultAction::DuplicateDelivery { extra_deliveries }) = before {
            for _ in 0..extra_deliveries {
                let _ = self.inner.cleanup(operation, request);
            }
        }
        self.after("cleanup")?;
        res
    }
}

#[test]
fn simulation_crash_child() {
    let Some(dir) = std::env::var_os("PACTUM_SIM_CHILD_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let schedule_json = std::fs::read_to_string(dir.join("schedule.json")).unwrap();
    let schedule = SimSchedule::from_json(&schedule_json).unwrap();
    let SimWorkload::TwoPhaseCommit {
        reject_second,
        cancel_before_decision,
        ..
    } = schedule.workload
    else {
        panic!("child helper expects TwoPhaseCommit workload");
    };

    let clock = SimClock::new(1_000);
    let controller = SimController::new(clock, None);
    for action in &schedule.actions {
        if let ScheduleAction::InjectFault { rule } = action {
            controller.add_rule(rule.clone()).unwrap();
        }
    }

    let pid_a = ParticipantId::new("a").unwrap();
    let pid_b = ParticipantId::new("b").unwrap();
    let a = Arc::new(DurableCounter::open(dir.join("a.db"), pid_a.clone()).unwrap());
    let b = Arc::new(DurableCounter::open(dir.join("b.db"), pid_b.clone()).unwrap());
    let participants: Vec<Arc<dyn Participant>> = vec![
        Arc::new(ControlledSqliteParticipant {
            resource: a,
            controller: controller.clone(),
            crash_dir: Some(dir.clone()),
        }),
        Arc::new(ControlledSqliteParticipant {
            resource: b,
            controller: controller.clone(),
            crash_dir: Some(dir.clone()),
        }),
    ];
    let journal = ControlledSqliteJournal {
        inner: SqliteJournal::open(dir.join("j.db")).unwrap(),
        controller,
        crash_dir: Some(dir.clone()),
    };
    if let Ok(mut coord) = Coordinator::open(journal, participants, fast_policy()) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let cancel = Cancellation::default();
            if cancel_before_decision {
                cancel.cancel();
            }
            let requests = vec![
                Enlistment {
                    participant: pid_b,
                    payload: if reject_second {
                        b"-1".to_vec()
                    } else {
                        b"10".to_vec()
                    },
                },
                Enlistment {
                    participant: pid_a,
                    payload: b"10".to_vec(),
                },
            ];
            let _ = coord
                .execute(TransactionId::new("sim-tx").unwrap(), requests, &cancel)
                .await;
        });
    }
    std::fs::write(dir.join("done"), "1").unwrap();
}

fn run_2pc_schedule_in_child_process(dir: &Path, schedule: &SimSchedule) {
    std::fs::write(dir.join("schedule.json"), schedule.to_json().unwrap()).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "simulation_crash_child", "--nocapture"])
        .env("PACTUM_SIM_CHILD_DIR", dir)
        .spawn()
        .unwrap();
    let ready = dir.join("ready");
    let done = dir.join("done");
    while !ready.exists() && !done.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "child exited unexpectedly: {status}");
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    if ready.exists() {
        child.kill().unwrap();
        let _ = child.wait();
    } else {
        assert!(child.wait().unwrap().success());
    }
}

async fn verify_2pc_schedule_matches_sqlite_and_child_process(
    explorer: &StateExplorer,
    schedule: &SimSchedule,
    use_os_child_kill: bool,
) {
    let sim_trace = explorer.run_schedule(schedule).await.unwrap();
    assert!(
        sim_trace.violation.is_none(),
        "simulator reported violation: {:?}",
        sim_trace.violation
    );
    let sim_decision = sim_trace.summary.two_phase_decision;
    let sim_outcome = sim_trace.summary.two_phase_outcome;
    let sim_a = sim_trace.summary.resource_values["a"];
    let sim_b = sim_trace.summary.resource_values["b"];
    let sim_seq = sim_trace.summary.durable_sequence;

    let SimWorkload::TwoPhaseCommit {
        reject_second,
        cancel_before_decision,
        compact_after_recovery,
    } = schedule.workload
    else {
        unreachable!();
    };

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    let pid_a = ParticipantId::new("a").unwrap();
    let pid_b = ParticipantId::new("b").unwrap();
    let tx = TransactionId::new("sim-tx").unwrap();
    let requests = vec![
        Enlistment {
            participant: pid_b.clone(),
            payload: if reject_second {
                b"-1".to_vec()
            } else {
                b"10".to_vec()
            },
        },
        Enlistment {
            participant: pid_a.clone(),
            payload: b"10".to_vec(),
        },
    ];

    if use_os_child_kill {
        run_2pc_schedule_in_child_process(path, schedule);
    } else {
        let clock = SimClock::new(1_000);
        let controller = SimController::new(clock, None);
        for action in &schedule.actions {
            if let ScheduleAction::InjectFault { rule } = action {
                controller.add_rule(rule.clone()).unwrap();
            }
        }
        let a = Arc::new(DurableCounter::open(path.join("a.db"), pid_a.clone()).unwrap());
        let b = Arc::new(DurableCounter::open(path.join("b.db"), pid_b.clone()).unwrap());
        let participants: Vec<Arc<dyn Participant>> = vec![
            Arc::new(ControlledSqliteParticipant {
                resource: a,
                controller: controller.clone(),
                crash_dir: None,
            }),
            Arc::new(ControlledSqliteParticipant {
                resource: b,
                controller: controller.clone(),
                crash_dir: None,
            }),
        ];
        let journal = ControlledSqliteJournal {
            inner: SqliteJournal::open(path.join("j.db")).unwrap(),
            controller,
            crash_dir: None,
        };
        if let Ok(mut coord) = Coordinator::open(journal, participants, fast_policy()) {
            let cancel = Cancellation::default();
            if cancel_before_decision {
                cancel.cancel();
            }
            let _ = coord.execute(tx.clone(), requests.clone(), &cancel).await;
        }
    }

    // Recovery pass on clean SQLite handles (matching `run_two_phase` in `StateExplorer`).
    let a = Arc::new(DurableCounter::open(path.join("a.db"), pid_a).unwrap());
    let b = Arc::new(DurableCounter::open(path.join("b.db"), pid_b).unwrap());
    let mut coord = Coordinator::open(
        SqliteJournal::open(path.join("j.db")).unwrap(),
        vec![a.clone(), b.clone()],
        fast_policy(),
    )
    .unwrap();
    let _ = coord.recover(&Cancellation::default()).await.unwrap();
    if compact_after_recovery && coord.diagnostics().durable_records > 0 {
        coord.compact(Retention::Compact).unwrap();
    }
    let final_status = coord.status(&tx).unwrap();
    let sqlite_decision = final_status.as_ref().and_then(|s| s.decision);
    let sqlite_outcome = final_status.as_ref().map(|s| s.outcome.clone());
    let sqlite_seq = coord.diagnostics().durable_records;

    assert_eq!(sqlite_decision, sim_decision, "schedule: {schedule:?}");
    assert_eq!(sqlite_outcome, sim_outcome, "schedule: {schedule:?}");
    assert_eq!(a.value().await.unwrap(), sim_a, "schedule: {schedule:?}");
    assert_eq!(b.value().await.unwrap(), sim_b, "schedule: {schedule:?}");
    assert_eq!(sqlite_seq, sim_seq, "schedule: {schedule:?}");
}

async fn verify_native_schedule_matches_sqlite(explorer: &StateExplorer, schedule: &SimSchedule) {
    let sim_trace = explorer.run_schedule(schedule).await.unwrap();
    assert!(sim_trace.violation.is_none());
    let sim_progress = sim_trace.summary.native_progress.clone().unwrap();
    let sim_seq = sim_trace.summary.durable_sequence;

    let (op_id, request, policy, compact_after_recovery) = match &schedule.workload {
        SimWorkload::NativeAtomic {
            valid_payload,
            compact_after_recovery,
        } => {
            let payload = if *valid_payload {
                serde_json::to_vec(&vec![SimValue {
                    key: "counter".into(),
                    value: 7,
                }])
                .unwrap()
            } else {
                serde_json::to_vec(&vec![
                    SimValue {
                        key: "counter".into(),
                        value: 7,
                    },
                    SimValue {
                        key: "overflow".into(),
                        value: i64::MAX,
                    },
                    SimValue {
                        key: "overflow".into(),
                        value: 1,
                    },
                ])
                .unwrap()
            };
            (
                OperationId::new("atomic-op").unwrap(),
                Request::Atomic(AtomicRequest { payload }),
                ExecutionPolicy::default(),
                *compact_after_recovery,
            )
        }
        SimWorkload::NativeChunked {
            chunks,
            reject_last,
            pause_after_first,
        } => {
            let mut chunk_vec = Vec::with_capacity(*chunks);
            for idx in 0..*chunks {
                if *reject_last && idx + 1 == *chunks {
                    chunk_vec.push(b"invalid-chunk-payload".to_vec());
                } else {
                    let delta = (idx as i64) + 2;
                    chunk_vec.push(
                        serde_json::to_vec(&vec![SimValue {
                            key: "counter".into(),
                            value: delta,
                        }])
                        .unwrap(),
                    );
                }
            }
            (
                OperationId::new("chunked-op").unwrap(),
                Request::Chunked(ChunkedRequest { chunks: chunk_vec }),
                ExecutionPolicy {
                    max_chunks: if *pause_after_first { 1 } else { usize::MAX },
                    ..ExecutionPolicy::default()
                },
                false,
            )
        }
        SimWorkload::NativeStagedPublish {
            expected_generation,
            valid_payload,
        } => {
            let payload = if *valid_payload {
                serde_json::to_vec(&vec![SimValue {
                    key: "snapshot".into(),
                    value: 42,
                }])
                .unwrap()
            } else {
                b"invalid-artifact".to_vec()
            };
            (
                OperationId::new("publish-op").unwrap(),
                Request::StagedPublish(PublishRequest {
                    target: OperationId::new("visible").unwrap(),
                    expected_generation: *expected_generation,
                    payload,
                }),
                ExecutionPolicy::default(),
                false,
            )
        }
        _ => unreachable!(),
    };

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    let clock = SimClock::new(1_000);
    let controller = SimController::new(clock, None);
    for action in &schedule.actions {
        if let ScheduleAction::InjectFault { rule } = action {
            controller.add_rule(rule.clone()).unwrap();
        }
    }

    let journal = ControlledSqliteJournal {
        inner: SqliteJournal::open(path.join("journal.db")).unwrap(),
        controller: controller.clone(),
        crash_dir: None,
    };
    let backend = ControlledSqliteBackend {
        inner: SqliteBackend::open(path.join("backend.db"), "sim").unwrap(),
        controller,
    };
    if let Ok(mut engine) = Engine::open(journal, backend, policy) {
        let _ = engine
            .execute(op_id.clone(), request.clone(), &Cancellation::default())
            .await;
        if matches!(
            schedule.workload,
            SimWorkload::NativeChunked {
                pause_after_first: true,
                ..
            }
        ) {
            let _ = engine.cancel(&op_id).await;
        }
    }

    // Reopen clean SQLite journal and backend to converge.
    let mut engine = Engine::open(
        SqliteJournal::open(path.join("journal.db")).unwrap(),
        SqliteBackend::open(path.join("backend.db"), "sim").unwrap(),
        ExecutionPolicy::default(),
    )
    .unwrap();
    let _ = engine.recover(&Cancellation::default()).await.unwrap();
    let status = engine
        .execute(op_id.clone(), request, &Cancellation::default())
        .await
        .unwrap();
    if compact_after_recovery {
        engine.compact(Retention::Compact).unwrap();
    }

    assert_eq!(status.progress, sim_progress, "schedule: {schedule:?}");
    match &schedule.workload {
        SimWorkload::NativeAtomic { .. } | SimWorkload::NativeChunked { .. } => {
            assert_eq!(
                engine.backend().value("counter").unwrap(),
                sim_trace.summary.resource_values["counter"],
                "schedule: {schedule:?}"
            );
        }
        SimWorkload::NativeStagedPublish { .. } => {
            let visible_val = engine
                .backend()
                .visible(&OperationId::new("visible").unwrap())
                .unwrap()
                .and_then(|v| v.values.first().map(|item| item.value))
                .unwrap_or(0);
            assert_eq!(
                visible_val, sim_trace.summary.resource_values["visible"],
                "schedule: {schedule:?}"
            );
        }
        _ => {}
    }
    drop(engine);

    let mut raw_journal = SqliteJournal::open(path.join("journal.db")).unwrap();
    let sqlite_seq = match raw_journal.checkpoint().unwrap() {
        Some(cp) => raw_journal
            .replay()
            .unwrap()
            .last()
            .map(|e| e.sequence)
            .unwrap_or(cp.watermark),
        None => raw_journal
            .replay()
            .unwrap()
            .last()
            .map(|e| e.sequence)
            .unwrap_or(0),
    };
    assert_eq!(sqlite_seq, sim_seq, "schedule: {schedule:?}");
}

#[tokio::test]
async fn simulator_schedules_match_sqlite_and_real_child_process_crashes() {
    let explorer = StateExplorer::new(ExplorationConfig::default()).unwrap();

    // 1. Representative 2PC schedules tested against real child-process OS kill (`Child::kill`)
    // at pre-prepare, pre-decision-flush, post-decision-flush, and post-commit-effect steps.
    for crash_step in [5, 17, 21, 24] {
        let schedule = SimSchedule {
            seed: crash_step as u64,
            workload: SimWorkload::TwoPhaseCommit {
                reject_second: false,
                cancel_before_decision: false,
                compact_after_recovery: false,
            },
            actions: vec![ScheduleAction::InjectFault {
                rule: FaultRule {
                    step: crash_step,
                    component: None,
                    operation: None,
                    phase: BoundaryPhase::BeforeEffect,
                    action: FaultAction::CrashProcess,
                },
            }],
        };
        verify_2pc_schedule_matches_sqlite_and_child_process(&explorer, &schedule, true).await;
    }

    // 2. Representative 2PC schedules with lost acknowledgements, duplicate deliveries, and
    // participant rejection cross-checked against SQLite.
    let schedules_2pc = vec![
        SimSchedule {
            seed: 101,
            workload: SimWorkload::TwoPhaseCommit {
                reject_second: false,
                cancel_before_decision: false,
                compact_after_recovery: true,
            },
            actions: vec![
                ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step: 7,
                        component: Some(SimComponent::Participant),
                        operation: Some("prepare".into()),
                        phase: BoundaryPhase::BeforeEffect,
                        action: FaultAction::DuplicateDelivery {
                            extra_deliveries: 2,
                        },
                    },
                },
                ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step: 24,
                        component: Some(SimComponent::Participant),
                        operation: Some("commit".into()),
                        phase: BoundaryPhase::AfterEffect,
                        action: FaultAction::LoseAck {
                            code: ErrorCode::TransportUnknown,
                        },
                    },
                },
            ],
        },
        SimSchedule {
            seed: 102,
            workload: SimWorkload::TwoPhaseCommit {
                reject_second: true,
                cancel_before_decision: false,
                compact_after_recovery: false,
            },
            actions: vec![ScheduleAction::InjectFault {
                rule: FaultRule {
                    step: 18,
                    component: Some(SimComponent::Journal),
                    operation: Some("flush".into()),
                    phase: BoundaryPhase::AfterEffect,
                    action: FaultAction::LoseAck {
                        code: ErrorCode::DurabilityUnknown,
                    },
                },
            }],
        },
    ];
    for schedule in &schedules_2pc {
        verify_2pc_schedule_matches_sqlite_and_child_process(&explorer, schedule, false).await;
    }

    // 3. Representative Native (`Atomic`, `Chunked`, `StagedPublish`) fault schedules
    // cross-checked against `SqliteJournal` + `SqliteBackend`.
    let native_schedules = vec![
        SimSchedule {
            seed: 201,
            workload: SimWorkload::NativeAtomic {
                valid_payload: true,
                compact_after_recovery: true,
            },
            actions: vec![ScheduleAction::InjectFault {
                rule: FaultRule {
                    step: 8,
                    component: Some(SimComponent::Backend),
                    operation: Some("unit".into()),
                    phase: BoundaryPhase::AfterEffect,
                    action: FaultAction::LoseAck {
                        code: ErrorCode::AdapterUnknown,
                    },
                },
            }],
        },
        SimSchedule {
            seed: 202,
            workload: SimWorkload::NativeChunked {
                chunks: 3,
                reject_last: false,
                pause_after_first: true,
            },
            actions: vec![ScheduleAction::InjectFault {
                rule: FaultRule {
                    step: 13,
                    component: Some(SimComponent::Journal),
                    operation: Some("flush".into()),
                    phase: BoundaryPhase::BeforeEffect,
                    action: FaultAction::CrashProcess,
                },
            }],
        },
        SimSchedule {
            seed: 203,
            workload: SimWorkload::NativeStagedPublish {
                expected_generation: 0,
                valid_payload: true,
            },
            actions: vec![ScheduleAction::InjectFault {
                rule: FaultRule {
                    step: 14,
                    component: Some(SimComponent::Backend),
                    operation: Some("publish".into()),
                    phase: BoundaryPhase::AfterEffect,
                    action: FaultAction::LoseAck {
                        code: ErrorCode::AdapterUnknown,
                    },
                },
            }],
        },
    ];
    for schedule in &native_schedules {
        verify_native_schedule_matches_sqlite(&explorer, schedule).await;
    }
}
