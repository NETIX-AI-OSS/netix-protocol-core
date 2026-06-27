//! Worker event types and channel wrapper.

use crossbeam_channel::{unbounded, Receiver, Sender};

use crate::import::MergeImportResult;
use crate::log::LogLevel;
use crate::model::{DiscoverOutcome, DiscoveredPoint, PointFailure, PointIdentity, PointSample, PublishStats};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepublisherLifecycle {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed(String),
}

/// Messages emitted by worker threads, drained by the UI.
#[derive(Debug)]
pub enum WorkerEvent {
    Log(LogLevel, String),
    Devices(DiscoverOutcome),
    Points(Vec<DiscoveredPoint>),
    ScanProgress {
        device_key: String,
        current: usize,
        total: usize,
    },
    BulkTagImport(MergeImportResult),
    Samples(Vec<PointSample>),
    Failures(Vec<PointFailure>),
    PublishStatus(PublishStats),
    PointPublish {
        identity: PointIdentity,
        error: Option<String>,
    },
    Lifecycle(RepublisherLifecycle),
    Finished(String),
}

/// A bidirectional channel pair the UI holds.
pub struct WorkerChannel {
    pub sender: Sender<WorkerEvent>,
    pub receiver: Receiver<WorkerEvent>,
}

impl Default for WorkerChannel {
    fn default() -> Self {
        let (sender, receiver) = unbounded();
        Self { sender, receiver }
    }
}

impl WorkerChannel {
    pub fn new() -> Self {
        Self::default()
    }
}

pub(crate) fn log(sender: &Sender<WorkerEvent>, level: LogLevel, message: impl Into<String>) {
    let _ = sender.send(WorkerEvent::Log(level, message.into()));
}
