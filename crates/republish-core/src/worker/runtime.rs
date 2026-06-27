//! Panic-safe async runtime wrapper for worker threads.

use crossbeam_channel::Sender;

use crate::log::LogLevel;

use super::events::{log, WorkerEvent};

/// Runs the worker future to completion, returning false if the runtime could not
/// start or the future panicked.
pub fn run_async<F>(sender: Sender<WorkerEvent>, future: F) -> bool
where
    F: std::future::Future<Output = ()>,
{
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            log(
                &sender,
                LogLevel::Error,
                format!("Failed to start async runtime: {error:#}"),
            );
            return false;
        }
    };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.block_on(future))) {
        Ok(()) => true,
        Err(panic) => {
            log(
                &sender,
                LogLevel::Error,
                format!("Worker thread crashed: {}", panic_message(panic.as_ref())),
            );
            let _ = sender.send(WorkerEvent::Finished(
                "Worker stopped unexpectedly".to_string(),
            ));
            false
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    #[test]
    fn run_async_reports_panics_instead_of_dying_silently() {
        let (sender, receiver) = unbounded();
        let completed = run_async(sender.clone(), async {
            panic!("boom");
        });
        assert!(!completed);
        let event = receiver.try_recv().unwrap();
        match event {
            WorkerEvent::Log(LogLevel::Error, message) => {
                assert!(message.contains("boom"));
            }
            other => panic!("expected error log, got {other:?}"),
        }
        match receiver.try_recv().unwrap() {
            WorkerEvent::Finished(message) => assert!(message.contains("unexpectedly")),
            other => panic!("expected Finished, got {other:?}"),
        }
    }
}
