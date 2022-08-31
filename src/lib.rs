use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_channel::{Receiver, Sender};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use async_channel::TrySendError;
pub use oneshot::Receiver as QueueReceiver;

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A task queue with a bounded number of workers.
pub struct Queue {
    workers: Vec<JoinHandle<()>>,
    sender: Sender<BoxFuture>,
    tasks_running: Arc<AtomicUsize>,
}

/// Statistics about the current queue state.
#[non_exhaustive]
#[derive(Debug)]
pub struct Stats {
    /// This is the total queue capacity that it was configured with.
    pub queue_capacity: Option<usize>,
    /// The current length of the queue.
    pub queue_len: usize,
    /// The number of workers the queue was configured with.
    pub workers: usize,
    /// The number currently active workers processing tasks.
    pub active_workers: usize,
}

impl Queue {
    /// Creates a new Task Queue.
    ///
    /// This immediately spawns `workers` onto `runtime`, and optionally allows
    /// specifying a bound for its task queue.
    pub fn new(runtime: Handle, workers: usize, queue_capacity: Option<usize>) -> Self {
        let tasks_running = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = match queue_capacity {
            Some(bound) => async_channel::bounded(bound),
            None => async_channel::unbounded(),
        };

        let workers = (0..workers)
            .map(|_| runtime.spawn(Self::worker(Arc::clone(&tasks_running), receiver.clone())))
            .collect();

        Self {
            sender,
            workers,
            tasks_running,
        }
    }

    /// Consumes and closes the task queue.
    ///
    /// This waits for all the remaining tasks to be processed.
    pub async fn close(self) {
        let Self {
            workers, sender, ..
        } = self;
        sender.close();
        // dropping the sender would also implicitly close it
        drop(sender);
        for worker in workers {
            // NOTE:
            // `JoinError` has two variants internally:
            // 1) a panic 2) cancellation.
            // We never cancel the worker tasks ourselves.
            // But the tasks themselves could have panicked.
            worker.await.unwrap();
        }
    }

    /// Returns statistics about the current state of the task queue.
    pub fn stats(&self) -> Stats {
        let active_workers = self.tasks_running.load(Ordering::Relaxed);
        Stats {
            queue_capacity: self.sender.capacity(),
            queue_len: self.sender.len(),
            workers: self.workers.len(),
            active_workers,
        }
    }

    /// Enqueue a new `future` onto this queue.
    ///
    /// Returns either a [`QueueReceiver`] if a task was successfully
    /// enqueued, or a [`TrySendError`] if the queue is full.
    pub fn enqueue<F, T>(&self, future: F) -> Result<QueueReceiver<T>, TrySendError<()>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (send, receiver) = oneshot::channel();
        match self.sender.try_send(Box::pin(async move {
            // XXX: `oneshot::Sender` is not `UnwindSafe` itself.
            send.send(future.await).ok();
        })) {
            Ok(_) => Ok(receiver),
            Err(TrySendError::Full(_)) => Err(TrySendError::Full(())),
            Err(TrySendError::Closed(_)) => Err(TrySendError::Closed(())),
        }
    }

    async fn worker(tasks_running: Arc<AtomicUsize>, receiver: Receiver<BoxFuture>) {
        loop {
            let future = match receiver.recv().await {
                Ok(future) => future,
                Err(_) => return,
            };

            tasks_running.fetch_add(1, Ordering::Relaxed);

            // TODO: once our `BoxFuture` are `UnwindSafe`, we could use the
            // `futures_util::future::CatchUnwind` utility to catch panics in
            // this worker.
            future.await;

            tasks_running.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
