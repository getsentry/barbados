use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_channel::{Receiver, Sender, TrySendError};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub struct Queue {
    workers: Vec<JoinHandle<()>>,
    sender: Sender<BoxFuture>,
    tasks_running: Arc<AtomicUsize>,
}

#[non_exhaustive]
#[derive(Debug)]
pub struct Stats {
    pub queue_capacity: Option<usize>,
    pub queue_len: usize,
    pub workers: usize,
    pub active_workers: usize,
}

impl Queue {
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

    pub async fn close(self) {
        let Self {
            workers, sender, ..
        } = self;
        drop(sender);
        for worker in workers {
            // NOTE:
            // `JoinError` has two variants internally:
            // 1) a panic 2) cancellation.
            // We never cancel the worker tasks ourselves, though they might
            // have panic-ed.
            worker.await.unwrap();
        }
    }

    pub fn stats(&self) -> Stats {
        let active_workers = self.tasks_running.load(Ordering::Relaxed);
        Stats {
            queue_capacity: self.sender.capacity(),
            queue_len: self.sender.len(),
            workers: self.workers.len(),
            active_workers,
        }
    }

    pub fn enqueue<F, T>(&self, future: F) -> Result<oneshot::Receiver<T>, TrySendError<()>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (send, receiver) = oneshot::channel();
        match self.sender.try_send(Box::pin(async move {
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

            future.await;

            tasks_running.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
