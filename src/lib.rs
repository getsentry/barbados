use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::{JoinError, JoinHandle};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub struct Queue {
    runtime: Handle,
    enqueue: mpsc::Sender<BoxFuture>,
    semaphore: Arc<Semaphore>,
    scheduler: JoinHandle<()>,
    queue_capacity: usize,
    concurrency: usize,
    tasks_running: Arc<AtomicUsize>,
}

#[non_exhaustive]
#[derive(Debug)]
pub struct Stats {
    pub queue_free: usize,
    pub queue_used: usize,
    pub tasks_free: usize,
    pub tasks_used: usize,
}

impl Queue {
    pub fn new(runtime: Handle, queue_capacity: usize, concurrency: usize) -> Self {
        let (enqueue, receive) = mpsc::channel(queue_capacity);
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let tasks_running = Arc::new(AtomicUsize::new(0));

        let scheduler = runtime.spawn(Self::scheduler(
            runtime.clone(),
            Arc::clone(&semaphore),
            Arc::clone(&tasks_running),
            receive,
        ));

        Self {
            runtime,
            enqueue,
            semaphore,
            scheduler,
            queue_capacity,
            concurrency,
            tasks_running,
        }
    }

    pub async fn close(self) -> Result<(), JoinError> {
        let Self {
            runtime: _runtime,
            enqueue,
            semaphore: _semaphore,
            scheduler,
            ..
        } = self;
        drop(enqueue);
        scheduler.await
    }

    pub fn stats(&self) -> Stats {
        let queue_free = self.enqueue.capacity();
        let tasks_used = self.tasks_running.load(Ordering::Relaxed);
        Stats {
            queue_free,
            queue_used: self.queue_capacity - queue_free,
            tasks_free: self.concurrency - tasks_used,
            tasks_used,
        }
    }

    pub fn enqueue<F, T>(
        &self,
        future: F,
    ) -> Result<oneshot::Receiver<T>, mpsc::error::TrySendError<()>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let permit = self.enqueue.try_reserve()?;
        let (send, receiver) = oneshot::channel();
        permit.send(Box::pin(async move {
            send.send(future.await).ok();
        }));
        Ok(receiver)
    }

    async fn scheduler(
        runtime: Handle,
        semaphore: Arc<Semaphore>,
        tasks_running: Arc<AtomicUsize>,
        mut receive: mpsc::Receiver<BoxFuture>,
    ) {
        loop {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            };

            let future = match receive.recv().await {
                Some(future) => future,
                None => return,
            };

            runtime.spawn(async move {
                future.await;
                drop(permit)
            });
        }
    }
}
