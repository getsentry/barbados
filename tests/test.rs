use std::time::Duration;

use barbados::Queue;
use tokio::time::sleep;

#[tokio::test]
async fn test_basic() {
    let runtime = tokio::runtime::Handle::current();
    let queue = Queue::new(runtime, 5, Some(5));

    let fut1 = queue.enqueue(async move { 1 });
    assert!(fut1.is_ok());

    let fut2 = queue.enqueue(async move { "str" });
    assert!(fut2.is_ok());

    let (res1, res2) = tokio::join!(fut1.unwrap(), fut2.unwrap());
    assert_eq!(res1, Ok(1));
    assert_eq!(res2, Ok("str"));
}

#[tokio::test]
async fn test_bounds() {
    let runtime = tokio::runtime::Handle::current();
    let queue = Queue::new(runtime, 2, Some(2));

    for i in 0..4 {
        let fut = queue.enqueue(async move {
            println!("task {i} started");
            sleep(Duration::from_millis(100)).await;
            println!("task {i} finished");
        });

        // give the scheduler a chance to spawn tasks:
        tokio::task::yield_now().await;

        assert!(fut.is_ok());
    }

    let fut = queue.enqueue(async move {});
    assert!(fut.is_err());

    let stats = queue.stats();
    assert_eq!(stats.queue_capacity, Some(stats.queue_len));
    assert_eq!(stats.active_workers, 2);

    sleep(Duration::from_millis(200)).await;

    let stats = queue.stats();
    assert_eq!(stats.queue_len, 0);
    assert_eq!(stats.active_workers, 2);

    sleep(Duration::from_millis(300)).await;

    let stats = queue.stats();
    assert_eq!(stats.queue_len, 0);
    assert_eq!(stats.active_workers, 0);
}
