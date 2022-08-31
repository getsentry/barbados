# Barbados

[![Build Status](https://github.com/getsentry/barbados/workflows/CI/badge.svg)](https://github.com/getsentry/barbados/actions?workflow=CI)
<a href="https://crates.io/crates/barbados"><img src="https://img.shields.io/crates/v/barbados.svg" alt=""></a>
<a href="https://github.com/getsentry/barbados/blob/master/LICENSE"><img src="https://img.shields.io/crates/l/barbados.svg" alt=""></a>
[![codecov](https://codecov.io/gh/getsentry/barbados/branch/master/graph/badge.svg?token=5K5HAK3RIQ)](https://codecov.io/gh/getsentry/barbados)

A (bounded) queue with a fixed number of async worker tasks.

## Example

```rust
// the workers are spawned on an explicit runtime
let runtime = tokio::runtime::Handle::current();
let queue = Queue::new(runtime, 5, Some(5));

// fire and forget:
let _ = queue.enqueue(async { 1 });

// when you are still interested in the result:
match queue.enqueue(async { 2 }) {
    Ok(receiver) => println!("Got {}", receiver.await);
    Err(err) => eprintln!("Enqueue failed: {}", err);
}

// calling `close` waits for all pending tasks to finish processing
queue.close().await;
```

## Why Barbados?

The [Barbados Threadsnake](https://en.wikipedia.org/wiki/Barbados_threadsnake) is
a tiny snake. And the german word for _snake_ and _queue_ is both _Schlange_.

## License

Barbados is licensed under the MIT license.
