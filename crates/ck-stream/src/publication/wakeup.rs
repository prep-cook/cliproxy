use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Notify, futures::Notified};

/// A `Notify` whose wake-up skips the waiter list while no task waits.
///
/// A waiter takes `waiting` before it rechecks stream state under the stream lock and holds
/// it until it stops waiting. A waker changes that state under the same lock and calls `wake`
/// after the change. If the recheck locked first, the lock orders the waiter's count before
/// the waker's load; otherwise the recheck already sees the change. No wake-up is lost.
pub(super) struct Wakeup {
    notify: Notify,
    waiting: AtomicUsize,
}

impl Wakeup {
    pub(super) fn new() -> Self {
        Self {
            notify: Notify::new(),
            waiting: AtomicUsize::new(0),
        }
    }

    pub(super) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    pub(super) fn waiting(&self) -> Waiting<'_> {
        self.waiting.fetch_add(1, Ordering::Relaxed);
        Waiting(&self.waiting)
    }

    pub(super) fn wake(&self) {
        if self.waiting.load(Ordering::Relaxed) != 0 {
            self.notify.notify_waiters();
        }
    }
}

pub(super) struct Waiting<'a>(&'a AtomicUsize);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
