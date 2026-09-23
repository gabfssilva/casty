//! How the binding takes a lock.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// A lock taken as it is, poisoned or not.
///
/// A lock is poisoned only by a panic that unwound while it was held. The release build aborts on a panic, so there no
/// lock ever is. A build that unwinds turns the panic into a `PanicException` in its caller and the node goes on: later
/// calls, and on free-threaded Python other threads, take the same locks. Refusing them from then on would turn one
/// failure into every later call failing, so the lock is taken with whatever the panic left in it. No critical section
/// of the binding runs code that panics on any input.
pub trait Locked<T> {
    fn locked(&self) -> MutexGuard<'_, T>;
}

impl<T> Locked<T> for Mutex<T> {
    fn locked(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
