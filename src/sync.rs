//! Locks and waits that survive a panic on another thread: the state they guard stays consistent
//! between statements, so a poisoned lock is taken as it is.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn wait<'a, T>(condition: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condition
        .wait(guard)
        .unwrap_or_else(PoisonError::into_inner)
}

/// Waits on `condition` until woken or until `deadline`; `None` waits until woken.
pub(crate) fn wait_until<'a, T>(
    condition: &Condvar,
    guard: MutexGuard<'a, T>,
    deadline: Option<Instant>,
) -> MutexGuard<'a, T> {
    match deadline {
        None => wait(condition, guard),
        Some(until) => {
            let left = until.saturating_duration_since(Instant::now());
            condition
                .wait_timeout(guard, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0
        }
    }
}
