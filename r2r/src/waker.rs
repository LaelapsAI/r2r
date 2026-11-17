// Not in upstream r2r 0.9.7: see the README's "Vendored copy" note.

use futures::stream::{Stream, StreamExt};
use std::{
    cell::UnsafeCell,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

use r2r_rcl::*;

use crate::{context::Context, error::*};

/// Wakes a node's `spin_once` from any thread.
///
/// It is a guard condition in every wait set the node waits on, as
/// rclcpp's executors use to interrupt a wait. A wake while the node isn't
/// waiting makes its next `spin_once` return at once, so none is lost.
#[derive(Clone)]
pub struct NodeWaker(Arc<GuardCondition>);

struct GuardCondition {
    handle: UnsafeCell<rcl_guard_condition_t>,
    // rcl needs the context to outlive the guard condition.
    context: Context,
}

// SAFETY: rcl allows triggering a guard condition from any thread, also
// concurrently with itself; init and fini run while no other reference
// exists.
unsafe impl Send for GuardCondition {}
unsafe impl Sync for GuardCondition {}

impl NodeWaker {
    pub(crate) fn new(context: &Context) -> Result<Self> {
        let mut handle = unsafe { rcl_get_zero_initialized_guard_condition() };
        let ret = {
            let mut ctx = context.context_handle.lock().unwrap();
            unsafe {
                rcl_guard_condition_init(
                    &mut handle,
                    ctx.as_mut(),
                    rcl_guard_condition_get_default_options(),
                )
            }
        };
        if ret != RCL_RET_OK as i32 {
            return Err(Error::from_rcl_error(ret));
        }
        Ok(Self(Arc::new(GuardCondition {
            handle: UnsafeCell::new(handle),
            context: context.clone(),
        })))
    }

    /// Makes the node's current `spin_once` return, or its next one if it
    /// isn't spinning.
    pub fn wake(&self) -> Result<()> {
        let ret = unsafe { rcl_trigger_guard_condition(self.0.handle.get()) };
        if ret == RCL_RET_OK as i32 {
            Ok(())
        } else {
            Err(Error::from_rcl_error(ret))
        }
    }

    pub(crate) fn handle(&self) -> *const rcl_guard_condition_t {
        self.0.handle.get()
    }
}

impl Drop for GuardCondition {
    fn drop(&mut self) {
        // fini functions are not thread safe, so lock the context, as
        // Node's drop does.
        let _ctx = self.context.context_handle.lock().unwrap();
        unsafe {
            rcl_guard_condition_fini(self.handle.get_mut());
        }
    }
}

/// A subscription's stream that wakes its node when dropped, so the node's
/// next `spin_once`, at once, destroys the subscription.
pub(crate) struct SubscriptionStream<S> {
    stream: Option<S>,
    waker: NodeWaker,
}

impl<S> SubscriptionStream<S> {
    pub(crate) fn new(stream: S, waker: NodeWaker) -> Self {
        Self { stream: Some(stream), waker }
    }
}

impl<S: Stream + Unpin> Stream for SubscriptionStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<S::Item>> {
        match self.stream.as_mut() {
            Some(stream) => stream.poll_next_unpin(cx),
            None => Poll::Ready(None),
        }
    }
}

impl<S> Drop for SubscriptionStream<S> {
    fn drop(&mut self) {
        // The receiver first, so the woken spin finds its channel closed.
        drop(self.stream.take());
        let _ = self.waker.wake();
    }
}
