#![cfg(r2r__lifecycle_msgs__msg__State)]
//! A ROS 2 lifecycle (managed) node.
//!
//! [`Lifecycle`] serves what rclcpp's `LifecycleNode` serves, so a lifecycle
//! manager and `ros2 lifecycle` drive a Rust node like any other: `~/change_state`, `~/get_state`, `~/get_available_states` and
//! `~/get_available_transitions` (`lifecycle_msgs`), and it publishes every
//! step on `~/transition_event`. The node runs each transition in its own
//! loop and says how it went:
//!
//! ```ignore
//! let (mut lifecycle, queries) = Lifecycle::new(&mut node)?;
//! tokio::spawn(queries);
//! loop {
//!     tokio::select! {
//!         Some(change) = lifecycle.next() => {
//!             let done = match change.transition() {
//!                 Transition::Configure => configure(),
//!                 // ...
//!             };
//!             lifecycle.finish(change, done);
//!         }
//!     }
//! }
//! ```
//!
//! rclcpp's state machine without its error processing: a transition whose
//! work fails goes back to the state it started from (a callback's FAILURE;
//! there is no ERROR), and a shutdown always ends finalized. A node starts
//! unconfigured.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{stream, Future, Stream, StreamExt};

use crate::lifecycle_msgs::msg::{
    State as StateMsg, Transition as TransitionMsg, TransitionDescription, TransitionEvent,
};
use crate::lifecycle_msgs::srv::{
    ChangeState, GetAvailableStates, GetAvailableTransitions, GetState,
};
use crate::{Node, Publisher, QosProfile, Result, ServiceRequest};

/// A primary state (`lifecycle_msgs/State` PRIMARY_STATE_*).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Unconfigured,
    Inactive,
    Active,
    Finalized,
}

impl State {
    pub const fn id(self) -> u8 {
        match self {
            Self::Unconfigured => 1,
            Self::Inactive => 2,
            Self::Active => 3,
            Self::Finalized => 4,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Unconfigured => "unconfigured",
            Self::Inactive => "inactive",
            Self::Active => "active",
            Self::Finalized => "finalized",
        }
    }
}

/// A transition a node can be asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    Configure,
    Cleanup,
    Activate,
    Deactivate,
    Shutdown,
}

impl Transition {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Configure => "configure",
            Self::Cleanup => "cleanup",
            Self::Activate => "activate",
            Self::Deactivate => "deactivate",
            Self::Shutdown => "shutdown",
        }
    }

    /// The transition state it passes through (`lifecycle_msgs/State`).
    const fn through(self) -> (u8, &'static str) {
        match self {
            Self::Configure => (10, "configuring"),
            Self::Cleanup => (11, "cleaningup"),
            Self::Shutdown => (12, "shuttingdown"),
            Self::Activate => (13, "activating"),
            Self::Deactivate => (14, "deactivating"),
        }
    }

    /// The ids of its ends (`lifecycle_msgs/Transition`
    /// TRANSITION_ON_*_SUCCESS and _FAILURE).
    const fn ends(self) -> (u8, u8) {
        match self {
            Self::Configure => (10, 11),
            Self::Cleanup => (20, 21),
            Self::Activate => (30, 31),
            Self::Deactivate => (40, 41),
            Self::Shutdown => (50, 51),
        }
    }
}

/// One transition from one state: its id there (`lifecycle_msgs/Transition`;
/// shutdown's depends on the state) and the state it ends in when it works.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Step {
    pub transition: Transition,
    pub id: u8,
    pub from: State,
    pub to: State,
}

/// The transitions `state` allows (rclcpp's state machine).
pub fn steps(state: State) -> Vec<Step> {
    use State::*;
    use Transition::*;
    let step = |transition, id, to| Step { transition, id, from: state, to };
    match state {
        Unconfigured => vec![step(Configure, 1, Inactive), step(Shutdown, 5, Finalized)],
        Inactive => vec![
            step(Cleanup, 2, Unconfigured),
            step(Activate, 3, Active),
            step(Shutdown, 6, Finalized),
        ],
        Active => vec![step(Deactivate, 4, Inactive), step(Shutdown, 7, Finalized)],
        Finalized => vec![],
    }
}

/// The step a `change_state` request asks for in `state`, by its label when
/// it has one, else by its id (rclcpp's rule: `ros2 service call` leaves the
/// id 0); none when the state doesn't allow it.
pub fn requested(state: State, id: u8, label: &str) -> Option<Step> {
    steps(state).into_iter().find(|s| {
        if label.is_empty() {
            s.id == id
        } else {
            s.transition.label() == label
        }
    })
}

/// Where a step ends: its target when the node's work for it succeeded,
/// else where it started (shutdown always finalizes).
pub fn end_state(step: Step, succeeded: bool) -> State {
    if succeeded || step.transition == Transition::Shutdown {
        step.to
    } else {
        step.from
    }
}

fn state_msg((id, label): (u8, &str)) -> StateMsg {
    StateMsg { id, label: label.to_string() }
}

fn primary(state: State) -> (u8, &'static str) {
    (state.id(), state.label())
}

/// Every state, primary and transition (`get_available_states`).
const STATES: [(u8, &str); 10] = [
    (0, "unknown"),
    (1, "unconfigured"),
    (2, "inactive"),
    (3, "active"),
    (4, "finalized"),
    (10, "configuring"),
    (11, "cleaningup"),
    (12, "shuttingdown"),
    (13, "activating"),
    (14, "deactivating"),
];

/// A transition the node was asked for and has to run; answer it with
/// [`Lifecycle::finish`].
pub struct Change {
    step: Step,
    request: ServiceRequest<ChangeState::Service>,
}

impl Change {
    pub fn transition(&self) -> Transition {
        self.step.transition
    }
}

type Changes = Box<dyn Stream<Item = ServiceRequest<ChangeState::Service>> + Unpin + Send>;

/// The node's lifecycle: its state and the services and topic above.
pub struct Lifecycle {
    state: State,
    /// What `get_state` answers: a primary state, or a transition state
    /// while one runs.
    current: Arc<Mutex<(u8, &'static str)>>,
    changes: Changes,
    events: Publisher<TransitionEvent>,
    logger: String,
}

impl Lifecycle {
    /// Creates the services and the topic on `node`. The returned future
    /// answers `get_state` and the available states and transitions; spawn
    /// it, so they are answered while the node's loop runs a transition's
    /// work.
    pub fn new(node: &mut Node) -> Result<(Self, impl Future<Output = ()> + Send + 'static)> {
        let qos = QosProfile::services_default;
        let changes = node.create_service::<ChangeState::Service>("~/change_state", qos())?;
        let get_state = node.create_service::<GetState::Service>("~/get_state", qos())?;
        let get_states =
            node.create_service::<GetAvailableStates::Service>("~/get_available_states", qos())?;
        let get_transitions = node.create_service::<GetAvailableTransitions::Service>(
            "~/get_available_transitions",
            qos(),
        )?;
        let events =
            node.create_publisher::<TransitionEvent>("~/transition_event", QosProfile::default())?;
        let current = Arc::new(Mutex::new(primary(State::Unconfigured)));
        let queries = serve_queries(current.clone(), get_state, get_states, get_transitions);
        let lifecycle = Self {
            state: State::Unconfigured,
            current,
            changes: Box::new(changes),
            events,
            logger: node.logger().to_string(),
        };
        Ok((lifecycle, queries))
    }

    pub fn state(&self) -> State {
        self.state
    }

    /// The next transition to run. A request the current state doesn't
    /// allow is refused here (`success` false). None once the service is
    /// gone. Cancel-safe: a request is only taken when it is returned or
    /// answered.
    pub async fn next(&mut self) -> Option<Change> {
        loop {
            let request = self.changes.next().await?;
            let t = &request.message.transition;
            let Some(step) = requested(self.state, t.id, &t.label) else {
                crate::log_warn!(
                    &self.logger,
                    "refused transition {} '{}' in state {}",
                    t.id,
                    t.label,
                    self.state.label()
                );
                let _ = request.respond(ChangeState::Response { success: false });
                continue;
            };
            let through = step.transition.through();
            *self.current.lock().unwrap() = through;
            self.publish(step.id, step.transition.label(), primary(step.from), through);
            return Some(Change { step, request });
        }
    }

    /// Ends `change`: in its target state when `succeeded`, else back where
    /// it started; then answers the request.
    pub fn finish(&mut self, change: Change, succeeded: bool) {
        let Change { step, request } = change;
        let end = end_state(step, succeeded);
        let (ok, failed) = step.transition.ends();
        let (id, label) =
            if succeeded { (ok, "transition_success") } else { (failed, "transition_failure") };
        self.state = end;
        *self.current.lock().unwrap() = primary(end);
        self.publish(id, label, step.transition.through(), primary(end));
        if !succeeded {
            crate::log_error!(
                &self.logger,
                "{} failed: back to {}",
                step.transition.label(),
                end.label()
            );
        }
        let _ = request.respond(ChangeState::Response { success: succeeded });
    }

    fn publish(&self, id: u8, label: &str, start: (u8, &str), goal: (u8, &str)) {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        let event = TransitionEvent {
            timestamp,
            transition: TransitionMsg { id, label: label.to_string() },
            start_state: state_msg(start),
            goal_state: state_msg(goal),
        };
        if let Err(e) = self.events.publish(&event) {
            crate::log_warn!(&self.logger, "transition event not published: {e}");
        }
    }
}

/// A request to one of the read-only lifecycle services.
enum Query {
    State(ServiceRequest<GetState::Service>),
    States(ServiceRequest<GetAvailableStates::Service>),
    Transitions(ServiceRequest<GetAvailableTransitions::Service>),
}

/// Answers the read-only lifecycle services until they are gone. Merged
/// streams rather than a runtime's `select!`, as r2r picks no runtime.
async fn serve_queries(
    current: Arc<Mutex<(u8, &'static str)>>,
    get_state: impl Stream<Item = ServiceRequest<GetState::Service>> + Unpin,
    get_states: impl Stream<Item = ServiceRequest<GetAvailableStates::Service>> + Unpin,
    get_transitions: impl Stream<Item = ServiceRequest<GetAvailableTransitions::Service>> + Unpin,
) {
    let mut queries = stream::select(
        get_state.map(Query::State),
        stream::select(get_states.map(Query::States), get_transitions.map(Query::Transitions)),
    );
    while let Some(query) = queries.next().await {
        match query {
            Query::State(request) => {
                let state = *current.lock().unwrap();
                let _ = request.respond(GetState::Response { current_state: state_msg(state) });
            }
            Query::States(request) => {
                let available_states = STATES.iter().copied().map(state_msg).collect();
                let _ = request.respond(GetAvailableStates::Response { available_states });
            }
            Query::Transitions(request) => {
                // During a transition, none (rclcpp's answer in a transition state).
                let (id, _) = *current.lock().unwrap();
                let state = [State::Unconfigured, State::Inactive, State::Active, State::Finalized]
                    .into_iter()
                    .find(|s| s.id() == id);
                let available_transitions = state
                    .map(steps)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|s| TransitionDescription {
                        transition: TransitionMsg { id: s.id, label: s.transition.label().into() },
                        start_state: state_msg(primary(s.from)),
                        goal_state: state_msg(s.transition.through()),
                    })
                    .collect();
                let _ = request
                    .respond(GetAvailableTransitions::Response { available_transitions });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle_msgs::msg::{State as S, Transition as T};

    #[test]
    fn ids_are_lifecycle_msgs() {
        assert_eq!(State::Unconfigured.id(), (S::PRIMARY_STATE_UNCONFIGURED as u8));
        assert_eq!(State::Inactive.id(), (S::PRIMARY_STATE_INACTIVE as u8));
        assert_eq!(State::Active.id(), (S::PRIMARY_STATE_ACTIVE as u8));
        assert_eq!(State::Finalized.id(), (S::PRIMARY_STATE_FINALIZED as u8));
        assert_eq!(Transition::Configure.through().0, (S::TRANSITION_STATE_CONFIGURING as u8));
        assert_eq!(Transition::Cleanup.through().0, (S::TRANSITION_STATE_CLEANINGUP as u8));
        assert_eq!(Transition::Shutdown.through().0, (S::TRANSITION_STATE_SHUTTINGDOWN as u8));
        assert_eq!(Transition::Activate.through().0, (S::TRANSITION_STATE_ACTIVATING as u8));
        assert_eq!(Transition::Deactivate.through().0, (S::TRANSITION_STATE_DEACTIVATING as u8));
        let ids: Vec<_> = [State::Unconfigured, State::Inactive, State::Active]
            .into_iter()
            .flat_map(steps)
            .map(|s| (s.transition, s.id))
            .collect();
        assert!(ids.contains(&(Transition::Configure, (T::TRANSITION_CONFIGURE as u8))));
        assert!(ids.contains(&(Transition::Cleanup, (T::TRANSITION_CLEANUP as u8))));
        assert!(ids.contains(&(Transition::Activate, (T::TRANSITION_ACTIVATE as u8))));
        assert!(ids.contains(&(Transition::Deactivate, (T::TRANSITION_DEACTIVATE as u8))));
        assert!(ids.contains(&(Transition::Shutdown, (T::TRANSITION_UNCONFIGURED_SHUTDOWN as u8))));
        assert!(ids.contains(&(Transition::Shutdown, (T::TRANSITION_INACTIVE_SHUTDOWN as u8))));
        assert!(ids.contains(&(Transition::Shutdown, (T::TRANSITION_ACTIVE_SHUTDOWN as u8))));
        assert_eq!(
            Transition::Configure.ends(),
            (
                (T::TRANSITION_ON_CONFIGURE_SUCCESS as u8),
                (T::TRANSITION_ON_CONFIGURE_FAILURE as u8)
            )
        );
        assert_eq!(
            Transition::Activate.ends(),
            ((T::TRANSITION_ON_ACTIVATE_SUCCESS as u8), (T::TRANSITION_ON_ACTIVATE_FAILURE as u8))
        );
        assert_eq!(
            Transition::Deactivate.ends(),
            (
                (T::TRANSITION_ON_DEACTIVATE_SUCCESS as u8),
                (T::TRANSITION_ON_DEACTIVATE_FAILURE as u8)
            )
        );
        assert_eq!(
            Transition::Cleanup.ends(),
            ((T::TRANSITION_ON_CLEANUP_SUCCESS as u8), (T::TRANSITION_ON_CLEANUP_FAILURE as u8))
        );
        assert_eq!(
            Transition::Shutdown.ends(),
            ((T::TRANSITION_ON_SHUTDOWN_SUCCESS as u8), (T::TRANSITION_ON_SHUTDOWN_FAILURE as u8))
        );
    }

    #[test]
    fn the_manager_s_path_up_and_down() {
        let configure = requested(State::Unconfigured, 1, "").unwrap();
        assert_eq!(end_state(configure, true), State::Inactive);
        let activate = requested(State::Inactive, 3, "").unwrap();
        assert_eq!(end_state(activate, true), State::Active);
        let deactivate = requested(State::Active, 4, "").unwrap();
        assert_eq!(end_state(deactivate, true), State::Inactive);
        let cleanup = requested(State::Inactive, 2, "").unwrap();
        assert_eq!(end_state(cleanup, true), State::Unconfigured);
    }

    #[test]
    fn a_failed_transition_goes_back_and_shutdown_always_finalizes() {
        let configure = requested(State::Unconfigured, 1, "").unwrap();
        assert_eq!(end_state(configure, false), State::Unconfigured);
        let activate = requested(State::Inactive, 3, "").unwrap();
        assert_eq!(end_state(activate, false), State::Inactive);
        let shutdown = requested(State::Active, 7, "").unwrap();
        assert_eq!(end_state(shutdown, false), State::Finalized);
    }

    #[test]
    fn the_label_wins_over_the_id() {
        // `ros2 service call .../change_state "{transition: {label: shutdown}}"`.
        let shutdown = requested(State::Inactive, 0, "shutdown").unwrap();
        assert_eq!((shutdown.transition, shutdown.id), (Transition::Shutdown, 6));
        assert_eq!(requested(State::Inactive, 1, "activate").unwrap().id, 3);
    }

    #[test]
    fn a_state_refuses_what_it_does_not_allow() {
        assert_eq!(requested(State::Unconfigured, 3, ""), None);
        assert_eq!(requested(State::Active, 1, ""), None);
        assert_eq!(requested(State::Inactive, 7, ""), None);
        assert_eq!(requested(State::Inactive, 0, "configure"), None);
        assert!(steps(State::Finalized).is_empty());
    }
}
