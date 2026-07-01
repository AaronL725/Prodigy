#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionCommand {
    PlaceMaker { attempt: u32 },
    CancelCurrent,
    PlaceTaker,
    MarkIntentExecuted,
    Wait,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPolicy {
    pub max_maker_attempts_before_taker: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentExecution {
    pub intent_id: String,
    pub action: String,
    pub maker_attempts: u32,
    pub has_live_order: bool,
    pub needs_cancel_confirmation: bool,
    pub filled: bool,
    pub taker_sent: bool,
}

impl IntentExecution {
    pub fn new(intent_id: impl Into<String>, action: impl Into<String>) -> Self {
        Self {
            intent_id: intent_id.into(),
            action: action.into(),
            maker_attempts: 0,
            has_live_order: false,
            needs_cancel_confirmation: false,
            filled: false,
            taker_sent: false,
        }
    }

    pub fn next_command(&self, policy: &ExecutionPolicy) -> ExecutionCommand {
        if self.filled {
            return ExecutionCommand::MarkIntentExecuted;
        }
        if self.needs_cancel_confirmation {
            return ExecutionCommand::CancelCurrent;
        }
        if self.has_live_order {
            return ExecutionCommand::Wait;
        }
        if self.maker_attempts < policy.max_maker_attempts_before_taker {
            return ExecutionCommand::PlaceMaker {
                attempt: self.maker_attempts + 1,
            };
        }
        if !self.taker_sent {
            return ExecutionCommand::PlaceTaker;
        }
        ExecutionCommand::Wait
    }

    pub fn on_order_placed(&mut self, _client_oid: &str) {
        self.has_live_order = true;
        self.needs_cancel_confirmation = false;
    }

    pub fn on_taker_sent(&mut self) {
        self.taker_sent = true;
        self.has_live_order = true;
    }

    // ponytail: set unconditionally (not guarded by has_live_order). The verbatim
    // test drives the maker cycle as command→timeout→cancel without an explicit
    // placement ack, so the timeout must request a cancel regardless. The actor
    // only calls this after issuing PlaceMaker, so there is always something to cancel.
    pub fn on_order_timeout(&mut self) {
        self.needs_cancel_confirmation = true;
    }

    // ponytail: book the maker attempt here (not in on_order_placed). A maker
    // attempt is consumed once the order is retired (cancelled); this avoids
    // double-counting whether or not the actor acks placement, and matches the
    // command→cancel cycle the test models. on_order_filled is terminal, so a
    // filled maker never needs counting.
    pub fn on_order_cancelled(&mut self) {
        self.has_live_order = false;
        self.needs_cancel_confirmation = false;
        self.maker_attempts += 1;
    }

    pub fn on_order_filled(&mut self) {
        self.filled = true;
        self.has_live_order = false;
        self.needs_cancel_confirmation = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_order_retries_maker_once_then_taker() {
        let policy = ExecutionPolicy {
            max_maker_attempts_before_taker: 2,
        };
        let mut state = IntentExecution::new("intent-1", "open");

        assert_eq!(
            state.next_command(&policy),
            ExecutionCommand::PlaceMaker { attempt: 1 }
        );
        state.on_order_timeout();
        assert_eq!(state.next_command(&policy), ExecutionCommand::CancelCurrent);
        state.on_order_cancelled();
        assert_eq!(
            state.next_command(&policy),
            ExecutionCommand::PlaceMaker { attempt: 2 }
        );
        state.on_order_timeout();
        assert_eq!(state.next_command(&policy), ExecutionCommand::CancelCurrent);
        state.on_order_cancelled();
        assert_eq!(state.next_command(&policy), ExecutionCommand::PlaceTaker);
    }

    #[test]
    fn filled_order_marks_execution_done() {
        let policy = ExecutionPolicy {
            max_maker_attempts_before_taker: 2,
        };
        let mut state = IntentExecution::new("intent-1", "open");

        state.on_order_placed("client-1");
        state.on_order_filled();

        assert_eq!(
            state.next_command(&policy),
            ExecutionCommand::MarkIntentExecuted
        );
    }
}
