/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
extern crate alloc;
use alloc::vec::Vec;

use crate::{member::Member, ProbeNumber};

pub(crate) struct Probe<T> {
    direct: Option<Member<T>>,
    indirect: Vec<T>,
    probe_number: ProbeNumber,

    direct_ack_ok: bool,
    indirect_ack_count: usize,

    reached_indirect_probe_stage: bool,
}

impl<T: Clone + PartialEq> Probe<T> {
    pub(crate) fn new(indirect: Vec<T>) -> Self {
        Self {
            indirect,
            direct: None,
            direct_ack_ok: false,
            indirect_ack_count: 0,
            reached_indirect_probe_stage: false,
            probe_number: ProbeNumber::default(),
        }
    }

    #[must_use]
    pub(crate) fn start(&mut self, target: Member<T>) -> ProbeNumber {
        self.clear();
        self.direct = Some(target);
        self.probe_number = self.probe_number.wrapping_add(1);
        self.probe_number
    }

    pub(crate) const fn probe_number(&self) -> ProbeNumber {
        self.probe_number
    }

    pub(crate) fn clear(&mut self) {
        self.direct = None;
        self.indirect.clear();
        self.direct_ack_ok = false;
        self.indirect_ack_count = 0;
        self.reached_indirect_probe_stage = false;
        // do NOT reset probe_number
    }

    pub(crate) fn mark_indirect_probe_stage_reached(&mut self) {
        self.reached_indirect_probe_stage = true;
    }

    pub(crate) const fn validate(&self) -> bool {
        // A probe that hasn't been started is
        // valid
        self.direct.is_none()
            // Otherwise it's only valid if the indirect
            // probing stage has been reached
            || self.reached_indirect_probe_stage
    }

    pub(crate) fn take_failed(&mut self) -> Option<Member<T>> {
        if !self.succeeded() {
            self.direct.take()
        } else {
            None
        }
    }

    #[cfg(any(feature = "tracing", test))]
    pub(crate) fn target(&self) -> Option<&T> {
        self.direct.as_ref().map(|probed| probed.id())
    }

    pub(crate) fn is_probing(&self, id: &T) -> bool {
        self.direct.as_ref().is_some_and(|probed| probed.id() == id)
    }

    pub(crate) const fn succeeded(&self) -> bool {
        self.direct_ack_ok || self.indirect_ack_count > 0
    }

    pub(crate) fn receive_ack(&mut self, from: &T, probeno: ProbeNumber) -> bool {
        if probeno == self.probe_number
            && self
                .direct
                .as_ref()
                .is_some_and(|direct| direct.id() == from)
        {
            self.direct_ack_ok = true;
            true
        } else {
            false
        }
    }

    pub(crate) fn expect_indirect_ack(&mut self, from: T) {
        debug_assert!(self
            .direct
            .as_ref()
            .is_some_and(|probed| probed.id() != &from));
        self.indirect.push(from);
    }

    pub(crate) fn receive_indirect_ack(&mut self, from: &T, probeno: ProbeNumber) -> bool {
        if self.probe_number != probeno {
            return false;
        }

        if let Some(position) = self.indirect.iter().position(|id| id == from) {
            self.indirect_ack_count += 1;
            // Ensure we can't double count the same candidate
            self.indirect.swap_remove(position);
            true
        } else {
            false
        }
    }
}
