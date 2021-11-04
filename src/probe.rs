/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
extern crate alloc;
use alloc::vec::Vec;

use crate::{member::Member, ProbeNumber};

// FIXME This whole thing is ugly AF :(

pub struct Probe<T> {
    direct: Option<Member<T>>,
    indirect: Vec<T>,
    probe_number: ProbeNumber,

    direct_ack_ok: bool,
    indirect_ack_count: usize,

    reached_indirect_probe_stage: bool,
}

impl<T: Clone + PartialEq> Probe<T> {
    pub fn new(indirect: Vec<T>) -> Self {
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
    pub fn start(&mut self, target: Member<T>) -> ProbeNumber {
        self.clear();
        self.direct = Some(target);
        self.probe_number = self.probe_number.wrapping_add(1);
        self.probe_number
    }

    pub(crate) fn probe_number(&self) -> ProbeNumber {
        self.probe_number
    }

    pub fn clear(&mut self) {
        self.direct = None;
        self.indirect.clear();
        self.direct_ack_ok = false;
        self.indirect_ack_count = 0;
        self.reached_indirect_probe_stage = false;
        // do NOT reset probe_number
    }

    pub fn mark_indirect_probe_stage_reached(&mut self) {
        self.reached_indirect_probe_stage = true;
    }

    pub fn validate(&self) -> bool {
        // A probe that hasn't been started is
        // valid
        self.direct.is_none()
            // Otherwise it's only valid if the indirect
            // probing stage has been reached
            || self.reached_indirect_probe_stage
    }

    pub fn take_failed(&mut self) -> Option<Member<T>> {
        if !self.succeeded() {
            self.direct.take()
        } else {
            None
        }
    }

    pub fn target(&self) -> Option<&T> {
        self.direct.as_ref().map(|probed| probed.id())
    }

    pub fn is_probing(&self, id: &T) -> bool {
        self.direct
            .as_ref()
            .map(|probed| probed.id() == id)
            .unwrap_or(false)
    }

    pub fn succeeded(&self) -> bool {
        self.direct_ack_ok || self.indirect_ack_count > 0
    }

    pub fn receive_ack(&mut self, from: &T, probeno: ProbeNumber) -> bool {
        if probeno == self.probe_number
            && self
                .direct
                .as_ref()
                .map(|direct| direct.id() == from)
                .unwrap_or(false)
        {
            self.direct_ack_ok = true;
            true
        } else {
            false
        }
    }

    pub fn expect_indirect_ack(&mut self, from: T) {
        debug_assert!(self
            .direct
            .as_ref()
            .map(|probed| probed.id() != &from)
            .unwrap_or(false));
        self.indirect.push(from);
    }

    pub fn receive_indirect_ack(&mut self, from: &T, probeno: ProbeNumber) -> bool {
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
