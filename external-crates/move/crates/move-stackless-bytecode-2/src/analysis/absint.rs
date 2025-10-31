// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use move_abstract_interpreter::absint::{self as absint, JoinResult};

use crate::ast::{Instruction, Label};

pub(crate) trait AbstractDomain: Clone {
    type Error;
    type State: Clone;

    fn join(&mut self, state: &Self::State, other: &Self::State) -> (Self::State, JoinResult);
    fn step(
        &mut self,
        state: &Self::State,
        block_id: Label,
        bounds: (usize, usize),
        offset: usize,
        instr: &Instruction,
    ) -> Result<Self::State, Self::Error>;
}

pub(crate) struct AbstractInterpreter<Domain: AbstractDomain> {
    pub(crate) domain: Domain,
}

impl<Domain> AbstractInterpreter<Domain>
where
    Domain: AbstractDomain,
{
    pub(crate) fn new(domain: Domain) -> Self {
        Self { domain }
    }
}

impl<Domain> absint::AbstractInterpreter for AbstractInterpreter<Domain>
where
    Domain: AbstractDomain,
{
    type State = Domain::State;
    type Instruction = Instruction;

    type Error = Domain::Error;

    type BlockId = Label;

    type InstructionIndex = usize;

    fn join(
        &mut self,
        pre: &mut Self::State,
        post: &Self::State,
    ) -> Result<move_abstract_interpreter::absint::JoinResult, Self::Error> {
        let (new_state, result) = self.domain.join(pre, post);
        *pre = new_state;
        Ok(result)
    }

    fn execute(
        &mut self,
        block_id: Self::BlockId,
        bounds: (Self::InstructionIndex, Self::InstructionIndex),
        state: &mut Self::State,
        offset: Self::InstructionIndex,
        instr: &Self::Instruction,
    ) -> Result<(), Self::Error> {
        let new_state = self.domain.step(state, block_id, bounds, offset, instr)?;
        *state = new_state;
        Ok(())
    }
}
