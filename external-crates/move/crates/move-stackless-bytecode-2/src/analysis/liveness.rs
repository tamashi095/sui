// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    analysis::absint::{AbstractDomain, AbstractInterpreter},
    ast::{Instruction, LocalId, RegId, Register, Trivial},
};

use move_abstract_interpreter::absint::{JoinResult, analyze_function};

use std::collections::{BTreeMap, BTreeSet};

// -------------------------------------------------------------------------------------------------
// Entry Point
// -------------------------------------------------------------------------------------------------

fn analysize(function: &crate::ast::Function) -> BTreeMap<usize, LiveSet> {
    let mut domain = Domain::new();
    let mut interpreter = AbstractInterpreter::new(domain);

    let cfg = function.to_vm_control_flow_graph();
    let reverse = cfg.to_reverse_control_flow_graph();
    let initial_state = LiveSet::default();

    let inv_map = analyze_function(&mut interpreter, reverse, &(), initial_state).unwrap();

    interpreter.domain.live
}

// -------------------------------------------------------------------------------------------------
// Types
// -------------------------------------------------------------------------------------------------

#[derive(Default, Clone)]
pub struct LiveSet(BTreeSet<Identifier>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Identifier {
    Register(RegId),
    Local(LocalId),
}

/// Compute liveness information for a function from stackless bytecode.
#[derive(Clone)]
pub struct Domain {
    // For each instruction, the set of live variables at the exit for it.
    live: BTreeMap<usize, LiveSet>,
}

// -------------------------------------------------------------------------------------------------
// Impl Blocks
// -------------------------------------------------------------------------------------------------

impl Domain {
    fn new() -> Self {
        Self {
            live: BTreeMap::new(),
        }
    }

    fn add_lives(&mut self, instr_index: usize, lives: &LiveSet) {
        self.live.entry(instr_index).or_default().extend(lives);
    }
}

impl LiveSet {
    fn insert_register(&mut self, reg: &Register) {
        self.0.insert(Identifier::Register(reg.name));
    }

    fn insert_local(&mut self, local: &LocalId) {
        self.0.insert(Identifier::Local(*local));
    }

    fn remove_register(&mut self, reg: &Register) {
        self.0.remove(&Identifier::Register(reg.name));
    }

    fn remove_local(&mut self, local: &LocalId) {
        self.0.remove(&Identifier::Local(*local));
    }

    fn extend(&mut self, other: &LiveSet) {
        self.0.extend(other.0.iter().cloned());
    }

    fn join(&self, other: &LiveSet) -> (Self, JoinResult) {
        let before = self.0.len();
        let mut result = self.clone();
        result.extend(other);
        let changed = if result.0.len() != before {
            JoinResult::Changed
        } else {
            JoinResult::Unchanged
        };
        (result, changed)
    }
}

// -------------------------------------------------------------------------------------------------
// Abs Int Traits
// -------------------------------------------------------------------------------------------------

impl AbstractDomain for Domain {
    type Error = ();

    type State = LiveSet;

    fn join(&mut self, state: &Self::State, other: &Self::State) -> (Self::State, JoinResult) {
        let (state, result) = state.join(other);
        (state, result)
    }

    fn step(
        &mut self,
        state: &Self::State,
        _block_id: crate::ast::Label,
        bounds: (usize, usize),
        offset: usize,
        instr: &Instruction,
    ) -> Result<Self::State, Self::Error> {
        let instr_index = bounds.0 + offset;
        let new_state = instruction(self, instr_index, instr, state.clone());
        self.add_lives(instr_index, &new_state);
        Ok(new_state)
    }
}

/// Return the set of live variables at the entry of the instruction.
/// Args:
/// - state: The current state of the analysis.
/// - instr_index: The index of the instruction being processed.
/// - instruction: The instruction being processed.
/// - live: The set of live variables at the exit of the instruction.
pub fn instruction(
    _domain: &mut Domain,
    _instr_index: usize,
    instruction: &Instruction,
    lives: LiveSet,
) -> LiveSet {
    let mut lives = lives;

    match instruction {
        Instruction::Return(trivials) => {
            for triv in trivials {
                match triv {
                    Trivial::Register(reg) => {
                        lives.insert_register(reg);
                    }
                    Trivial::Immediate(_) => (),
                }
            }
        }
        Instruction::AssignReg { lhs, rhs } => {
            for reg in lhs {
                lives.remove_register(reg);
            }
            rvalue(&mut lives, rhs);
        }
        Instruction::StoreLoc { loc, value } => {
            lives.remove_local(loc);
            trivial(&mut lives, value);
        }
        Instruction::Jump(_) => (),
        Instruction::JumpIf {
            condition,
            then_label: _,
            else_label: _,
        } => {
            trivial(&mut lives, condition);
        }
        Instruction::Abort(triv) => {
            trivial(&mut lives, triv);
        }
        Instruction::Nop => todo!(),
        Instruction::VariantSwitch {
            condition,
            enum_: _,
            variants: _,
            labels: _,
        } => {
            trivial(&mut lives, condition);
        }
        Instruction::Drop(register) => {
            lives.remove_register(register);
        }
        Instruction::NotImplemented(_) => (),
    }

    lives
}

fn rvalue(lives: &mut LiveSet, rhs: &crate::ast::RValue) {
    match rhs {
        crate::ast::RValue::Call { target: _, args }
        | crate::ast::RValue::Primitive { op: _, args }
        | crate::ast::RValue::Data { op: _, args } => {
            for arg in args {
                trivial(lives, arg);
            }
        }
        crate::ast::RValue::Local { op: _, arg } => {
            lives.insert_local(arg);
        }
        crate::ast::RValue::Trivial(triv) => trivial(lives, triv),
        crate::ast::RValue::Constant(_) => (),
    }
}

fn trivial(lives: &mut LiveSet, triv: &Trivial) {
    match triv {
        Trivial::Register(reg) => {
            lives.insert_register(reg);
        }
        Trivial::Immediate(_) => (),
    }
}
