// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{ast::Exp, refinement::Refine};

pub fn refine(exp: &mut Exp) -> bool {
    FlattenSeq.refine(exp)
}

// -------------------------------------------------------------------------------------------------
// Refinement

struct FlattenSeq;

impl Refine for FlattenSeq {
    fn refine_custom(&mut self, exp: &mut Exp) -> bool {
        let Exp::IfElse(_test, _conseq, alt) = exp else {
            return false;
        };

        let Some(alt_ref) = alt.as_mut() else {
            return false;
        };
        let Exp::Seq(alt_seq) = alt_ref else {
            return false;
        };
        let [] = &alt_seq[..] else {
            return false;
        };
        let _ = std::mem::replace(alt, Box::new(None));
        true
    }
}
