// Copyright (c) The Diem Core Contributors
// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

//! This module defines the control-flow graph uses for bytecode verification.
use std::collections::{BTreeMap, BTreeSet, VecDeque, btree_map::Entry};

// -------------------------------------------------------------------------------------------------
// Traits
// -------------------------------------------------------------------------------------------------

/// A trait that specifies the basic requirements for a CFG
pub trait ControlFlowGraph {
    type BlockId: Copy + Ord;
    type InstructionIndex: Copy + Ord;
    type Instruction;
    type Instructions: ?Sized;

    /// Start index of the block ID in the bytecode vector
    fn block_start(&self, block_id: Self::BlockId) -> Self::InstructionIndex;

    /// End index of the block ID in the bytecode vector
    fn block_end(&self, block_id: Self::BlockId) -> Self::InstructionIndex;

    /// Successors of the block ID in the bytecode vector
    fn successors(&self, block_id: Self::BlockId) -> impl Iterator<Item = Self::BlockId>;

    /// Return the next block in traversal order
    fn next_block(&self, block_id: Self::BlockId) -> Option<Self::BlockId>;

    /// Iterator over the indexes of instructions in this block
    fn instructions<'a>(
        &'a self,
        function_code: &'a Self::Instructions,
        block_id: Self::BlockId,
    ) -> impl Iterator<Item = (Self::InstructionIndex, &'a Self::Instruction)>
    where
        Self::Instruction: 'a;

    /// Return an iterator over the blocks of the CFG
    fn blocks(&self) -> impl Iterator<Item = Self::BlockId>;

    /// Return the number of blocks (vertices) in the control flow graph
    fn num_blocks(&self) -> usize;

    /// Return the id of the entry block for this control-flow graph
    /// Note: even a CFG with no instructions has an (empty) entry block.
    fn entry_block_id(&self) -> Self::BlockId;

    /// Checks if the block ID is a loop head
    fn is_loop_head(&self, block_id: Self::BlockId) -> bool;

    /// Checks if the edge from cur->next is a back edge
    /// returns false if the edge is not in the cfg
    fn is_back_edge(&self, cur: Self::BlockId, next: Self::BlockId) -> bool;
}

/// Used for the VM control flow graph
pub trait Instruction: Sized {
    type Index: Copy + Ord;
    type VariantJumpTables: ?Sized;
    const ENTRY_BLOCK_ID: Self::Index;

    /// Return the successors of a given instruction
    fn get_successors(
        pc: Self::Index,
        code: &[Self],
        jump_tables: &Self::VariantJumpTables,
    ) -> Vec<Self::Index>;

    /// Return the offsets of jump targets for a given instruction
    fn offsets(&self, jump_tables: &Self::VariantJumpTables) -> Vec<Self::Index>;

    fn usize_as_index(i: usize) -> Self::Index;
    fn index_as_usize(i: Self::Index) -> usize;

    /// Returns trie if the instruction is branching (e.g., a jump or exit)
    fn is_branch(&self) -> bool;

    /// Returns true if the instruction is exiting (e.g, abort or return)
    fn is_exit(&self) -> bool;
}

pub trait BlockInfo<I: Instruction> {
    fn successors(&self, index: &I::Index) -> impl Iterator<Item = I::Index>;
    fn len(&self) -> usize;
}

// -------------------------------------------------------------------------------------------------
// Types
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct BasicBlock<InstructionIndex> {
    exit: InstructionIndex,
    successors: Vec<InstructionIndex>,
}

/// The control flow graph that we build from the bytecode.
/// Assumes a list of bytecode isntructions that satisfy the invariants specified in the VM's
/// file format.
#[derive(Debug, Clone)]
pub struct VMControlFlowGraph<I: Instruction> {
    /// The basic blocks
    blocks: BTreeMap<I::Index, BasicBlock<I::Index>>,
    /// Basic block ordering for traversal
    traversal_successors: BTreeMap<I::Index, I::Index>,
    /// Map of loop heads with all of their back edges
    loop_heads: BTreeMap<I::Index, /* back edges */ BTreeSet<I::Index>>,
}

#[allow(dead_code)]
pub struct ReverseCFG<I: Instruction> {
    pub terminal: I::Index,
    terminal_block: BasicBlock<I::Index>,
    blocks: BTreeMap<I::Index, BasicBlock<I::Index>>,
    pub successor_map: BTreeMap<I::Index, BTreeSet<I::Index>>,
    pub predecessor_map: BTreeMap<I::Index, BTreeSet<I::Index>>,
    pub traversal_order: Vec<I::Index>,
    traversal_successors: BTreeMap<I::Index, I::Index>,
    // Map from an ID to it loop head(s), if they are defined.
    back_loop_heads: BTreeMap<I::Index, BTreeSet<I::Index>>,
}

// -------------------------------------------------------------------------------------------------
// Display
// -------------------------------------------------------------------------------------------------

impl<InstructionIndex: std::fmt::Display + std::fmt::Debug> BasicBlock<InstructionIndex> {
    pub fn display(&self, entry: InstructionIndex) {
        println!("+=======================+");
        println!("| Enter:  {}            |", entry);
        println!("+-----------------------+");
        println!("==> Children: {:?}", self.successors);
        println!("+-----------------------+");
        println!("| Exit:   {}            |", self.exit);
        println!("+=======================+");
    }
}

// -------------------------------------------------------------------------------------------------
// Impls
// -------------------------------------------------------------------------------------------------

impl<I: Instruction> VMControlFlowGraph<I> {
    pub fn new(code: &[I], jump_tables: &I::VariantJumpTables) -> Self {
        use std::collections::BTreeMap as Map;
        let code_len = code.len();
        // First go through and collect block ids, i.e., offsets that begin basic blocks.
        // Need to do this first in order to handle backwards edges.
        let mut block_ids = BTreeSet::new();
        block_ids.insert(I::ENTRY_BLOCK_ID);
        for pc in 0..code.len() {
            VMControlFlowGraph::record_block_ids(pc, code, jump_tables, &mut block_ids);
        }

        // Create basic blocks
        let mut blocks: BTreeMap<I::Index, BasicBlock<I::Index>> = Map::new();
        let mut entry = 0;
        let mut exit_to_entry = Map::new();
        for pc in 0..code.len() {
            let co_pc = I::usize_as_index(pc);

            // Create a basic block
            if Self::is_end_of_block(pc, code, &block_ids) {
                let exit = co_pc;
                exit_to_entry.insert(exit, entry);
                let successors = I::get_successors(co_pc, code, jump_tables);
                let bb = BasicBlock { exit, successors };
                blocks.insert(I::usize_as_index(entry), bb);
                entry = pc + 1;
            }
        }
        let blocks = blocks;
        assert_eq!(entry, code_len);

        let (loop_heads, mut post_order_traversal) = loop_heads_and_post_order_traversal::<
            I,
            BTreeMap<I::Index, BasicBlock<I::Index>>,
        >(I::ENTRY_BLOCK_ID, &blocks);

        let traversal_order = {
            // This reverse post order is akin to a topological sort (ignoring cycles) and is
            // different from a pre-order in the presence of diamond patterns in the graph.
            post_order_traversal.reverse();
            post_order_traversal
        };
        // build a mapping from a block id to the next block id in the traversal order
        let traversal_successors = traversal_order
            .windows(2)
            .map(|window| {
                debug_assert!(window.len() == 2);
                (window[0], window[1])
            })
            .collect();

        VMControlFlowGraph {
            blocks,
            traversal_successors,
            loop_heads,
        }
    }

    pub fn display(&self)
    where
        I::Index: std::fmt::Debug + std::fmt::Display,
    {
        for (entry, block) in &self.blocks {
            block.display(*entry);
        }
        println!("Traversal: {:#?}", self.traversal_successors);
    }

    fn is_end_of_block(pc: usize, code: &[I], block_ids: &BTreeSet<I::Index>) -> bool {
        pc + 1 == code.len() || block_ids.contains(&I::usize_as_index(pc + 1))
    }

    fn record_block_ids(
        pc: usize,
        code: &[I],
        jump_tables: &I::VariantJumpTables,
        block_ids: &mut BTreeSet<I::Index>,
    ) {
        let bytecode = &code[pc];

        block_ids.extend(bytecode.offsets(jump_tables));

        if bytecode.is_branch() && pc + 1 < code.len() {
            block_ids.insert(I::usize_as_index(pc + 1));
        }
    }

    /// A utility function that implements BFS-reachability from block_id with
    /// respect to get_targets function
    fn traverse_by(&self, block_id: I::Index) -> Vec<I::Index> {
        let mut ret = Vec::new();
        // We use this index to keep track of our frontier.
        let mut index = 0;
        // Guard against cycles
        let mut seen = BTreeSet::new();

        ret.push(block_id);
        seen.insert(block_id);

        while index < ret.len() {
            let block_id = ret[index];
            index += 1;
            for block_id in self.successors(block_id) {
                if !seen.contains(&block_id) {
                    ret.push(block_id);
                    seen.insert(block_id);
                }
            }
        }

        ret
    }

    pub fn reachable_from(&self, block_id: I::Index) -> Vec<I::Index> {
        self.traverse_by(block_id)
    }

    pub fn num_back_edges(&self) -> usize {
        self.loop_heads
            .iter()
            .fold(0, |acc, (_, edges)| acc + edges.len())
    }

    pub fn predecessor_map(&self) -> BTreeMap<I::Index, BTreeSet<I::Index>> {
        let mut preds = self
            .blocks
            .keys()
            .map(|lbl| (*lbl, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        for (block, block_info) in &self.blocks {
            for &succ in &block_info.successors {
                preds.get_mut(&succ).unwrap().insert(*block);
            }
        }
        preds
    }

    pub fn determine_infinite_loop_starts(&self) -> BTreeSet<I::Index> {
        fn loop_has_exit<I: Instruction>(
            head: I::Index,
            loop_heads: &BTreeMap<I::Index, BTreeSet<I::Index>>, // Head -> {back-edge sources}
            blocks: &BTreeMap<I::Index, BasicBlock<I::Index>>,
            preds: &BTreeMap<I::Index, BTreeSet<I::Index>>,
        ) -> bool {
            // 1) Build the natural loop nodes for `head`.
            let mut loop_nodes: BTreeSet<I::Index> = BTreeSet::new();
            loop_nodes.insert(head);

            // Work queue is initial latch nodes
            let mut work: VecDeque<I::Index> = loop_heads
                .get(&head)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default();

            // Find all non-exiting nodes in the loop
            while let Some(n) = work.pop_back() {
                if !loop_nodes.insert(n) {
                    continue; // already in the loop
                }
                // If any predecessor to this node is not the loop head, it must be in the loop,
                // since we know these are the latch nodes for our loop head.
                if let Some(ps) = preds.get(&n) {
                    for &p in ps {
                        if p != head {
                            work.push_back(p);
                        }
                    }
                }
            }

            // 2) Any edge leaving the loop, or an empty-successor block, means the loop has an exit.
            for &n in &loop_nodes {
                if let Some(block) = blocks.get(&n) {
                    if block.successors.is_empty() {
                        return true; // explicit exit
                    }
                    for &succ in &block.successors {
                        if !loop_nodes.contains(&succ) {
                            return true; // edge to outside
                        }
                    }
                }
            }
            false // closed loop (no exits)
        }

        let preds = self.predecessor_map();
        let infinite_loop_heads: std::collections::BTreeSet<I::Index> = self
            .loop_heads
            .keys()
            .copied()
            .filter(|&h| !loop_has_exit::<I>(h, &self.loop_heads, &self.blocks, &preds))
            .collect();

        infinite_loop_heads
    }

    pub fn to_reverse_control_flow_graph<'this>(&self, instructions: &[I]) -> ReverseCFG<I> {
        ReverseCFG::new(self, instructions)
    }
}

impl<I: Instruction> ReverseCFG<I> {
    pub fn new(forward_cfg: &VMControlFlowGraph<I>, instructions: &[I]) -> Self {

        let mut blocks = forward_cfg.blocks.clone();

        let mut forward_successors = forward_cfg
            .blocks
            .keys()
            .map(|lbl| (*lbl, forward_cfg.successors(*lbl).collect()))
            .collect::<BTreeMap<_, Vec<_>>>();
        let mut forward_predecessor_map = forward_cfg.predecessor_map();

        let infinite_loop_heads = forward_cfg.determine_infinite_loop_starts();
        println!("Infinite loop heads: {:#?}", infinite_loop_heads.iter().map(|ndx| I::index_as_usize(*ndx)).collect::<Vec<_>>());

        // Set up a fake terminal block that will act as the "start" node
        let terminal = I::usize_as_index(instructions.len());
        let terminal_block = BasicBlock {
            exit: terminal,
            successors: vec![],
        };
        blocks.insert(terminal, terminal_block.clone());

        assert!(
            !forward_cfg.blocks.contains_key(&terminal),
            "Invalid terminal"
        );

        let end_blocks = {
            let mut end_blocks = BTreeSet::new();
            for (lbl, successors) in forward_successors.iter() {
                let loop_start_successors = successors
                    .iter()
                    .filter(|l| infinite_loop_heads.contains(l));
                for loop_start_successor in loop_start_successors {
                    if lbl >= loop_start_successor {
                        end_blocks.insert(*lbl);
                    }
                }
            }
            for (lbl, block) in forward_cfg.blocks.iter() {
                let last_cmd = &instructions[I::index_as_usize(block.exit)];
                if last_cmd.is_exit() {
                    end_blocks.insert(*lbl);
                }
            }
            end_blocks
        };

        for end_block in &end_blocks {
            forward_successors
                .entry(*end_block)
                .or_default()
                .push(terminal);
        }

        forward_successors.insert(terminal, vec![]);
        forward_predecessor_map.insert(terminal, end_blocks);

        let (forward_loop_heads, post_order) =
            loop_heads_and_post_order_traversal::<I, BTreeMap<I::Index, Vec<I::Index>>>(
                forward_cfg.entry_block_id(),
                &forward_successors,
            );
        let successor_map = forward_predecessor_map;
        let predecessor_map = forward_successors
            .into_iter()
            .map(|(lbl, vec)| (lbl, vec.into_iter().collect()))
            .collect();
        let traversal_order = post_order;
        let traversal_successors = traversal_order
            .windows(2)
            .map(|window| (window[0], window[1]))
            .collect();

        let mut back_loop_heads: BTreeMap<I::Index, BTreeSet<I::Index>> = BTreeMap::new();
        for (head, sources) in &forward_loop_heads {
            for &src in sources {
                back_loop_heads.entry(src).or_default().insert(*head);
            }
        }

        Self {
            terminal,
            terminal_block,
            blocks,
            successor_map,
            predecessor_map,
            traversal_order,
            traversal_successors,
            back_loop_heads,
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Utility Functions
// -------------------------------------------------------------------------------------------------

fn loop_heads_and_post_order_traversal<I: Instruction, B>(
    start: I::Index,
    block_info: &B,
) -> (BTreeMap<I::Index, BTreeSet<I::Index>>, Vec<I::Index>)
where
    B: BlockInfo<I>,
{
    use std::collections::{BTreeMap as Map, BTreeSet as Set};

    #[derive(Copy, Clone)]
    enum Exploration {
        InProgress,
        Done,
    }

    let mut exploration: Map<I::Index, Exploration> = Map::new();
    let mut stack = vec![start];

    let mut loop_heads: Map<I::Index, Set<I::Index>> = Map::new();

    let mut post_order = Vec::with_capacity(block_info.len());

    while let Some(block) = stack.pop() {
        match exploration.entry(block) {
            Entry::Vacant(entry) => {
                // Record the fact that exploration of this block and its sub-graph has started.
                entry.insert(Exploration::InProgress);

                // Push the block back on the stack to finish processing it, and mark it as done
                // once its sub-graph has been traversed.
                stack.push(block);

                for succ in block_info.successors(&block) {
                    match exploration.get(&succ) {
                        // This successor has never been visited before, add it to the stack to
                        // be explored before `block` gets marked `Done`.
                        None => stack.push(succ),

                        // This block's sub-graph was being explored, meaning it is a (reflexive
                        // transitive) predecessor of `block` as well as being a successor,
                        // implying a loop has been detected -- greedily choose the successor
                        // block as the loop head.
                        Some(Exploration::InProgress) => {
                            loop_heads.entry(succ).or_default().insert(block);
                        }

                        // Cross-edge detected, this block and its entire sub-graph (modulo
                        // cycles) has already been explored via a different path, and is
                        // already present in `post_order`.
                        Some(Exploration::Done) => { /* skip */ }
                    };
                }
            }

            Entry::Occupied(mut entry) => match entry.get() {
                // Already traversed the sub-graph reachable from this block, so skip it.
                Exploration::Done => continue,

                // Finish up the traversal by adding this block to the post-order traversal
                // after its sub-graph (modulo cycles).
                Exploration::InProgress => {
                    post_order.push(block);
                    entry.insert(Exploration::Done);
                }
            },
        }
    }

    (loop_heads, post_order)
}

// -------------------------------------------------------------------------------------------------
// ControlFlowGraph Impls
// -------------------------------------------------------------------------------------------------

impl<I: Instruction> ControlFlowGraph for VMControlFlowGraph<I> {
    type BlockId = I::Index;
    type InstructionIndex = I::Index;
    type Instruction = I;
    type Instructions = [I];

    // Note: in the following procedures, it's safe not to check bounds because:
    // - Every CFG (even one with no instructions) has a block at ENTRY_BLOCK_ID
    // - The only way to acquire new BlockId's is via block_successors()
    // - block_successors only() returns valid BlockId's
    // Note: it is still possible to get a BlockId from one CFG and use it in another CFG where it
    // is not valid. The design does not attempt to prevent this abuse of the API.

    fn block_start(&self, block_id: Self::BlockId) -> I::Index {
        block_id
    }

    fn block_end(&self, block_id: Self::BlockId) -> I::Index {
        self.blocks[&block_id].exit
    }

    fn successors(&self, block_id: Self::BlockId) -> impl Iterator<Item = Self::BlockId> {
        self.blocks[&block_id].successors.iter().copied()
    }

    fn next_block(&self, block_id: Self::BlockId) -> Option<I::Index> {
        debug_assert!(self.blocks.contains_key(&block_id));
        self.traversal_successors.get(&block_id).copied()
    }

    fn instructions<'a>(
        &self,
        function_code: &'a [I],
        block_id: Self::BlockId,
    ) -> impl Iterator<Item = (Self::BlockId, &'a I)>
    where
        I: 'a,
    {
        let start = I::index_as_usize(self.block_start(block_id));
        let end = I::index_as_usize(self.block_end(block_id));
        (start..=end).map(|pc| (I::usize_as_index(pc), &function_code[pc]))
    }

    fn blocks(&self) -> impl Iterator<Item = Self::BlockId> {
        self.blocks.keys().copied()
    }

    fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    fn entry_block_id(&self) -> Self::BlockId {
        I::ENTRY_BLOCK_ID
    }

    fn is_loop_head(&self, block_id: Self::BlockId) -> bool {
        self.loop_heads.contains_key(&block_id)
    }

    fn is_back_edge(&self, cur: Self::BlockId, next: Self::BlockId) -> bool {
        self.loop_heads
            .get(&next)
            .is_some_and(|back_edges| back_edges.contains(&cur))
    }
}

impl<I: Instruction> ControlFlowGraph for ReverseCFG<I> {
    type BlockId = I::Index;
    type InstructionIndex = I::Index;
    type Instruction = I;
    type Instructions = [I];

    fn block_start(&self, block_id: Self::BlockId) -> Self::InstructionIndex {
        block_id
    }

    fn block_end(&self, block_id: Self::BlockId) -> Self::InstructionIndex {
        self.blocks[&block_id].exit
    }

    fn successors(&self, block_id: Self::BlockId) -> impl Iterator<Item = Self::BlockId> {
        self.successor_map.get(&block_id).unwrap().iter().copied()
    }

    fn next_block(&self, block_id: Self::BlockId) -> Option<Self::BlockId> {
        self.traversal_successors.get(&block_id).copied()
    }

    fn instructions<'a>(
        &'a self,
        function_code: &'a Self::Instructions,
        block_id: Self::BlockId,
    ) -> impl Iterator<Item = (Self::InstructionIndex, &'a Self::Instruction)>
    where
        Self::Instruction: 'a,
    {
        let start = I::index_as_usize(self.block_start(block_id));
        let end = I::index_as_usize(self.block_end(block_id));
        (start..=end).map(|pc| (I::usize_as_index(pc), &function_code[pc]))
    }

    fn blocks(&self) -> impl Iterator<Item = Self::BlockId> {
        self.blocks.keys().copied()
    }

    fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    fn entry_block_id(&self) -> Self::BlockId {
        // Hand back the terminal since it is the "entry"
        self.terminal
    }

    /// Indicates if it is a "loop end"
    fn is_loop_head(&self, block_id: Self::BlockId) -> bool {
        self.back_loop_heads.contains_key(&block_id)
    }

    /// Indicates if it points to a "loop head"
    fn is_back_edge(&self, cur: Self::BlockId, next: Self::BlockId) -> bool {
        self.back_loop_heads
            .get(&next)
            .is_some_and(|back_edges| back_edges.contains(&cur))
    }
}

// -------------------------------------------------------------------------------------------------
// BlockInfo Impls
// -------------------------------------------------------------------------------------------------

impl<I> BlockInfo<I> for BTreeMap<I::Index, BasicBlock<I::Index>>
where
    I: Instruction,
{
    fn successors(&self, index: &I::Index) -> impl Iterator<Item = I::Index> {
        self.get(&index).unwrap().successors.iter().map(|ndx| *ndx)
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl<I> BlockInfo<I> for BTreeMap<I::Index, Vec<I::Index>>
where
    I: Instruction,
{
    fn successors(&self, index: &I::Index) -> impl Iterator<Item = I::Index> {
        self.get(&index).unwrap().iter().map(|ndx| *ndx)
    }
    fn len(&self) -> usize {
        self.len()
    }
}
