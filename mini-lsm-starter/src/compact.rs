// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = self.state.read().clone();

        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut iters = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                for sstable in l0_sstables
                    .iter()
                    .chain(l1_sstables.iter())
                    .map(|sst_id| state.sstables[sst_id].clone())
                {
                    iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        sstable,
                    )?));
                }

                let mut merge_iterator = MergeIterator::create(iters);
                let mut sstable_builder = Some(SsTableBuilder::new(self.options.block_size));
                let mut compact_sstables = Vec::new();

                while merge_iterator.is_valid() {
                    if sstable_builder.is_none() {
                        sstable_builder = Some(SsTableBuilder::new(self.options.block_size));
                    }

                    let builder = sstable_builder.as_mut().unwrap();
                    if !merge_iterator.value().is_empty() {
                        builder.add(merge_iterator.key(), merge_iterator.value());
                    }

                    if builder.estimated_size() >= self.options.target_sst_size {
                        let builder = sstable_builder.take().unwrap();
                        let sst_id = self.next_sst_id();
                        let sstable = builder.build(
                            sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(sst_id),
                        )?;

                        compact_sstables.push(Arc::new(sstable));
                    }

                    merge_iterator.next()?;
                }

                if let Some(builder) = sstable_builder {
                    let sst_id = self.next_sst_id();
                    let sstable = builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?;

                    compact_sstables.push(Arc::new(sstable));
                }

                Ok(compact_sstables)
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => {
                if let Some(upper_level) = upper_level {
                    let upper_level_sstables = upper_level_sst_ids
                        .iter()
                        .map(|sst_id| state.sstables[sst_id].clone())
                        .collect();

                    let upper_level_iters =
                        SstConcatIterator::create_and_seek_to_first(upper_level_sstables)?;

                    let lower_level_sstables = lower_level_sst_ids
                        .iter()
                        .map(|sst_id| state.sstables[sst_id].clone())
                        .collect();

                    let lower_level_iters =
                        SstConcatIterator::create_and_seek_to_first(lower_level_sstables)?;

                    let mut sstable_iters =
                        TwoMergeIterator::create(upper_level_iters, lower_level_iters)?;

                    let mut sstable_builder = Some(SsTableBuilder::new(self.options.block_size));
                    let mut compact_sstables = Vec::new();

                    while sstable_iters.is_valid() {
                        if sstable_builder.is_none() {
                            sstable_builder = Some(SsTableBuilder::new(self.options.block_size));
                        }

                        let builder = sstable_builder.as_mut().unwrap();
                        if !sstable_iters.value().is_empty() {
                            builder.add(sstable_iters.key(), sstable_iters.value());
                        }

                        if builder.estimated_size() >= self.options.target_sst_size {
                            let builder = sstable_builder.take().unwrap();
                            let sst_id = self.next_sst_id();
                            let sstable = builder.build(
                                sst_id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(sst_id),
                            )?;

                            compact_sstables.push(Arc::new(sstable));
                        }

                        sstable_iters.next()?;
                    }

                    if let Some(builder) = sstable_builder {
                        let sst_id = self.next_sst_id();
                        let sstable = builder.build(
                            sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(sst_id),
                        )?;

                        compact_sstables.push(Arc::new(sstable));
                    }

                    Ok(compact_sstables)
                } else {
                    let mut upper_sst_iters_vec = Vec::with_capacity(upper_level_sst_ids.len());
                    for sstable in upper_level_sst_ids
                        .iter()
                        .map(|sst_id| state.sstables[sst_id].clone())
                    {
                        upper_sst_iters_vec.push(Box::new(
                            SsTableIterator::create_and_seek_to_first(sstable)?,
                        ));
                    }

                    let upper_level_iters = MergeIterator::create(upper_sst_iters_vec);

                    let lower_level_sstables = lower_level_sst_ids
                        .iter()
                        .map(|sst_id| state.sstables[sst_id].clone())
                        .collect();

                    let lower_level_iters =
                        SstConcatIterator::create_and_seek_to_first(lower_level_sstables)?;

                    let mut sstable_iters =
                        TwoMergeIterator::create(upper_level_iters, lower_level_iters)?;

                    let mut sstable_builder = Some(SsTableBuilder::new(self.options.block_size));
                    let mut compact_sstables = Vec::new();

                    while sstable_iters.is_valid() {
                        if sstable_builder.is_none() {
                            sstable_builder = Some(SsTableBuilder::new(self.options.block_size));
                        }

                        let builder = sstable_builder.as_mut().unwrap();
                        if !sstable_iters.value().is_empty() {
                            builder.add(sstable_iters.key(), sstable_iters.value());
                        }

                        if builder.estimated_size() >= self.options.target_sst_size {
                            let builder = sstable_builder.take().unwrap();
                            let sst_id = self.next_sst_id();
                            let sstable = builder.build(
                                sst_id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(sst_id),
                            )?;

                            compact_sstables.push(Arc::new(sstable));
                        }

                        sstable_iters.next()?;
                    }

                    if let Some(builder) = sstable_builder {
                        let sst_id = self.next_sst_id();
                        let sstable = builder.build(
                            sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(sst_id),
                        )?;

                        compact_sstables.push(Arc::new(sstable));
                    }

                    Ok(compact_sstables)
                }
            }

            _ => unimplemented!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let state = self.state.read().clone();

        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: state.l0_sstables.clone(),
            l1_sstables: state.levels[0].1.clone(),
        };
        let compaction_sstables = self.compact(&compaction_task)?;

        let state_guard = self.state_lock.lock();
        let mut new_state = (**self.state.read()).clone();

        let (old_l0_sstables, old_l1_sstables) = {
            match &compaction_task {
                CompactionTask::ForceFullCompaction {
                    l0_sstables,
                    l1_sstables,
                } => (l0_sstables, l1_sstables),
                _ => {
                    // SAFETY: compaction_task is initialized to ForceFullCompaction and remains unchanged
                    unsafe { std::hint::unreachable_unchecked() }
                }
            }
        };

        for remove_sst_id in old_l0_sstables.iter().chain(old_l1_sstables.iter()) {
            new_state.sstables.remove(remove_sst_id);
        }

        for insert_sstable in &compaction_sstables {
            new_state
                .sstables
                .insert(insert_sstable.sst_id(), insert_sstable.clone());
        }

        new_state
            .l0_sstables
            .retain(|sst_id| !old_l0_sstables.contains(sst_id));

        let _ = std::mem::replace(
            &mut new_state.levels[0].1,
            compaction_sstables
                .iter()
                .map(|sstable| sstable.sst_id())
                .collect(),
        );

        *self.state.write() = Arc::new(new_state);

        drop(state_guard);

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let state = self.state.read().clone();

        let compaction_task = self.compaction_controller.generate_compaction_task(&state);

        if let Some(compaction_task) = compaction_task {
            let compact_sstables = self.compact(&compaction_task)?;
            let compact_sst_ids: Vec<_> = compact_sstables
                .iter()
                .map(|sstable| sstable.sst_id())
                .collect();

            let state_guard = self.state_lock.lock();
            let state = self.state.read().clone();

            let (mut new_state, remove_sst_ids) = self
                .compaction_controller
                .apply_compaction_result(&state, &compaction_task, &compact_sst_ids, false);

            for remove_sst_id in &remove_sst_ids {
                new_state.sstables.remove(remove_sst_id);
            }

            for add_sstalbe in compact_sstables {
                new_state.sstables.insert(add_sstalbe.sst_id(), add_sstalbe);
            }

            *self.state.write() = Arc::new(new_state);
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let state = self.state.read().clone();

        if state.imm_memtables.len() + 1 > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
