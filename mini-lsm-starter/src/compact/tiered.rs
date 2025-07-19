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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let all_level_size: usize = snapshot
            .levels
            .iter()
            .map(|(tier_id, sst_ids)| sst_ids.len())
            .sum();

        let last_level_size = snapshot
            .levels
            .last()
            .expect("levels len must >= num_tiers now")
            .1
            .len();

        if (all_level_size - last_level_size) as f64 / last_level_size as f64
            >= self.options.max_size_amplification_percent as f64 * 0.01
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        let mut sum_previous_level_size = 0;
        for (idx, (tier_id, sst_ids)) in snapshot.levels.iter().enumerate() {
            if idx == 0 || idx < self.options.min_merge_width {
                continue;
            }

            if sst_ids.len() as f64 > (100 + self.options.size_ratio) as f64 * 0.01 {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..idx].to_vec(),
                    bottom_tier_included: false,
                });
            }

            sum_previous_level_size += sst_ids.len();
        }

        let idx = self
            .options
            .max_merge_width
            .unwrap_or(usize::MAX)
            .min(snapshot.levels.len());

        Some(TieredCompactionTask {
            tiers: snapshot.levels[..idx].to_vec(),
            bottom_tier_included: idx == snapshot.levels.len() - 1,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut state = snapshot.clone();
        let mut delete_sst_ids = Vec::new();

        let idx = snapshot
            .levels
            .iter()
            .position(|(tier_id, _)| *tier_id == task.tiers[0].0)
            .expect("Should find match tier_id in task");
        let drain_levels: Vec<_> = state.levels.drain(idx..idx + task.tiers.len()).collect();

        assert_eq!(drain_levels, task.tiers);
        state.levels.insert(idx, (output[0], output.to_vec()));
        output.to_vec();

        delete_sst_ids.extend(task.tiers.iter().flat_map(|(_, sst_ids)| sst_ids.iter()));

        (state, delete_sst_ids)
    }
}
