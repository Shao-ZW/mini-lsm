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

use std::{ops::Bound, sync::Arc};

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = if let Some(sstable) = sstables.first() {
            Some(SsTableIterator::create_and_seek_to_first(sstable.clone())?)
        } else {
            None
        };

        Ok(Self {
            current,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        for (sst_idx, sstable) in sstables.iter().enumerate() {
            if sstable.range_overlap(Bound::Included(key.raw_ref()), Bound::Unbounded) {
                let current = SsTableIterator::create_and_seek_to_key(sstable.clone(), key)?;
                return Ok(Self {
                    current: Some(current),
                    next_sst_idx: sst_idx + 1,
                    sstables,
                });
            }
        }

        Ok(Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .expect("Should set current sstable iterator")
            .key()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("Should set current sstable iterator")
            .value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        let current = self
            .current
            .as_mut()
            .expect("Should set current sstable iterator");
        current.next()?;

        if !current.is_valid() {
            if let Some(sstable) = self.sstables.get(self.next_sst_idx) {
                self.current = Some(SsTableIterator::create_and_seek_to_first(sstable.clone())?);
                self.next_sst_idx += 1;
            } else {
                self.current = None;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
