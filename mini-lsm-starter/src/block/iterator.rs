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

use std::sync::Arc;

use bytes::Buf;

use crate::{
    block::{KEY_LEN, VALUE_LEN},
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let first_key = block.get_first_key();
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iter = Self::new(block);
        block_iter.seek_to_first();
        block_iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block_iter = Self::new(block);
        block_iter.seek_to_key(key);
        block_iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        assert!(self.is_valid());
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_by_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_by_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (mut lbound, mut rbound) = (0, self.block.offsets.len());
        loop {
            let idx = (lbound + rbound) / 2;
            self.seek_by_idx(idx);

            if self.key.as_key_slice() < key {
                lbound = idx + 1;
            } else {
                rbound = idx;
            }

            if rbound == lbound {
                break;
            }
        }

        self.seek_by_idx(lbound);
    }

    /// Seek to the key-value pair by idx
    fn seek_by_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            return;
        }

        self.idx = idx;

        let entry_start = self.block.offsets[idx] as usize;
        let entry_end = self
            .block
            .offsets
            .get(idx + 1)
            .copied()
            .unwrap_or(self.block.data.len() as u16) as usize;

        let mut raw_entry = &self.block.data[entry_start..entry_end];

        let key_len = raw_entry.get_u16_le() as usize;
        self.key = KeyVec::from_vec(raw_entry[..key_len].to_vec());

        self.value_range = (entry_start + KEY_LEN + key_len + VALUE_LEN, entry_end);
    }
}
