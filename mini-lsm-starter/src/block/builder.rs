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

use bytes::BufMut;

use crate::{
    block::{KEY_OVERLAP_LEN_SIZE, KEY_REST_LEN_SIZE, OFFSET_LEN_SIZE, VALUE_LEN_SIZE},
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The current block size
    current_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            current_size: OFFSET_LEN_SIZE, // num of elements initialize to 0
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_overlap_len = key.overlap_prefix_len(self.first_key.as_key_slice());
        let key_rest_len = key.len() - key_overlap_len;

        let entry_size: u16 = (KEY_OVERLAP_LEN_SIZE
            + KEY_REST_LEN_SIZE
            + key_rest_len
            + VALUE_LEN_SIZE
            + value.len())
        .try_into()
        .expect("The entry len should not bigger than 65535(2 Bytes)");

        if self.current_size + entry_size as usize + OFFSET_LEN_SIZE > self.block_size
            && !self.offsets.is_empty()
        {
            return false;
        }

        if self.offsets.is_empty() {
            self.first_key = key.to_key_vec();
        }

        self.offsets.push(
            self.data
                .len()
                .try_into()
                .expect("The offset should not bigger than 65535(2 Bytes)"),
        );

        self.current_size += entry_size as usize + OFFSET_LEN_SIZE;

        self.data.put_u16_le(key_overlap_len as u16);
        self.data.put_u16_le(key_rest_len as u16);
        self.data.put_slice(&key.raw_ref()[key_overlap_len..]);
        self.data.put_u16_le(value.len() as u16);
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        assert!(self.current_size <= self.block_size || self.offsets.len() == 1);

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
