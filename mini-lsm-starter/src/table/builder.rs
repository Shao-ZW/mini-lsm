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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::KeySlice,
    lsm_storage::BlockCache,
    table::{FileObject, bloom::Bloom},
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    hash_keys: Vec<u32>,
}

const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            data: Vec::new(),
            meta: Vec::new(),
            hash_keys: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let need_split = !self.builder.add(key, value);

        if need_split {
            self.force_build_block();
            let _ = self.builder.add(key, value);
        }

        self.hash_keys.push(farmhash::fingerprint32(key.raw_ref()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.force_build_block();
        }

        let first_key = self
            .meta
            .first()
            .expect("At least one meta in a table")
            .first_key
            .clone();

        let last_key = self
            .meta
            .last()
            .expect("At least one meta in a table")
            .last_key
            .clone();

        let meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32_le(meta_offset as u32);

        let bloom_offset = self.data.len();
        let bloom = Bloom::build_from_key_hashes(
            &self.hash_keys,
            Bloom::bloom_bits_per_key(self.hash_keys.len(), BLOOM_FALSE_POSITIVE_RATE),
        );
        bloom.encode(&mut self.data);
        self.data.put_u32_le(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), self.data)?;

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

    fn force_build_block(&mut self) {
        let block_builder =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        let block = block_builder.build();
        let mut block_data = block.encode().to_vec();

        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: block.get_first_key().into_key_bytes(),
            last_key: block.get_last_key().into_key_bytes(),
        });

        self.data.append(&mut block_data);
    }
}
