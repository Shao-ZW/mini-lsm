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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

const META_BLOCK_OFFSET_LEN: usize = 4;
const BLOOM_FILTER_OFFSET_LEN: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        buf.put_u16_le(block_meta.len() as u16);

        for meta in block_meta {
            buf.put_u32_le(meta.offset as u32);
            buf.put_u16_le(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16_le(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let meta_num = buf.get_u16_le() as usize;
        let mut block_meta = Vec::with_capacity(meta_num);

        for _ in 0..meta_num {
            let offset = buf.get_u32_le() as usize;
            let first_key_len = buf.get_u16_le() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len = buf.get_u16_le() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));

            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bloom_filter_offset = file
            .read(
                file.1 - BLOOM_FILTER_OFFSET_LEN as u64,
                BLOOM_FILTER_OFFSET_LEN as u64,
            )?
            .as_slice()
            .get_u32_le() as usize;

        let bloom = Bloom::decode(
            file.read(
                bloom_filter_offset as u64,
                file.1 - bloom_filter_offset as u64 - BLOOM_FILTER_OFFSET_LEN as u64,
            )?
            .as_slice(),
        )?;

        let block_meta_offset = file
            .read(
                (bloom_filter_offset - META_BLOCK_OFFSET_LEN) as u64,
                META_BLOCK_OFFSET_LEN as u64,
            )?
            .as_slice()
            .get_u32_le() as usize;

        let block_meta = BlockMeta::decode_block_meta(
            file.read(
                block_meta_offset as u64,
                (bloom_filter_offset - block_meta_offset - META_BLOCK_OFFSET_LEN) as u64,
            )?
            .as_slice(),
        );

        let first_key = block_meta
            .first()
            .expect("At least one meta in a table")
            .first_key
            .clone();

        let last_key = block_meta
            .last()
            .expect("At least one meta in a table")
            .last_key
            .clone();

        Ok(Self {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_offset = self.block_meta[block_idx].offset;
        let next_block_offset = self
            .block_meta
            .get(block_idx + 1)
            .map(|meta| meta.offset)
            .unwrap_or(self.block_meta_offset);

        let raw_block_data = self.file.read(
            block_offset as u64,
            (next_block_offset - block_offset) as u64,
        )?;
        let block = Block::decode(&raw_block_data);

        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(block_cache) = &self.block_cache {
            block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the first block that contain key which >= `key`
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let (mut lbound, mut rbound) = (0, self.block_meta.len() - 1);

        while lbound < rbound {
            let idx = (lbound + rbound + 1) / 2;
            let meta = &self.block_meta[idx];

            if meta.first_key.as_key_slice() <= key {
                lbound = idx;
            } else {
                rbound = idx - 1;
            }
        }

        if self.block_meta[lbound].last_key.as_key_slice() < key {
            return (lbound + 1).min(self.block_meta.len() - 1);
        }

        lbound
    }

    /// Check whether sstable contains the key in the bound
    pub fn range_overlap(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        match lower {
            Bound::Excluded(key) => {
                if self.last_key.raw_ref() <= key {
                    return false;
                }
            }
            Bound::Included(key) => {
                if self.last_key.raw_ref() < key {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        match upper {
            Bound::Excluded(key) => {
                if self.first_key.raw_ref() >= key {
                    return false;
                }
            }
            Bound::Included(key) => {
                if self.first_key.raw_ref() > key {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        true
    }

    /// Using bloom filter check whether sstable contains the key
    pub fn bloom_filter(&self, key: KeySlice) -> bool {
        if let Some(bloom) = &self.bloom {
            bloom.may_contain(farmhash::fingerprint32(key.raw_ref()))
        } else {
            true
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
