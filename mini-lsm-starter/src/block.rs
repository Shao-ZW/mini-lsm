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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
pub use iterator::BlockIterator;

use bytes::{Buf, BufMut, Bytes};

use crate::key::KeyVec;

// Key overlap length 2 bytes
const KEY_OVERLAP_LEN_SIZE: usize = 2;
// Key rest length 2 bytes
const KEY_REST_LEN_SIZE: usize = 2;
// Value length 2 bytes
const VALUE_LEN_SIZE: usize = 2;
// Offset length and num of elements both 2 bytes
const OFFSET_LEN_SIZE: usize = 2;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = self.data.clone();
        for offset in &self.offsets {
            bytes.put_u16_le(*offset);
        }
        bytes.put_u16_le(self.offsets.len() as u16);

        bytes.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        assert!(data.len() >= OFFSET_LEN_SIZE);

        let elements_num = (&data[data.len() - OFFSET_LEN_SIZE..]).get_u16_le() as usize;
        let data_end = data.len() - (elements_num + 1) * OFFSET_LEN_SIZE;

        let data_raw = &data[..data_end];
        let offsets_raw = &data[data_end..data.len() - OFFSET_LEN_SIZE];

        let data = data_raw.to_vec();
        let offsets = offsets_raw
            .chunks(OFFSET_LEN_SIZE)
            .map(|mut chunk| chunk.get_u16_le())
            .collect();

        Self { data, offsets }
    }

    pub fn get_first_key(&self) -> KeyVec {
        let entry_start = 0;
        let mut raw_entry = &self.data[entry_start..];

        let key_overlap_len = raw_entry.get_u16_le() as usize;
        assert_eq!(key_overlap_len, 0);
        let key_rest_len = raw_entry.get_u16_le() as usize;
        KeyVec::from_vec(raw_entry[..key_rest_len].to_vec())
    }

    pub fn get_last_key(&self) -> KeyVec {
        let entry_start = self
            .offsets
            .last()
            .copied()
            .expect("At least have one key-value pair") as usize;

        let mut raw_entry = &self.data[entry_start..];

        let key_overlap_len = raw_entry.get_u16_le() as usize;
        let key_rest_len = raw_entry.get_u16_le() as usize;
        let first_key = self.get_first_key();

        KeyVec::from_vec({
            let mut key = Vec::with_capacity(key_overlap_len + key_rest_len);
            key.extend_from_slice(&first_key.raw_ref()[..key_overlap_len]);
            key.extend_from_slice(&raw_entry[..key_rest_len]);
            key
        })
    }
}
