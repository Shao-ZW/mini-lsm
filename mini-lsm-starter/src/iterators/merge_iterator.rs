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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<_> = iters
            .into_iter()
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|item| HeapWrapper(item.0, item.1))
            .collect();

        let current = iters.pop();

        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if let Some(current) = &self.current {
            current.1.key()
        } else {
            unreachable!()
        }
    }

    fn value(&self) -> &[u8] {
        if let Some(current) = &self.current {
            current.1.value()
        } else {
            unreachable!()
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        if let Some(mut current) = self.current.take() {
            let used_key = current.1.key();

            loop {
                if let Some(mut iter) = self.iters.peek_mut() {
                    while iter.1.is_valid() && iter.1.key() <= used_key {
                        if let Err(err) = iter.1.next() {
                            PeekMut::pop(iter);
                            return Err(err);
                        }
                    }

                    if !iter.1.is_valid() {
                        PeekMut::pop(iter);
                    }
                } else {
                    break;
                }

                if let Some(iter) = self.iters.peek() {
                    if iter.1.key() > used_key {
                        break;
                    }
                }
            }

            current.1.next()?;

            if !current.1.is_valid() {
                self.current = self.iters.pop();
                return Ok(());
            }

            let exchange = if let Some(peek) = self.iters.peek() {
                *peek > current
            } else {
                false
            };

            if exchange {
                self.iters.push(current);
                self.current = self.iters.pop();
            } else {
                self.current = Some(current);
            }
        }

        Ok(())
    }
}
