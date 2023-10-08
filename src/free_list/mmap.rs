extern crate libc;
extern crate memmap2; // Use the memmap2 crate for memory-mapped file support // Use the libc crate for the mmap flags

use crate::b_tree::b_node::{Node, BTREE_PAGE_SIZE};
use memmap2::{MmapMut, MmapOptions};
use std::fs::File;
use std::io::{self, Error, ErrorKind};

pub struct MMap {
    /** file size, can be larger than the database size */
    pub file: u64,
    /** mmap size, can be larger than the file size */
    pub total: usize,
    /** multiple mmaps, can be non-continuous */
    pub chunks: Vec<MmapMut>,
}

impl MMap {
    pub fn new(file_pointer: &File) -> io::Result<MMap> {
        let metadata = file_pointer.metadata()?;
        let file_size = metadata.len();

        if file_size % BTREE_PAGE_SIZE as u64 != 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "File size is not a multiple of page size.",
            ));
        }

        let mut mmap_size: usize = 64 << 20; // 64 MiB
        assert!(mmap_size % BTREE_PAGE_SIZE == 0);

        while mmap_size < file_size as usize {
            mmap_size *= 2;
        }

        // mmap_size can be larger than the file
        let mmap = unsafe { MmapOptions::new().len(mmap_size).map_mut(file_pointer)? };

        Ok(MMap {
            file: file_size,
            total: mmap_size,
            chunks: vec![mmap],
        })
    }

    pub fn extend_mmap(&mut self, file_pointer: &File, npages: usize) -> io::Result<()> {
        if self.total >= npages * BTREE_PAGE_SIZE {
            return Ok(());
        }

        let chunk = unsafe {
            memmap2::MmapOptions::new()
                .offset(self.total as u64)
                .len(self.total)
                .map_mut(file_pointer)
        }
        .map_err(|e| Error::new(ErrorKind::Other, format!("mmap: {}", e)))?;

        self.total *= 2;
        self.chunks.push(chunk);

        Ok(())
    }

    /** returns the chunk index and then the offset of the page the ptr is referring to */
    fn get_offset_of_ptr(&self, ptr: u64) -> (usize, u64) {
        let mut start: u64 = 0;
        for (i, chunk) in self.chunks.iter().enumerate() {
            let end = start + chunk.len() as u64 / BTREE_PAGE_SIZE as u64;
            if ptr < end {
                let offset = BTREE_PAGE_SIZE as u64 * (ptr - start);
                return (i, offset);
            }
            start = end;
        }
        panic!("bad pointer");
    }

    pub fn page_get_mapped<T: Node>(&self, ptr: u64) -> T {
        let (chunk_index, offset) = self.get_offset_of_ptr(ptr);
        let chunk = &self.chunks[chunk_index];
        T::from(&chunk[offset as usize..offset as usize + BTREE_PAGE_SIZE])
    }

    pub fn page_get_mapped_raw_mut(&mut self, ptr: u64) -> &mut [u8] {
        let (chunk_index, offset) = self.get_offset_of_ptr(ptr);
        let chunk = &mut self.chunks[chunk_index];
        &mut chunk[offset as usize..offset as usize + BTREE_PAGE_SIZE]
    }

    pub fn page_set(&mut self, ptr: u64, value: &[u8; BTREE_PAGE_SIZE]) {
        let (chunk_index, offset) = self.get_offset_of_ptr(ptr);
        let chunk = &mut self.chunks[chunk_index];
        chunk[offset as usize..offset as usize + BTREE_PAGE_SIZE].copy_from_slice(value);
    }

    pub fn close(mut self) {
        self.chunks.clear();
    }
}
