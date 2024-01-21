use crate::prelude::*;
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::Write,
};

use crate::{
    b_tree::b_node::{Node, BTREE_PAGE_SIZE},
    free_list::fl_node::FLNode,
};

use super::{master_page::MasterPage, mmap::MMap};
pub struct PageManager {
    /// Mapped memory of the database file
    pub mmap: MMap,
    /// Pointer to the database file
    pub file_pointer: File,
    /// Database size in number of pages
    pub flushed: u64,
    /// Number of pages appended to the database
    pub nappend: i64,
    /// newly allocated or deallocated pages keyed by the pointer. empty vector means the page is deallocated
    pub updates: HashMap<u64, Option<[u8; BTREE_PAGE_SIZE]>>,
}

impl PageManager {
    pub fn new(file_pointer: File) -> Result<Self> {
        Ok(Self {
            mmap: MMap::new(&file_pointer)?,
            file_pointer,
            flushed: 0,
            nappend: 0,
            updates: HashMap::new(),
        })
    }

    pub fn master_load(&mut self) -> Result<MasterPage> {
        let master_page = MasterPage::master_load(&self.mmap)?;
        self.flushed = master_page.total_used_pages;
        Ok(master_page)
    }

    pub fn set_master_page(&mut self, btree_root: u64, free_list_head: u64) -> Result<()> {
        let master_page = MasterPage::new(btree_root, self.flushed, free_list_head);
        master_page.master_save(&mut self.file_pointer)
    }

    pub fn page_get<T: Node>(&self, ptr: u64) -> T {
        // Get from temp pages if it exists
        match self.updates.get(&ptr) {
            Some(data) => T::from(data.as_ref().unwrap()),
            None => self.mmap.page_get_mapped(ptr),
        }
    }

    pub fn page_get_raw_mut(&mut self, ptr: u64) -> &mut [u8] {
        // Get from temp pages if it exists
        match self.updates.get_mut(&ptr) {
            Some(data) => &mut data.as_mut().unwrap()[..],
            None => self.mmap.page_get_mapped_raw_mut(ptr),
        }
    }

    // callback for free list, allocate a new page
    pub fn page_append(&mut self, node: FLNode) -> u64 {
        let ptr = self.flushed + self.nappend as u64;
        self.nappend += 1;
        self.updates.insert(ptr, Some(node.get_data()));
        ptr
    }

    // callback for free list, reuse a page
    pub fn page_reuse(&mut self, ptr: u64, node: FLNode) {
        self.updates.insert(ptr, Some(node.get_data()));
    }

    pub fn page_del(&mut self, ptr: u64) {
        self.updates.insert(ptr, None);
    }

    pub fn get_freed_ptrs(&mut self) -> VecDeque<u64> {
        let mut freed_ptrs = VecDeque::new();
        for (ptr, data) in self.updates.iter() {
            if data.is_none() {
                freed_ptrs.push_back(*ptr);
            }
        }
        freed_ptrs
    }

    pub fn write_pages(&mut self) -> Result<()> {
        self.extend_file()?;
        self.extend_mmap()?;

        // copy temp data to mmap
        for (ptr, temp_page) in self.updates.iter() {
            if temp_page.is_some() {
                self.mmap.page_set(*ptr, temp_page.as_ref().unwrap());
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        // Flush data to the disk. Must be done before updating the master page.
        self.file_pointer.flush()?;

        self.flushed += self.nappend as u64;
        self.nappend = 0;
        self.updates.clear();

        Ok(())
    }

    pub fn extend_file(&mut self) -> Result<()> {
        let npages = self.flushed + self.nappend as u64;
        let mut file_pages = self.mmap.file / BTREE_PAGE_SIZE as u64;
        if file_pages >= npages {
            return Ok(());
        }

        while file_pages < npages {
            // the file size is increased exponentially,
            // so that we don't have to extend the file for every update.
            let mut inc = file_pages / 8;
            if inc < 1 {
                inc = 1;
            }
            file_pages += inc;
        }

        let file_size = file_pages * BTREE_PAGE_SIZE as u64;
        let result = self.file_pointer.set_len(file_size);
        if result.is_err() {
            return Err(Error::Generic(format!(
                "failed to extend file: {:?}",
                result.unwrap_err()
            )));
        }

        self.mmap.file = file_size;
        Ok(())
    }

    pub fn extend_mmap(&mut self) -> Result<()> {
        let npages = self.flushed + self.nappend as u64;
        self.mmap.extend_mmap(&self.file_pointer, npages as usize)
    }

    pub fn close(self) {
        self.mmap.close();
    }
}
