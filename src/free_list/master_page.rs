use std::{fs::File, os::unix::prelude::FileExt};

use crate::prelude::*;

use byteorder::{ByteOrder, LittleEndian};
use fs2::FileExt as OtherFileExt;

use crate::b_tree::b_node::BTREE_PAGE_SIZE;

use super::mmap::MMap;

const DB_SIG: &str = "BuildYourOwnDB00";

pub struct MasterPage {
    pub btree_root: u64,
    pub total_used_pages: u64,
    pub free_list_head: u64,
}

impl MasterPage {
    pub fn new(btree_root: u64, total_used_pages: u64, free_list_head: u64) -> Self {
        Self {
            btree_root,
            total_used_pages,
            free_list_head,
        }
    }

    /// Loads the master page. If the file is empty, the master page will be created on the first write.
    /// If the master page is invalid, an error is returned.
    /// Returns the root of the BTree, and the head of the free list
    pub fn master_load(mmap: &MMap) -> Result<MasterPage> {
        if mmap.file == 0 {
            // empty file, the master page will be create on the first write
            return Ok(MasterPage {
                btree_root: 0,
                total_used_pages: 1, // reserved for the master page
                free_list_head: 0,
            });
        }

        let data = mmap.chunks[0].as_ref();
        let btree_root = LittleEndian::read_u64(&data[16..]);
        let total_used_pages = LittleEndian::read_u64(&data[24..]);
        let free_list_head = LittleEndian::read_u64(&data[32..]);

        // Check that the master page is valid
        if &data[..16] != DB_SIG.as_bytes() {
            return Err(Error::Static("bad signature"));
        }

        let mut bad =
            !(1 <= total_used_pages && total_used_pages <= mmap.file / BTREE_PAGE_SIZE as u64);
        bad = bad || btree_root >= total_used_pages;
        bad = bad || free_list_head >= total_used_pages;
        bad = bad || free_list_head < 1 || free_list_head == btree_root;

        if bad {
            return Err(Error::Static("bad master page"));
        }

        Ok(MasterPage {
            btree_root,
            total_used_pages,
            free_list_head,
        })
    }

    /// Saves the master page
    pub fn master_save(&self, file_pointer: &mut File) -> Result<()> {
        let mut data = [0; 40];
        // Convert signature to bytes
        assert!(DB_SIG.len() == 16, "const DG_SIG must be 16 bytes");
        data[..16].copy_from_slice(DB_SIG.as_bytes());
        LittleEndian::write_u64(&mut data[16..], self.btree_root);
        LittleEndian::write_u64(&mut data[24..], self.total_used_pages);
        LittleEndian::write_u64(&mut data[32..], self.free_list_head);

        // Atomic write to the master page
        file_pointer.lock_exclusive()?;
        let result = file_pointer.write_at(&data, 0);
        if let Err(err) = result {
            file_pointer.unlock()?;
            return Err(Error::IO(err));
        }
        file_pointer.unlock()?;

        Ok(())
    }
}
