pub struct BTree  {
    // pointer (a nonzero page number)
    pub root: u64,
}

impl BTree {
    pub fn new() -> BTree {
        BTree {
            root: 0,
        }
    }

    
}