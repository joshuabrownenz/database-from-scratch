pub struct TableDef {
    // User Defined
    pub name : String,
    pub types : Vec<u32>,
    pub columns : Vec<String>,
    pub primary_keys : i64,
    // Auto-assigned B-tree key prefixes for different tables
    pub prefix : u32,
}
