mod b_tree;
mod free_list;
mod kv_store;

use kv_store::KV;

fn main() {
    let kv = KV::open("main.db".to_string());
    let mut kv = kv.unwrap_or_else(|_| panic!("Failed to open database"));

    kv.set(&"hello".as_bytes().to_vec(), &"world".as_bytes().to_vec())
        .unwrap();
    println!(
        "hello {}",
        String::from_utf8(kv.get(&"hello".as_bytes().to_vec()).unwrap()).unwrap()
    );
    kv.del(&"hello".as_bytes().to_vec()).unwrap();
    kv.close();
}
