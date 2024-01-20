use database_from_scratch::kv_store::KV;

fn main() {
    let kv = KV::open("main.db".to_string());
    let mut kv = kv.unwrap_or_else(|_| panic!("Failed to open database"));

    kv.set("hello".as_bytes(), "world".as_bytes())
        .unwrap();
    println!(
        "hello {}",
        String::from_utf8(kv.get(&"hello".as_bytes().to_vec()).unwrap()).unwrap()
    );
    kv.del(&"hello".as_bytes().to_vec()).unwrap();
    kv.close();
}
