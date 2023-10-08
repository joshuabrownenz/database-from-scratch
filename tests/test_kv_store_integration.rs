use database_from_scratch::kv_store::KV;

#[test]
fn test_kv_store_integration() {
    let mut kv = KV::open("test.db".to_string()).unwrap();

    // Test `set` and `get`
    kv.set(&"key1".as_bytes().to_vec(), &"value1".as_bytes().to_vec())
        .unwrap();
    assert_eq!(
        kv.get(&"key1".as_bytes().to_vec()).unwrap(),
        "value1".as_bytes().to_vec()
    );

    // Test `set` and `remove`
    kv.set(&"key2".as_bytes().to_vec(), &"value2".as_bytes().to_vec())
        .unwrap();
    kv.del(&"key2".as_bytes().to_vec()).unwrap();
    assert!(kv.get(&"key2".as_bytes().to_vec()).is_err());
}
