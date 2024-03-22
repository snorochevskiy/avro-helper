use apache_avro::{types::Value, Reader};
use avro_utils::avro_lens;
use std::fs;

#[test]
fn test_avro_uncompressed() {
    // root
    //  |-- version: string (nullable = true)
    //  |-- uuid: string (nullable = true)
    //  |-- msg: struct (nullable = true)
    //  |    |-- msgId: long (nullable = false)
    //  |    |-- msgName: string (nullable = true)
    //  |    |-- nums: array (nullable = true)
    //  |    |    |-- element: integer (containsNull = false)
    let f = fs::File::open("test_data/avro/uncompressed/part-00000-cdb31271-7aba-45f8-baf5-57cb827b8a0d-c000.avro",).unwrap();

    let reader = Reader::new(f).unwrap();

    let results = reader
        .into_iter()
        .filter_map(|maybe_row| {
            maybe_row
                .ok()
                .and_then(|row| avro_lens::extract(&row, &["msg", "msgId"]).map(|r| r.to_owned()))
        })
        .collect::<Vec<Value>>();

    assert_eq!(results, vec![Value::Long(1), Value::Long(2), Value::Long(3)]);
}
