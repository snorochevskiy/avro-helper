use apache_avro::types::Value as AvroValue;
use apache_avro::{Schema, Reader};

use avro_utils::avro_lens;

pub fn process_avro(read: impl std::io::Read, column_path: &[String], values: &[String]) -> (Vec<AvroValue>, Schema) {
    let reader = Reader::new(read).unwrap();

    let schema = reader.writer_schema().clone();

    let mut result: Vec<AvroValue> = Vec::new();

    for maybe_row in reader.into_iter() {
        if let Ok(row) = maybe_row {
            if let Some(v) = avro_lens::extract(&row, column_path) {
                if check_value_eq(v, values) {
                    result.push(row.clone());
                }
            }
        }
    }

    (result, schema)
}

fn check_value_eq(v: &AvroValue, values: &[String]) -> bool {
    match v {
        AvroValue::Long(a) => values.contains(&a.to_string()),
        AvroValue::Int(a) => values.contains(&a.to_string()),
        AvroValue::Boolean(a) => values.contains(&a.to_string()),
        AvroValue::String(a) => values.contains(&a),
        _ => false,
    }
}