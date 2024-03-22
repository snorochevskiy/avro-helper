
use apache_avro::{types::Value};

/// Using given path, traverses AVRO row tree and returns the node identified by the path.
/// 
/// E.g. given we have row: {id: 1, date: "2020-01-01", person: {name: "John", age: 25}}
/// and a path ["person", "name"].
/// Then function should return Value::String("John")
pub fn extract<'a, S>(graph: &'a Value, path: &[S]) -> Option<&'a Value> where S : AsRef<str> {
    let mut current_node: &Value = &graph;
    'outer: for element_name in path {
        match current_node {
            Value::Record(ref fields) => {
                for (field_name, field_value) in fields {
                    if field_name == element_name.as_ref() {
                        match field_value {
                            Value::Union(_, b) => {
                                if let Value::Null = **b {
                                    return None;
                                } else {
                                    current_node = b.as_ref();
                                }
                            },
                            v => current_node = v,
                        }
                        continue 'outer;
                    }
                }
                return None;
            },
            _ => return None
        }
    }
    Some(current_node)
}
