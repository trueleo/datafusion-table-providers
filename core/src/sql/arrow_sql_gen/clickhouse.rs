use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

pub fn clickhouse_type_to_arrow_type(clickhouse_type: &str) -> Option<DataType> {
    let type_ = clickhouse_type.trim_matches([' ', '\n']);

    // Strip Nullable(...)
    if let Some(inner) = strip_wrapper(type_, "Nullable") {
        return clickhouse_type_to_arrow_type(inner);
    }

    // Strip LowCardinality(...)
    if let Some(inner) = strip_wrapper(type_, "LowCardinality") {
        return clickhouse_type_to_arrow_type(inner);
    }

    // FixedString(N)
    if type_.starts_with("FixedString(") {
        return Some(DataType::Utf8);
    }

    // Decimal(P, S)
    if type_.starts_with("Decimal(") {
        let (precision, scale) = strip_wrapper(type_, "Decimal")
            .and_then(|inner| inner.split_once(',').map(|(p, s)| (p.trim(), s.trim())))
            .unwrap();

        if let (Ok(precision), Ok(scale)) = (precision.parse::<u8>(), scale.parse::<u8>()) {
            return Some(DataType::Decimal128(precision, scale as i8));
        } else {
            panic!(
                "Could not parse precision and scale on the returned Decimal value P:{}, S:{}",
                precision, scale
            );
        }
    }

    if type_ == "Decimal" {
        return Some(DataType::Decimal128(38, 10));
    }

    // DateTime64(N)
    if type_.starts_with("DateTime64(") {
        let precision = strip_wrapper(type_, "DateTime64").unwrap();
        let precision: u8 = precision.parse().unwrap();
        let unit = match precision {
            3 => TimeUnit::Millisecond,
            6 => TimeUnit::Microsecond,
            9 => TimeUnit::Nanosecond,
            _ => return None,
        };
        return Some(DataType::Timestamp(unit, None));
    }

    if type_ == "DateTime64" {
        return Some(DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    // Arrays
    if let Some(inner) = strip_wrapper(type_, "Array") {
        let inner_type = clickhouse_type_to_arrow_type(inner)?;
        return Some(DataType::List(Field::new("item", inner_type, true).into()));
    }

    // Enums → treated as Utf8 for now
    if type_.starts_with("Enum") {
        return Some(DataType::Utf8);
    }

    // Tuple types
    if type_.starts_with("Tuple(") {
        return resolve_tuple_type(type_);
    }

    // Base types
    match type_.to_lowercase().as_str() {
        "int8" => Some(DataType::Int8),
        "int16" => Some(DataType::Int16),
        "int32" | "int" => Some(DataType::Int32),
        "int64" => Some(DataType::Int64),

        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),

        "float32" => Some(DataType::Float32),
        "float64" | "float" => Some(DataType::Float64),

        "string" | "uuid" => Some(DataType::Utf8),

        "date" => Some(DataType::Date32),
        "datetime" => Some(DataType::Timestamp(TimeUnit::Second, None)),

        "boolean" | "bool" => Some(DataType::Boolean),

        _ => None,
    }
}

/// Helper to strip wrapping types like Nullable(...), Array(...), etc.
fn strip_wrapper<'a>(s: &'a str, wrapper: &str) -> Option<&'a str> {
    let s = s.strip_prefix(wrapper)?;
    let s = s.strip_prefix('(')?;
    let s = s.strip_suffix(')')?;
    Some(s)
}

fn resolve_tuple_type(clickhouse_type: &str) -> Option<DataType> {
    let inner = strip_wrapper(clickhouse_type, "Tuple")?;
    let parts = split_comma_separated(inner);
    let fields: Option<Vec<Field>> = parts
        .into_iter()
        .map(|mut part| {
            part = part.trim_matches([' ', '\n']);
            let (field_name, type_) = part.split_once(' ').unwrap();
            clickhouse_type_to_arrow_type(type_).map(|dt| Field::new(field_name.trim(), dt, true))
        })
        .collect();
    fields.map(|fields| DataType::Struct(fields.into()))
}

/// Splits comma-separated parts, handling nested types correctly (e.g., Tuple(Int32, Array(Nullable(String))))
fn split_comma_separated(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut level = 0;
    let mut start = 0;

    for (i, c) in s.char_indices() {
        match c {
            '(' => level += 1,
            ')' => level -= 1,
            ',' if level == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        parts.push(&s[start..]);
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

    #[test]
    fn test_basic_types() {
        let cases = vec![
            ("Int8", DataType::Int8),
            ("Int16", DataType::Int16),
            ("Int32", DataType::Int32),
            ("Int64", DataType::Int64),
            ("UInt8", DataType::UInt8),
            ("UInt16", DataType::UInt16),
            ("UInt32", DataType::UInt32),
            ("UInt64", DataType::UInt64),
            ("Float32", DataType::Float32),
            ("Float64", DataType::Float64),
            ("String", DataType::Utf8),
            ("UUID", DataType::Utf8),
            ("Bool", DataType::Boolean),
            ("Boolean", DataType::Boolean),
        ];

        for (ch_type, expected) in cases {
            assert_eq!(
                clickhouse_type_to_arrow_type(ch_type),
                Some(expected),
                "Failed for: {}",
                ch_type
            );
        }
    }

    #[test]
    fn test_temporal_types() {
        assert_eq!(
            clickhouse_type_to_arrow_type("Date"),
            Some(DataType::Date32)
        );

        assert_eq!(
            clickhouse_type_to_arrow_type("DateTime"),
            Some(DataType::Timestamp(TimeUnit::Second, None))
        );

        assert_eq!(
            clickhouse_type_to_arrow_type("DateTime64"),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        );

        assert_eq!(
            clickhouse_type_to_arrow_type("DateTime64(3)"),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        );
    }

    #[test]
    fn test_wrappers_nullable_lowcardinality() {
        assert_eq!(
            clickhouse_type_to_arrow_type("Nullable(Int32)"),
            Some(DataType::Int32)
        );

        assert_eq!(
            clickhouse_type_to_arrow_type("LowCardinality(String)"),
            Some(DataType::Utf8)
        );

        assert_eq!(
            clickhouse_type_to_arrow_type("Nullable(LowCardinality(UInt64))"),
            Some(DataType::UInt64)
        );
    }

    #[test]
    fn test_decimal() {
        assert_eq!(
            clickhouse_type_to_arrow_type("Decimal(10, 2)"),
            Some(DataType::Decimal128(10, 2))
        );

        assert_eq!(
            clickhouse_type_to_arrow_type("Decimal"),
            Some(DataType::Decimal128(38, 10))
        );
    }

    #[test]
    fn test_array_of_primitive() {
        assert_eq!(
            clickhouse_type_to_arrow_type("Array(Int32)"),
            Some(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true
            ))))
        );
    }

    #[test]
    fn test_enum_as_utf8() {
        assert_eq!(
            clickhouse_type_to_arrow_type("Enum8('ok' = 0, 'error' = 1)"),
            Some(DataType::Utf8)
        );
    }

    #[test]
    fn test_tuple_unnamed() {
        let result = clickhouse_type_to_arrow_type("Tuple(Int32, String)").unwrap();
        if let DataType::Struct(fields) = result {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].data_type(), &DataType::Int32);
            assert_eq!(fields[1].data_type(), &DataType::Utf8);
        } else {
            panic!("Expected Struct");
        }
    }

    #[test]
    fn test_tuple_named() {
        let result = clickhouse_type_to_arrow_type("Tuple(code Int32, msg String)").unwrap();
        if let DataType::Struct(fields) = result {
            assert_eq!(fields[0].name(), "code");
            assert_eq!(fields[0].data_type(), &DataType::Int32);
            assert_eq!(fields[1].name(), "msg");
            assert_eq!(fields[1].data_type(), &DataType::Utf8);
        } else {
            panic!("Expected Struct");
        }
    }

    #[test]
    fn test_array_of_named_tuple() {
        let result = clickhouse_type_to_arrow_type(
            "Array(Tuple(error_code String, error_description String))",
        )
        .unwrap();
        if let DataType::List(inner) = result {
            if let DataType::Struct(fields) = inner.data_type() {
                assert_eq!(fields[0].name(), "error_code");
                assert_eq!(fields[1].name(), "error_description");
            } else {
                panic!("Expected Struct inside Array");
            }
        } else {
            panic!("Expected List");
        }
    }

    #[test]
    fn test_nested_tuple_of_tuples() {
        let result =
            clickhouse_type_to_arrow_type("Tuple(inner Tuple(id UInt32, tag String), status Bool)")
                .unwrap();
        if let DataType::Struct(fields) = result {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "inner");

            if let DataType::Struct(nested) = fields[0].data_type() {
                assert_eq!(nested[0].name(), "id");
                assert_eq!(nested[1].data_type(), &DataType::Utf8);
            } else {
                panic!("Expected nested Struct");
            }

            assert_eq!(fields[1].name(), "status");
        } else {
            panic!("Expected top-level Struct");
        }
    }

    #[test]
    fn test_unsupported_type_returns_none() {
        assert_eq!(clickhouse_type_to_arrow_type("CustomTypeXYZ"), None);
        assert_eq!(clickhouse_type_to_arrow_type("Map(String, Int32)"), None);
    }
}
