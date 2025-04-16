use std::{sync::Arc, usize};

use arrow::{
    array::{
        make_builder, ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder,
        Decimal256Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        Int64Builder, Int8Builder, ListBuilder, NullBuilder, RecordBatch, StringBuilder,
        StructBuilder, TimestampSecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder,
        UInt8Builder,
    },
    datatypes::{i256, DataType, Field, Schema, TimeUnit},
};
use klickhouse::{block::Block, Type, Value};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {clickhouse_type}"))]
    FailedToDowncastBuilder { clickhouse_type: DataType },

    #[snafu(display("Failed to get a row value for {clickhouse_type}: {source}"))]
    FailedToGetRowValue {
        clickhouse_type: DataType,
        source: klickhouse::KlickhouseError,
    },

    #[snafu(display("Failed to append a row value for {}: {}", clickhouse_type, source))]
    FailedToAppendRowValue {
        clickhouse_type: DataType,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("No Arrow field found for index {index}"))]
    NoArrowFieldForIndex { index: usize },

    #[snafu(display("No column name for index: {index}"))]
    NoColumnNameForIndex { index: usize },

    #[snafu(display("Failed to parse decimal string as BigInterger {value}: {source}"))]
    FailedToParseBigDecimalFromClickhouse {
        value: String,
        source: bigdecimal::ParseBigDecimalError,
    },

    #[snafu(display("Unsupported column type: {:?}", column_type))]
    UnsupportedColumnType { column_type: Type },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Converts `Clickhouse` `Block` to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the `sql_type` returned for column.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
pub fn block_to_arrow(block: Block) -> Result<RecordBatch> {
    let fields = block
        .column_types
        .iter()
        .map(|(name, column_type)| Field::new(name, map_column_to_data_type(column_type), true))
        .collect::<Vec<_>>();

    let capacity = block.rows.max(usize::MAX as u64) as usize;
    let mut builders: Vec<_> = fields
        .iter()
        .map(|field| make_builder(field.data_type(), capacity))
        .collect();

    drive_struct_builder(&fields, &mut builders, block.column_data)?;

    let columns = builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect::<Vec<ArrayRef>>();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|err| Error::FailedToBuildRecordBatch { source: err })
}

fn map_column_to_data_type(column_type: &Type) -> DataType {
    match column_type {
        Type::Int8 => DataType::Int8,
        Type::Int16 => DataType::Int16,
        Type::Int32 => DataType::Int32,
        Type::Int64 => DataType::Int64,
        Type::UInt8 => DataType::UInt8,
        Type::UInt16 => DataType::UInt16,
        Type::UInt32 => DataType::UInt32,
        Type::UInt64 => DataType::UInt64,
        Type::Float32 => DataType::Float32,
        Type::Float64 => DataType::Float64,
        Type::String | Type::FixedString(_) | Type::Uuid => DataType::Utf8,
        Type::Date => DataType::Date32,
        Type::DateTime(_) => DataType::Timestamp(TimeUnit::Second, None),
        Type::DateTime64(_, tz) => DataType::Timestamp(TimeUnit::Second, Some(tz.name().into())),
        Type::Decimal32(align) => DataType::Decimal128(9, *align as i8),
        Type::Decimal64(align) => DataType::Decimal128(18, *align as i8),
        Type::Decimal128(align) => DataType::Decimal128(38, *align as i8),
        Type::Decimal256(align) => DataType::Decimal256(76, *align as i8),
        Type::Nullable(inner) => map_column_to_data_type(inner),
        Type::Array(inner) => DataType::new_list(map_column_to_data_type(&inner), true),
        Type::Tuple(inner) => {
            let types: Vec<_> = inner
                .iter()
                .map(|x| map_column_to_data_type(x))
                .enumerate()
                .map(|(idx, value)| Field::new(idx.to_string(), value, true))
                .collect();
            DataType::Struct(types.into())
        }
        _ => unimplemented!("Unsupported column type {:?}", column_type),
    }
}

fn drive_struct_builder<'a>(
    fields: &'a [Field],
    builder: &'a mut [Box<dyn ArrayBuilder>],
    mut data: klickhouse::IndexMap<String, Vec<Value>>,
) -> Result<()> {
    for (idx, field) in fields.iter().enumerate() {
        drive_builder(
            field,
            &mut builder[idx],
            data.swap_remove(field.name()).unwrap(),
        )
    }
    todo!()
}

fn drive_builder(field: &Field, builder: &mut dyn ArrayBuilder, data: Vec<Value>) {
    match field.data_type() {
        DataType::Null => builder
            .as_any_mut()
            .downcast_mut::<NullBuilder>()
            .unwrap()
            .append_empty_values(data.len()),
        DataType::Boolean => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            for value in data {
                match value {
                    Value::UInt8(value) => builder.append_value(value != 0u8),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Int8 => {
            let builder = builder.as_any_mut().downcast_mut::<Int8Builder>().unwrap();
            for value in data {
                match value {
                    Value::Int8(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Int16 => {
            let builder = builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap();
            for value in data {
                match value {
                    Value::Int16(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Int32 => {
            let builder = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            for value in data {
                match value {
                    Value::Int32(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Int64 => {
            let builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            for value in data {
                match value {
                    Value::Int64(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::UInt8 => {
            let builder = builder.as_any_mut().downcast_mut::<UInt8Builder>().unwrap();
            for value in data {
                match value {
                    Value::UInt8(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::UInt16 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .unwrap();
            for value in data {
                match value {
                    Value::UInt16(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::UInt32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .unwrap();
            for value in data {
                match value {
                    Value::UInt32(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::UInt64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap();
            for value in data {
                match value {
                    Value::UInt64(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Float32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap();
            for value in data {
                match value {
                    Value::Float32(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Float64 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            for value in data {
                match value {
                    Value::Float64(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Date32 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap();
            for value in data {
                match value {
                    Value::Date(value) => builder.append_value(value.0.into()),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Timestamp(_, _) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampSecondBuilder>()
                .unwrap();
            for value in data {
                match value {
                    Value::DateTime(value) => builder.append_value(value.1.into()),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Utf8 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            for value in data {
                match value {
                    Value::String(value) => builder.append_value(String::from_utf8(value).unwrap()),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Decimal128(_, _) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .unwrap();
            for value in data {
                match value {
                    Value::Decimal32(_, value) => builder.append_value(value as i128),
                    Value::Decimal64(_, value) => builder.append_value(value as i128),
                    Value::Decimal128(_, value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Decimal256(_, _) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Decimal256Builder>()
                .unwrap();
            for value in data {
                match value {
                    Value::Decimal256(_, value) => {
                        builder.append_value(i256::from_be_bytes(value.0))
                    }
                    Value::Null => builder.append_null(),
                    _ => unreachable!(),
                }
            }
        }
        DataType::List(field) => {
            let list_builder = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();

            for value in data {
                match value {
                    Value::Array(data) => {
                        drive_builder(field, list_builder.values(), data);
                        list_builder.append(true);
                    }
                    Value::Null => list_builder.append(false),
                    _ => unreachable!(),
                }
            }
        }
        DataType::Struct(fields) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();

            for value in data {
                match value {
                    Value::Tuple(data) => {
                        for (idx, (field, data)) in fields.iter().zip(data).enumerate() {
                            drive_builder(
                                field,
                                &mut builder.field_builders_mut()[idx],
                                vec![data],
                            );
                        }
                        builder.append(true);
                    }
                    Value::Null => builder.append(false),
                    _ => unreachable!(),
                }
            }
        }
        _ => unimplemented!(),
    }
}
