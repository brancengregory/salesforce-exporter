use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use cirrus::Cirrus;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::config;

/// Export a single Salesforce table to a Parquet file.
pub async fn run(
    sf: &Cirrus,
    table_name: &str,
    limit: Option<usize>,
    output: Option<PathBuf>,
    requested_fields: Option<Vec<String>>,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        let field_types = resolve_export_fields(sf, table_name, requested_fields).await?;
        let output_file = resolve_output_path(table_name, output)?;
        println!(
            "[dry-run] Would export {} with {} fields to {}",
            table_name,
            field_types.len(),
            output_file.display()
        );
        for (name, dtype) in &field_types {
            println!("  - {}: {:?}", name, dtype);
        }
        return Ok(());
    }

    let batch = fetch_table_as_arrow(sf, table_name, limit, requested_fields).await?;

    println!("\nFetched {} rows with {} columns", batch.num_rows(), batch.num_columns());

    let output_file = resolve_output_path(table_name, output)?;
    write_to_parquet(&batch, &output_file)?;
    println!("Written to {}", output_file.display());

    Ok(())
}

/// Batch export from a TOML config file.
pub async fn run_batch(sf: &Cirrus, config_path: &PathBuf, dry_run: bool) -> Result<()> {
    let spec = config::load_config(config_path)?;

    let mut successes = 0;
    let mut failures: Vec<String> = Vec::new();

    for item in &spec.export {
        let effective_limit = item.limit.or(spec.limit);
        let effective_output = item.output.clone().or_else(|| spec.output_dir.clone());
        let effective_fields = item.fields.clone();

        if dry_run {
            match resolve_export_fields(sf, &item.object, effective_fields).await {
                Ok(field_types) => {
                    match resolve_output_path(&item.object, effective_output) {
                        Ok(path) => {
                            println!(
                                "[dry-run] Would export {} with {} fields to {}",
                                item.object,
                                field_types.len(),
                                path.display()
                            );
                            for (name, dtype) in &field_types {
                                println!("  - {}: {:?}", name, dtype);
                            }
                            successes += 1;
                        }
                        Err(e) => {
                            let msg = format!(
                                "{}: failed to resolve output path: {}",
                                item.object, e
                            );
                            eprintln!("[dry-run] {}", msg);
                            failures.push(msg);
                        }
                    }
                }
                Err(e) => {
                    let msg = format!("{}: {}", item.object, e);
                    eprintln!("[dry-run] {}", msg);
                    failures.push(msg);
                }
            }
        } else {
            let result =
                fetch_table_as_arrow(sf, &item.object, effective_limit, effective_fields).await;
            match result {
                Ok(batch) => {
                    let output_file = match resolve_output_path(&item.object, effective_output) {
                        Ok(p) => p,
                        Err(e) => {
                            let msg = format!(
                                "{}: failed to resolve output path: {}",
                                item.object, e
                            );
                            eprintln!("{}", msg);
                            failures.push(msg);
                            continue;
                        }
                    };

                    if let Err(e) = write_to_parquet(&batch, &output_file) {
                        let msg = format!("{}: failed to write parquet: {}", item.object, e);
                        eprintln!("{}", msg);
                        failures.push(msg);
                    } else {
                        println!(
                            "Exported {}: {} rows, {} columns -> {}",
                            item.object,
                            batch.num_rows(),
                            batch.num_columns(),
                            output_file.display()
                        );
                        successes += 1;
                    }
                }
                Err(e) => {
                    let msg = format!("{}: {}", item.object, e);
                    eprintln!("{}", msg);
                    failures.push(msg);
                }
            }
        }
    }

    println!(
        "\nBatch export complete: {} succeeded, {} failed",
        successes,
        failures.len()
    );
    if !failures.is_empty() {
        println!("\nFailures:");
        for f in &failures {
            println!("  - {}", f);
        }
        return Err(anyhow::anyhow!(
            "Batch export completed with {} failures",
            failures.len()
        ));
    }

    Ok(())
}

/// Resolve and validate which fields to export for a given object.
/// Errors if a requested field is missing or has an unsupported type.
/// When no fields are requested, all fields are used and any unsupported type causes an error.
async fn resolve_export_fields(
    sf: &Cirrus,
    table_name: &str,
    requested_fields: Option<Vec<String>>,
) -> Result<Vec<(String, DataType)>> {
    let describe = sf.sobject(table_name).describe().await?;

    let mut field_map: HashMap<String, (String, String)> = HashMap::new();

    if let Some(fields) = describe.get("fields").and_then(|f| f.as_array()) {
        for field in fields {
            if let Some(name) = field.get("name").and_then(|n| n.as_str())
                && let Some(sf_type) = field.get("type").and_then(|t| t.as_str())
            {
                field_map.insert(name.to_lowercase(), (name.to_string(), sf_type.to_string()));
            }
        }
    }

    if field_map.is_empty() {
        return Err(anyhow::anyhow!("No fields found for table {}", table_name));
    }

    let mut field_types = Vec::new();

    match requested_fields {
        Some(requested) => {
            for req in requested {
                let req_lower = req.trim().to_lowercase();
                let Some((name, sf_type)) = field_map.get(&req_lower) else {
                    return Err(anyhow::anyhow!(
                        "Field '{}' not found on object '{}'",
                        req,
                        table_name
                    ));
                };
                let Some(arrow_type) = salesforce_type_to_arrow(sf_type) else {
                    return Err(anyhow::anyhow!(
                        "Field '{}' has unsupported Salesforce type '{}'",
                        name,
                        sf_type
                    ));
                };
                field_types.push((name.clone(), arrow_type));
            }
        }
        None => {
            for (_, (name, sf_type)) in field_map {
                let Some(arrow_type) = salesforce_type_to_arrow(&sf_type) else {
                    return Err(anyhow::anyhow!(
                        "Field '{}' has unsupported Salesforce type '{}'. \
                         Use --fields to select only supported fields.",
                        name,
                        sf_type
                    ));
                };
                field_types.push((name, arrow_type));
            }
        }
    }

    if field_types.is_empty() {
        return Err(anyhow::anyhow!("No fields to export for table {}", table_name));
    }

    Ok(field_types)
}

/// Fetches all records from a Salesforce table and converts to Arrow RecordBatch.
async fn fetch_table_as_arrow(
    sf: &Cirrus,
    table_name: &str,
    limit: Option<usize>,
    requested_fields: Option<Vec<String>>,
) -> Result<RecordBatch> {
    let start = Instant::now();

    let field_types = resolve_export_fields(sf, table_name, requested_fields).await?;
    let field_names: Vec<String> = field_types.iter().map(|(name, _)| name.clone()).collect();

    let fields_str = field_names.join(", ");
    let query = if let Some(l) = limit {
        format!("SELECT {} FROM {} LIMIT {}", fields_str, table_name, l)
    } else {
        format!("SELECT {} FROM {}", fields_str, table_name)
    };

    println!("Querying {} fields...", field_names.len());
    let query_start = Instant::now();

    let mut stream = sf.query_stream(&query);
    let mut records: Vec<Value> = Vec::new();
    let mut last_report = Instant::now();

    while let Some(result) = stream.next().await {
        match result {
            Ok(record) => {
                records.push(record);

                if last_report.elapsed().as_secs() >= 5 {
                    println!(
                        "  ... fetched {} records in {:?}",
                        records.len(),
                        query_start.elapsed()
                    );
                    last_report = Instant::now();
                }
            }
            Err(e) => eprintln!("Error fetching record: {}", e),
        }
    }

    println!("Fetched {} records in {:?}", records.len(), query_start.elapsed());

    let batch = json_to_arrow(&field_types, &records)?;
    println!("Converted to Arrow in {:?}", start.elapsed());

    Ok(batch)
}

/// Maps Salesforce field types to Arrow DataTypes.
fn salesforce_type_to_arrow(sf_type: &str) -> Option<DataType> {
    match sf_type {
        "id" | "string" | "textarea" | "url" | "email" | "phone" | "picklist"
        | "multipicklist" | "combobox" | "reference" | "encryptedstring" => Some(DataType::Utf8),
        "int" => Some(DataType::Int64),
        "double" | "currency" | "percent" => Some(DataType::Float64),
        "boolean" => Some(DataType::Boolean),
        "datetime" => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "date" => Some(DataType::Date32),
        "time" => Some(DataType::Time64(TimeUnit::Microsecond)),
        _ => None,
    }
}

/// Converts JSON records to Arrow RecordBatch.
fn json_to_arrow(field_types: &[(String, DataType)], records: &[Value]) -> Result<RecordBatch> {
    if records.is_empty() {
        let fields: Vec<Field> = field_types
            .iter()
            .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = field_types
            .iter()
            .map(|(_, dtype)| empty_array(dtype))
            .collect();
        return Ok(RecordBatch::try_new(schema, arrays)?);
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (field_name, data_type) in field_types {
        let array = build_arrow_array(field_name, data_type, records)?;
        arrays.push(array);
    }

    let fields: Vec<Field> = field_types
        .iter()
        .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Builds an Arrow array for a specific field from JSON records.
fn build_arrow_array(
    field_name: &str,
    data_type: &DataType,
    records: &[Value],
) -> Result<ArrayRef> {
    match data_type {
        DataType::Utf8 => {
            let values: Vec<Option<String>> = records
                .iter()
                .map(|r| {
                    r.get(field_name).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            Some(v.as_str().unwrap_or("").to_string())
                        }
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)) as ArrayRef)
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = records
                .iter()
                .map(|r| {
                    r.get(field_name).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_i64()
                        }
                    })
                })
                .collect();
            Ok(Arc::new(arrow::array::Int64Array::from(values)) as ArrayRef)
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = records
                .iter()
                .map(|r| {
                    r.get(field_name).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_f64()
                        }
                    })
                })
                .collect();
            Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
        }
        DataType::Boolean => {
            let values: Vec<Option<bool>> = records
                .iter()
                .map(|r| {
                    r.get(field_name).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_bool()
                        }
                    })
                })
                .collect();
            Ok(Arc::new(BooleanArray::from(values)) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values: Vec<Option<i64>> = records
                .iter()
                .map(|r| {
                    r.get(field_name).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_str().and_then(|s| {
                                s.parse::<jiff::Timestamp>()
                                    .ok()
                                    .map(|t| t.as_microsecond())
                            })
                        }
                    })
                })
                .collect();
            Ok(Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef)
        }
        DataType::Date32 => {
            let values: Vec<Option<i32>> = records
                .iter()
                .map(|r| {
                    r.get(field_name).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_str().and_then(|s| {
                                jiff::civil::Date::strptime("%Y-%m-%d", s)
                                    .ok()
                                    .map(|d| {
                                        let epoch = jiff::civil::date(1970, 1, 1);
                                        (d.duration_since(epoch).as_secs() / 86_400) as i32
                                    })
                            })
                        }
                    })
                })
                .collect();
            Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let values: Vec<Option<i64>> = records
                .iter()
                .map(|r| {
                    r.get(field_name).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            v.as_str().and_then(|s| {
                                s.parse::<jiff::civil::Time>()
                                    .ok()
                                    .map(|t| {
                                        t.duration_since(jiff::civil::Time::midnight())
                                            .as_micros() as i64
                                    })
                            })
                        }
                    })
                })
                .collect();
            Ok(Arc::new(Time64MicrosecondArray::from(values)) as ArrayRef)
        }
        _ => {
            let values: Vec<Option<String>> = records
                .iter()
                .map(|r| {
                    r.get(field_name).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            Some(v.to_string())
                        }
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)) as ArrayRef)
        }
    }
}

/// Creates an empty Arrow array for a given data type.
fn empty_array(data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Utf8 => Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        DataType::Int64 => Arc::new(arrow::array::Int64Array::from(Vec::<Option<i64>>::new())),
        DataType::Float64 => Arc::new(Float64Array::from(Vec::<Option<f64>>::new())),
        DataType::Boolean => Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from(Vec::<Option<i64>>::new()))
        }
        DataType::Date32 => Arc::new(Date32Array::from(Vec::<Option<i32>>::new())),
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(Vec::<Option<i64>>::new()))
        }
        _ => Arc::new(StringArray::from(Vec::<Option<String>>::new())),
    }
}

/// Resolves the output path for a Parquet export.
///
/// Rules:
/// - If `output` is a file path (has extension), use it as-is.
/// - If `output` is a directory or has a trailing separator, write `{clean_object}.parquet` inside it.
/// - If `output` is None, default to `{clean_object}.parquet` in the current directory.
///
/// The default filename strips the `__c` suffix from custom objects and lowercases everything.
fn resolve_output_path(table_name: &str, output: Option<PathBuf>) -> Result<PathBuf> {
    let clean_name = table_name.trim_end_matches("__c").to_lowercase();
    let default_filename = format!("{clean_name}.parquet");

    let Some(path) = output else {
        return Ok(PathBuf::from(default_filename));
    };

    if path.extension().is_some() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        return Ok(path);
    }

    std::fs::create_dir_all(&path)?;
    Ok(path.join(default_filename))
}

/// Writes a RecordBatch to a Parquet file.
fn write_to_parquet(batch: &RecordBatch, path: &std::path::Path) -> Result<()> {
    let file = File::create(path)?;

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    Ok(())
}
