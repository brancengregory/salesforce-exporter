# salesforce-exporter

Bulk export of Salesforce data to Parquet.

## Build

```sh
cargo build --release
```

## Usage

```sh
# List visible Salesforce objects
salesforce-exporter list

# Describe an object's fields
salesforce-exporter describe Account

# Export an object to Parquet
salesforce-exporter export Account --output account.parquet

# Batch export via config file
salesforce-exporter export --config export.toml
```

## License

See [LICENSE](LICENSE).
