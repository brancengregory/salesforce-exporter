use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct ExportSpec {
    #[serde(default)]
    pub output_dir: Option<PathBuf>,
    #[serde(default)]
    pub limit: Option<usize>,
    pub export: Vec<ExportItem>,
}

#[derive(Debug, Deserialize)]
pub struct ExportItem {
    pub object: String,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub output: Option<PathBuf>,
}

pub fn load_config(path: &PathBuf) -> anyhow::Result<ExportSpec> {
    let content = std::fs::read_to_string(path)?;
    let spec: ExportSpec = toml::from_str(&content)?;
    Ok(spec)
}
