use anyhow::Result;
use cirrus::Cirrus;

/// Describe a single Salesforce object and print its field metadata as JSON.
pub async fn run(sf: &Cirrus, object: &str) -> Result<()> {
    let describe = sf.sobject(object).describe().await?;
    println!("{}", serde_json::to_string_pretty(&describe)?);
    Ok(())
}
