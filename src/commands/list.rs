use anyhow::Result;
use cirrus::Cirrus;

/// List all Salesforce objects visible to the authenticated user.
pub async fn run(sf: &Cirrus) -> Result<()> {
    let dg = sf.sobjects().describe_global().await?;

    let objects: Vec<serde_json::Value> = dg
        .sobjects
        .into_iter()
        .map(|obj| {
            serde_json::json!({
                "name": obj.name,
                "label": obj.label,
                "label_plural": obj.label_plural,
                "queryable": obj.queryable,
                "custom": obj.custom,
                "createable": obj.createable,
                "updateable": obj.updateable,
                "deletable": obj.deletable,
                "key_prefix": obj.key_prefix,
            })
        })
        .collect();

    println!("{}", serde_json::to_string_pretty(&objects)?);
    Ok(())
}
