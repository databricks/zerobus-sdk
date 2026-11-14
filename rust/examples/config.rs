use std::{env, error::Error};

/// Connection settings shared by every example, read from the environment.
pub struct ExampleConfig {
    pub server_endpoint: String,
    pub workspace_url: String,
    pub table_name: String,
    pub client_id: String,
    pub client_secret: String,
}

impl ExampleConfig {
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            server_endpoint: required("ZEROBUS_SERVER_ENDPOINT")?,
            workspace_url: required("DATABRICKS_WORKSPACE_URL")?,
            table_name: required("ZEROBUS_TABLE_NAME")?,
            client_id: required("DATABRICKS_CLIENT_ID")?,
            client_secret: required("DATABRICKS_CLIENT_SECRET")?,
        })
    }
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) => Err(format!("{name} is empty; see rust/examples/README.md").into()),
        Err(error) => Err(format!("{name}: {error}; see rust/examples/README.md").into()),
    }
}
