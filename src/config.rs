use {
    anyhow::Result,
    ethrpc::types::{ArrayVec, LogFilterValue},
    serde::Deserialize,
    solabi::{
        abi::EventDescriptor,
        ethprim::{Address, Digest},
    },
    std::{
        fmt::{self, Debug, Formatter},
        fs,
        path::{Path, PathBuf},
        time::Duration,
    },
    url::Url,
};

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub ethrpc: Url,
    pub database: Database,
    #[serde(default = "indexer::default")]
    pub indexer: Indexer,
    #[serde(rename = "event")]
    pub events: Vec<Event>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Database {
    Sqlite { connection: String },
    Postgres { connection: String },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Indexer {
    #[serde(default = "indexer::default_page_size")]
    pub page_size: u64,
    #[serde(default = "indexer::default_poll_interval", with = "duration")]
    pub poll_interval: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Event {
    pub name: String,
    #[serde(default)]
    pub start: u64,
    pub contract: Contract,
    #[serde(default)]
    pub topics: ArrayVec<LogFilterValue<Digest>, 3>,
    #[serde(with = "signature")]
    pub signature: EventDescriptor,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum Contract {
    #[serde(with = "contract")]
    All,
    Address(Address),
}

impl Config {
    /// Reads a configuration from the specified path, returning the parsed
    /// configuration and its root path.
    pub fn load(
        path: &Path,
        node_url: Option<String>,
        db_url: Option<String>,
    ) -> Result<(Self, PathBuf)> {
        let toml = manual_override(fs::read_to_string(path)?, node_url, db_url);
        let config = toml::from_str(&toml)?;
        let root = fs::canonicalize(path)?
            .parent()
            .expect("file path without a parent")
            .to_owned();
        Ok((config, root))
    }
}

fn manual_override(mut toml: String, node_url: Option<String>, db_url: Option<String>) -> String {
    // override for node_url
    if let Some(ethrpc) = node_url {
        tracing::info!("using env NODE_URL");
        // comment any existing config
        toml = toml.replace("ethrpc", "#ethrpc");
        // Insert the new one at the top
        toml = format!("ethrpc = \"{ethrpc}\"\n") + &toml;
    }
    // override for db_url
    if let Some(connection) = db_url {
        tracing::info!("using env DB_URL");
        // comment any existing config
        toml = toml.replace("[database.", "#[database.");
        let db_type = if connection.contains("file:") {
            "sqlite"
        } else {
            "postgres"
        };
        // Append the new one at the bottom.
        toml = toml + &format!("\n[database.{db_type}]\nconnection = \"{connection}\"");
    }
    toml
}

impl Debug for Config {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Config")
            .field("ethrpc", &self.ethrpc.as_str())
            .field("database", &self.database)
            .field("indexer", &self.indexer)
            .field("event", &self.events)
            .finish()
    }
}

mod signature {
    use {
        serde::{de, Deserialize, Deserializer},
        solabi::abi::EventDescriptor,
        std::borrow::Cow,
    };

    pub fn deserialize<'de, D>(deserializer: D) -> Result<EventDescriptor, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = Cow::<str>::deserialize(deserializer)?;
        EventDescriptor::parse_declaration(s.as_ref()).map_err(de::Error::custom)
    }
}

mod contract {
    use {
        serde::{de, Deserialize, Deserializer},
        std::borrow::Cow,
    };

    pub fn deserialize<'de, D>(deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = Cow::<str>::deserialize(deserializer)?;
        (s == "*")
            .then_some(())
            .ok_or_else(|| de::Error::custom("expected '*' string"))
    }
}

mod indexer {
    use {super::Indexer, std::time::Duration};

    pub fn default() -> Indexer {
        Indexer {
            page_size: default_page_size(),
            poll_interval: default_poll_interval(),
        }
    }

    pub fn default_page_size() -> u64 {
        1000
    }

    pub fn default_poll_interval() -> Duration {
        Duration::from_secs_f64(0.1)
    }
}

mod duration {
    use {
        serde::{Deserialize, Deserializer},
        std::time::Duration,
    };

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = f64::deserialize(deserializer)?;
        Ok(Duration::from_secs_f64(secs))
    }
}

impl Event {
    #[cfg(test)]
    pub fn for_signature(signature: &str) -> Self {
        let signature = EventDescriptor::parse_declaration(signature).unwrap();
        Self {
            name: signature.name.clone(),
            start: 0,
            contract: Contract::All,
            topics: ArrayVec::new(),
            signature,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manual_override() {
        // Sample contains both ethrpc and database.sqlite config
        let sample_toml = fs::read_to_string("arak.example.toml").unwrap();
        let sample_config: Config = toml::from_str(&sample_toml).unwrap();
        let sample_node_url = Url::parse("http://localhost:8545").unwrap();
        assert_eq!(sample_config.ethrpc, sample_node_url);
        match sample_config.database {
            Database::Postgres { .. } => panic!("Expected sqlite"),
            Database::Sqlite { .. } => (),
        }

        // Manual (Env) Values
        let node_url = Some("https://url.com".to_string());
        let db_url = Some("postgres://DB_URL".to_string());

        // override both
        let config: Config = toml::from_str(&manual_override(
            sample_toml.clone(),
            node_url.clone(),
            db_url.clone(),
        ))
        .unwrap();
        assert_eq!(
            config.ethrpc,
            Url::parse(&node_url.clone().unwrap()).unwrap()
        );
        match config.database {
            Database::Postgres { .. } => (),
            Database::Sqlite { .. } => panic!("Expected postgres"),
        }

        // only node_url
        let config: Config = toml::from_str(&manual_override(
            sample_toml.clone(),
            node_url.clone(),
            None,
        ))
        .unwrap();
        assert_eq!(
            config.ethrpc,
            Url::parse(&node_url.clone().unwrap()).unwrap()
        );
        match config.database {
            Database::Postgres { .. } => panic!("Expected sqlite"),
            Database::Sqlite { .. } => (),
        }

        // only db_url
        let config: Config =
            toml::from_str(&manual_override(sample_toml.clone(), None, db_url.clone())).unwrap();
        assert_eq!(config.ethrpc, sample_node_url);
        match config.database {
            Database::Postgres { .. } => (),
            Database::Sqlite { .. } => panic!("Expected postgres"),
        }

        // toml without node or db provided
        let my_toml = r#"
            [[event]]
            name = "crypto_kitty_mints"
            start = 4605160
            contract = "0x06012c8cf97bead5deae237070f9587f8e7a266d"
            topics = [
                "0x0000000000000000000000000000000000000000000000000000000000000000"
            ]
            signature = "event Transfer (address from, address to, uint256 tokenId)""#;
        assert!(
            toml::from_str::<Config>(&manual_override(my_toml.to_string(), node_url, db_url))
                .is_ok()
        );
    }
}
