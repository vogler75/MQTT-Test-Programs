use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use rumqttc::{MqttOptions, Transport};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub broker_host: String,
    pub broker_port: u16,
    pub num_producers: usize,
    pub num_topics: usize,
    pub topics_per_node: usize,
    pub max_depth: usize,
    pub sleep_ms: u64,
    pub qos: i32,
    pub retained: bool,
    pub topic_prefix: String,
    pub subscribe_percentage: u8,
    #[serde(default)]
    pub use_wildcard: bool,
    #[serde(default)]
    pub use_websocket: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            broker_host: "localhost".to_string(),
            broker_port: 1883,
            num_producers: 10,
            num_topics: 100,
            topics_per_node: 10,
            max_depth: 3,
            sleep_ms: 100,
            qos: 0,
            retained: false,
            topic_prefix: "test".to_string(),
            subscribe_percentage: 100,
            use_wildcard: false,
            use_websocket: false,
        }
    }
}

impl Config {
    pub fn save(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string_pretty(self)?;
        fs::write(path, json)?;
        Ok(())
    }

    pub fn load(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
        if !Path::new(path).exists() {
            return Err(format!("Config file not found: {}", path).into());
        }
        let json = fs::read_to_string(path)?;
        let config = serde_json::from_str(&json)?;
        Ok(config)
    }

    pub fn load_or_default(path: Option<&str>) -> Self {
        match path {
            Some(p) => Self::load(p).unwrap_or_default(),
            None => Self::default(),
        }
    }

    pub fn create_mqtt_options(&self, client_id: String) -> MqttOptions {
        let (host, port) = if self.use_websocket {
            // For WebSocket, pass the full URL as the host parameter
            (format!("ws://{}:{}/mqtt", self.broker_host, self.broker_port), self.broker_port)
        } else {
            (self.broker_host.clone(), self.broker_port)
        };

        let mut options = MqttOptions::new(client_id, &host, port);

        if self.use_websocket {
            options.set_transport(Transport::ws());
        }

        options
    }
}
