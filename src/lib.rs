use bytes::Bytes;
use md5::{Digest, Md5};
use reqwest::{Client, Error};
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::Mutex;

/// A `Nacos` service without authentication.
pub struct Nacos {
    use_https: bool,
    server_addr: SocketAddr,
    namespace: Option<String>,
    group: String,
    /// Data id to md5.
    current_config: Mutex<HashMap<String, String>>,
    client: Client,
}

impl Nacos {
    pub fn new(
        use_https: bool,
        server_addr: SocketAddr,
        namespace: Option<String>,
        group: String,
    ) -> Self {
        Self {
            use_https,
            server_addr,
            namespace,
            group,
            current_config: Default::default(),
            client: Client::new(),
        }
    }

    pub async fn wait_for_new_config(&self, data_id: &str) -> Result<Bytes, Error> {
        if !self.current_config.lock().await.contains_key(data_id) {
            // New config that we never saw. Get it from server.
            let config = self.get_config(data_id).await?;
            self.update_md5(data_id, &config).await;
            Ok(config)
        } else {
            loop {
                let md5 = self
                    .current_config
                    .lock()
                    .await
                    .get(data_id)
                    .unwrap()
                    .clone();
                let mut listening_configs = data_id.to_string();
                listening_configs.push(2 as char);
                listening_configs.push_str(&self.group);
                listening_configs.push(2 as char);
                listening_configs.push_str(&md5);
                if let Some(namespace) = &self.namespace {
                    listening_configs.push(2 as char);
                    listening_configs.push_str(namespace);
                }
                listening_configs.push(1 as char);

                let url = self.make_url("/nacos/v1/cs/configs/listener");
                let request = self.client.post(url);
                let request = request.header("Long-Pulling-Timeout", "30000");
                let request = request.query(&[("Listening-Configs", &listening_configs)]);

                let response = request.send().await?;
                let response = response.error_for_status()?;
                let config = response.bytes().await?;
                if config.is_empty() {
                    log::debug!("No new config for {}", data_id);
                } else {
                    self.update_md5(data_id, &config).await;
                    return Ok(config);
                }
            }
        }
    }
}

impl Nacos {
    fn make_url(&self, path: &str) -> String {
        format!(
            "{}://{}{}",
            if self.use_https { "https" } else { "http" },
            self.server_addr,
            path
        )
    }

    async fn get_config(&self, data_id: &str) -> Result<Bytes, Error> {
        let url = self.make_url("/nacos/v1/cs/configs");
        let mut request = self.client.get(url);
        if let Some(namespace) = &self.namespace {
            request = request.query(&[("tenant", namespace.as_str())]);
        }
        request = request.query(&[("group", self.group.as_str()), ("dataId", data_id)]);
        let response = request.send().await?;
        let response = response.error_for_status()?;
        response.bytes().await
    }

    async fn update_md5(&self, data_id: &str, config: &Bytes) {
        let mut hasher = Md5::new();
        hasher.update(&config);
        let md5 = hasher.finalize();
        let md5 = hex::encode(md5);

        self.current_config.lock().await.insert(data_id.into(), md5);
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    async fn test(namespace: Option<String>) {
        let nacos = Nacos::new(
            false,
            SocketAddr::from_str("192.168.10.252:8848").unwrap(),
            namespace,
            "DEFAULT_GROUP".into(),
        );
        let data_id = "com.oppentech.ysl-custom-design.renderer";
        let bytes = nacos.wait_for_new_config(data_id).await.unwrap();
        println!("{}", std::str::from_utf8(&bytes).unwrap());
        nacos.wait_for_new_config(data_id).await.unwrap();
    }

    #[tokio::test]
    async fn it_works() {
        test(None).await;
        test(Some("8b0a9030-215e-45f3-9cc2-b53c93229909".into())).await;
    }
}
