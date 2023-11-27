use anyhow::Result;
use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, env};
use struct_iterable::Iterable;

pub struct Equities {
    client: reqwest::Client,
    api_token: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Iterable)]
pub struct OhlcvMetadata {
    pub data_type: String,
    pub isin: String,
    pub ticker: String,
    pub source: String,
    pub exchange: String,
}

impl Equities {
    pub async fn new() -> Self {
        dotenv().ok();
        let eod_api_token = env::var("API_TOKEN").expect("Failed tp parse API_TOKEN from .env");
        Self {
            client: reqwest::Client::new(),
            api_token: eod_api_token,
        }
    }

    pub async fn get_ticker_general(
        &self,
        ticker: &str,
        exchange: &str,
    ) -> Result<HashMap<String, Value>> {
        let client = &self.client;
        let api_token = &self.api_token;
        let param = vec![
            ("api_token", api_token.as_str()),
            ("fmt", "json"),
            ("filter", "General"),
        ];

        let response_string: String = client
            .get(format!(
                "https://eodhd.com/api/fundamentals/{}.{}",
                ticker, exchange
            ))
            .query(&param)
            .send()
            .await?
            .text()
            .await?;

        let response_hashmap: HashMap<String, Value> = serde_json::from_str(&response_string)
            .expect("get_ticker_general() Failed to deserialise response_text");
        
        Ok(response_hashmap)
    }

    pub async fn get_series_metadata(&self, ticker: &str, exchange: &str) -> Result<OhlcvMetadata> {
        let ticker_general = self
            .get_ticker_general(ticker, exchange)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "get_metadata_info() failed on get_ticker_general() for {:?}",
                    ticker
                )
            });

        let isin_value = ticker_general
            .get("ISIN")
            .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
            .unwrap_or_else(|| {
                eprintln!(
                    "get_metadata_info() failed to retrieve ISIN for ticker: {:?}",
                    ticker
                );
                ""
            })
            .trim_matches('"')
            .to_string();

        let exchange_value = ticker_general
            .get("Exchange")
            .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
            .unwrap_or_else(|| {
                eprintln!(
                    "get_metadata_info() failed to retrieve exchange for ticker: {:?}",
                    ticker
                );
                ""
            })
            .trim_matches('"')
            .to_string();

        let series_metadata = OhlcvMetadata {
            data_type: "ticker_series".to_string(),
            isin: isin_value,
            ticker: ticker.to_string(),
            source: "eod".to_string(),
            exchange: exchange_value,
        };

        Ok(series_metadata)
    }
}