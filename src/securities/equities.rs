use anyhow::Result;
use dotenv::dotenv;
use polars::prelude::*;
use reqwest::Client;
use serde_json::{to_string, Value};
use std::{collections::HashMap, env};

use crate::models::OhlcvMetadata;
use crate::wrappers::WrapperFunctions;

pub struct Equities {
    client: Client,
    api_token: String,
}

impl Equities {
    pub async fn new() -> Self {
        dotenv().ok();
        let eod_api_token = env::var("API_TOKEN").expect("Failed tp parse API_TOKEN from .env");
        Self {
            client: Client::new(),
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
                    "get_metadata_info() failed on get_ticker_general() for {}",
                    ticker
                )
            });

        let isin_value = ticker_general
            .get("ISIN")
            .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
            .unwrap_or_else(|| {
                eprintln!(
                    "get_metadata_info() failed to retrieve ISIN for ticker: {}",
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
            exchange: exchange.to_string(),
        };

        Ok(series_metadata)
    }

    pub async fn batch_get_metadata_info(
        &self,
        tickers: &[&str],
        exchanges: &[&str],
    ) -> Result<Vec<OhlcvMetadata>> {
        let mut series_metadata: Vec<OhlcvMetadata> = Vec::new();
        for (ticker, exchange) in tickers.iter().zip(exchanges.iter()) {
            let metadata = self.get_series_metadata(ticker, exchange).await?;
            series_metadata.push(metadata);
        }
        Ok(series_metadata)
    }

    pub async fn batch_get_ohlcv_equities(
        &self,
        tickers: Vec<&str>,
        exchanges: Vec<&str>,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<DataFrame>> {
        let wrapper_batch_client = WrapperFunctions::new().await;
        let metadata_vec = self.batch_get_metadata_info(&tickers, &exchanges).await?;
        let dfs = wrapper_batch_client
            .batch_get_ohlcv(tickers, exchanges, start_date, end_date)
            .await?;

        let mut dfs_clean = Vec::new();
        for (mut df, metadata) in dfs.into_iter().zip(metadata_vec.into_iter()) {
            let series = Series::new("metadata", vec![to_string(&metadata)?; df.height()]);
            df.with_column(series)
                .expect("Could not add metadata to dataframe");
            dfs_clean.push(df)
        }

        Ok(dfs_clean)
    }
}
