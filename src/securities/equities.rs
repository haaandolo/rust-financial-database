use anyhow::Result;
use dotenv::dotenv;
use polars::prelude::*;
use reqwest::Client;
use serde_json::{to_string, Value};
use std::{collections::HashMap, env};

use crate::models::OhlcvMetadata;
use crate::wrappers::WrapperFunctions;

#[derive(Debug, Clone)]
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

    pub async fn batch_get_ohlcv_equities(
        &self,
        tickers_exchanges: Vec<(&str, &str)>,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<DataFrame>> {
        let wrapper_batch_client = WrapperFunctions::new().await;
        let metadata_vec = self.clone().batch_get_metadata_info(&tickers_exchanges).await?;
        let dfs = wrapper_batch_client
            .batch_get_ohlcv(tickers_exchanges, start_date, end_date)
            .await?;

        let mut dfs_clean = Vec::new();
        for (mut df, metadata) in dfs.into_iter().zip(metadata_vec.into_iter()) {
            // CHANGE "to_string(&metadata)?" TO HASHMAP IF WE NEED FOR DATABASE READS
            let series = Series::new("metadata", vec![to_string(&metadata)?; df.height()]);
            df.with_column(series)?;
            dfs_clean.push(df)
        }

        Ok(dfs_clean)
    }

    pub async fn batch_get_intraday_data_equities(
        &self,
        tickers_exchanges: Vec<(&str, &str)>,
        start_date: &str,
        end_date: &str,
        interval: &str,
    ) -> Result<Vec<DataFrame>> {
        let wrapper_batch_client = WrapperFunctions::new().await;
        let metadata_vec = self.clone().batch_get_metadata_info(&tickers_exchanges).await?;
        let dfs = wrapper_batch_client
            .batch_get_intraday_data(tickers_exchanges, start_date, end_date, interval)
            .await?;

        let mut dfs_clean = Vec::new();
        for (mut df, metadata) in dfs.into_iter().zip(metadata_vec.into_iter()) {
            // CHANGE "to_string(&metadata)?" TO HASHMAP IF WE NEED FOR DATABASE READS
            let series = Series::new("metadata", vec![to_string(&metadata)?; df.height()]);
            df.with_column(series)?;
            dfs_clean.push(df)
        }

        Ok(dfs_clean)
    }

    pub async fn batch_get_live_lagged_data_equity(
        &self,
        tickers_exchanges: Vec<(&str, &str)>,
    ) -> Result<Vec<DataFrame>> {
        let wrapper_batch_client = WrapperFunctions::new().await;
        let metadata_vec = self.clone().batch_get_metadata_info(&tickers_exchanges).await?;
        let dfs = wrapper_batch_client
            .batch_get_live_lagged_data(tickers_exchanges)
            .await?;

        let mut dfs_clean = Vec::new();
        for (mut df, metadata) in dfs.into_iter().zip(metadata_vec.into_iter()) {
            // CHANGE "to_string(&metadata)?" TO HASHMAP IF WE NEED FOR DATABASE READS
            let series = Series::new("metadata", vec![to_string(&metadata)?; df.height()]);
            df.with_column(series)?;
            dfs_clean.push(df)
        }

        Ok(dfs_clean)
    }

    /*-------------------- FUNDAMENTAL DATA -------------------- */

    pub async fn batch_get_fundamental_data_equities(
        &self,
        tickers_exchanges: Vec<(&str, &str)>,
    ) -> Result<Vec<DataFrame>> {
        let wrapper_batch_client = WrapperFunctions::new().await;
        let dfs_fundamentals = wrapper_batch_client
            .batch_get_fundamental_data(tickers_exchanges)
            .await?;

        Ok(dfs_fundamentals)
    }

    pub async fn batch_get_ticker_generals(
        &self,
        tickers_exchanges: &[(&str, &str)],
    ) -> Result<Vec<DataFrame>> {
        let wrapper_batch_client = WrapperFunctions::new().await;
        let mut urls = Vec::new();
        tickers_exchanges.iter()
            .for_each(|ticker_exchange| {
                let url = format!(
                    "https://eodhistoricaldata.com/api/fundamentals/{}.{}?api_token={}&fmt=json&filter=General",
                    ticker_exchange.0,
                    ticker_exchange.1,
                    self.api_token
                );
                urls.push(url);
            });

        let response_vec_dfs = wrapper_batch_client.async_http_request(urls).await?;

        Ok(response_vec_dfs)
    }

    pub async fn batch_get_metadata_info(
        self,
        tickers_exchanges: &[(&str, &str)],
    ) -> Result<Vec<OhlcvMetadata>> {
        let ticker_generals = self.batch_get_ticker_generals(tickers_exchanges).await?;
        let mut series_metadata_vec = Vec::new();
        ticker_generals.iter().zip(tickers_exchanges.iter()).for_each(|df_ticker_exchange| {
            let isin_value = df_ticker_exchange.0
                .column("ISIN")
                .expect("batch_get_series_metadata() failed to unwrap series from Result<&Series, PolarError>")
                .get(0)
                .expect("batch_get_series_metadata() failed to unwrap get value from Series")
                .to_string()
                .trim_matches('\"')
                .to_string();

            let series_metadata = OhlcvMetadata {
                data_type: "ticker_series".to_string(),
                isin: isin_value,
                ticker: df_ticker_exchange.1.0.to_string(),
                source: "eod".to_string(),
                exchange: df_ticker_exchange.1.1.to_string()
            };

            series_metadata_vec.push(series_metadata);            
        });

        Ok(series_metadata_vec)
    }

    /**---------------------------------------------------------------------------------------------- */

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

    pub async fn batch_get_metadata_info2(
        &self,
        tickers_exchanges: &[(&str, &str)],
    ) -> Result<Vec<OhlcvMetadata>> {
        let mut series_metadata: Vec<OhlcvMetadata> = Vec::new();

        for ticker_exchange in tickers_exchanges.iter() {
            let metadata = self
                .get_series_metadata(ticker_exchange.0, ticker_exchange.1)
                .await?;
            series_metadata.push(metadata);
        }
        Ok(series_metadata)
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
                "https://eodhistoricaldata.com/api/fundamentals/{}.{}",
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
}
