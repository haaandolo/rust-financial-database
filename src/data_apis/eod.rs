use anyhow::Result;
use dotenv::dotenv;
use polars::frame::DataFrame;
use polars::prelude::*;
use reqwest::Client;
use serde_json::to_string;
use std::{collections::HashMap, env};

use crate::models::eod_models::{MongoTickerParams, OhlcvMetaData};
use crate::utility_functions::{
    async_http_request, get_current_date_string, get_current_datetime_bson, get_timestamps_tuple,
};

pub struct EodApi {
    client: Client,
    api_token: String,
}

impl EodApi {
    pub async fn new() -> Self {
        dotenv().ok();
        let eod_api_token = env::var("API_TOKEN").expect("Failed tp parse API_TOKEN from .env");
        log::info!("Established Client for EOD API");
        Self {
            client: Client::new(),
            api_token: eod_api_token,
        }
    }

    fn get_metadata(&self, ticker: MongoTickerParams) -> Result<OhlcvMetaData> {
        match ticker.exchange.as_str() {
            "COM" => {
                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    currency: None,
                };
                log::info!(
                    "batch_get_metadata() retrieving metadata for: {}",
                    ticker.ticker
                );
                Ok(series_metadata)
            }
            "CC" => {
                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    currency: None,
                };
                log::info!(
                    "batch_get_metadata() retrieving metadata for: {}",
                    ticker.ticker
                );
                Ok(series_metadata)
            }
            "BOND" => {
                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    currency: None,
                };
                log::info!(
                    "batch_get_metadata() retrieving metadata for: {}",
                    ticker.ticker
                );
                Ok(series_metadata)
            }
            "FOREX" => {
                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    currency: None,
                };
                log::info!(
                    "batch_get_metadata() retrieving metadata for: {}",
                    ticker.ticker
                );
                Ok(series_metadata)
            }
            _ => {
                // let response_string = self.client
                //     .get(format!(
                //         "https://eodhistoricaldata.com/api/fundamentals/{}.{}?api_token={}&fmt=json&filter=General",
                //         ticker.ticker, ticker.exchange, self.api_token,
                //     ))
                //     .send()
                //     .await
                //     .unwrap_or_else(|_| panic!("batch_get_metadata() failed to unwrap response {}", ticker.ticker))
                //     .text()
                //     .await
                //     .unwrap_or_else(|_| panic!("batch_get_metadata() failed to unwrap response {}", ticker.ticker));

                // let response_hashmap: HashMap<String, Value> =
                //     serde_json::from_str(&response_string)
                //         .expect("batch_get_metadata() Failed to deserialise response_text");

                // let ticker_denomination = response_hashmap
                //     .get("CurrencyCode")
                //     .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
                //     .unwrap_or_else(|| {
                //         eprintln!(
                //             "batch_get_metadata() failed to retrieve currecny for ticker: {}",
                //             ticker.ticker
                //         );
                //         ""
                //     })
                //     .trim_matches('"')
                //     .to_string();

                // let series_metadata = OhlcvMetaData {
                //     metadata_collection_name: ticker.series_collection_name.to_string(),
                //     ticker: ticker.ticker.to_string(),
                //     source: "eod".to_string(),
                //     exchange: ticker.exchange.to_string(),
                //     currency: Some(ticker_denomination),
                // };

                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    currency: None,
                };
                log::info!(
                    "batch_get_metadata() retrieving metadata for: {}",
                    ticker.ticker
                );
                Ok(series_metadata)
            }
        }
    }

    pub async fn batch_get_series_all(
        &self,
        tickers: Vec<MongoTickerParams>,
    ) -> Result<Vec<DataFrame>> {
        let mut urls = Vec::new();
        for ticker in tickers.clone().into_iter() {
            let end_date_string = get_current_date_string();
            let granularity = ticker.series_collection_name.chars().last();
            log::info!(
                "batch_get_series_all() making url for: {} belonging to collection {}",
                ticker.ticker,
                ticker.series_collection_name
            );
            match granularity {
                Some('d') => {
                    let from_date = &ticker.from.to_string()[..10];
                    let url = format!(
                        "https://eodhistoricaldata.com/api/eod/{}.{}?api_token={}&fmt=json&from={}&to={}",
                        ticker.ticker, ticker.exchange, self.api_token, from_date, end_date_string
                    );
                    urls.push((ticker, url));
                }
                Some('h') | Some('m') => {
                    let end_date_datetime = get_current_datetime_bson();
                    let collection_name_split = &ticker
                        .series_collection_name
                        .split('_')
                        .collect::<Vec<&str>>();
                    let interval = collection_name_split.last().expect(
                        "batch_get_series_all() failed to get interval from collection_name",
                    );
                    let timestamps_tuple =
                        get_timestamps_tuple(ticker.from, end_date_datetime, interval)?;
                    for (from, to) in timestamps_tuple.iter() {
                        let url = format!(
                            "https://eodhistoricaldata.com/api/intraday/{}.{}?api_token={}&interval={}&fmt=json&from={}&to={}",
                            ticker.ticker, &ticker.exchange, self.api_token, interval, from, to
                        );
                        urls.push((ticker.clone(), url));
                    }
                }
                Some('e') => {
                    let url = format!(
                        "https://eodhistoricaldata.com/api/real-time/{}.{}?api_token={}&fmt=json",
                        ticker.ticker, ticker.exchange, self.api_token,
                    );
                    urls.push((ticker, url));
                }
                _ => log::error!(
                    "batch_get_series_all() Could not parse granularity for: {}",
                    ticker.ticker
                ),
            }
        }

        let mut urls_unique = HashMap::new();
        for (params, url) in urls.into_iter() {
            urls_unique.entry(url).or_insert(params);
        }

        let dfs = async_http_request(self.client.clone(), urls_unique).await?;

        let mut dfs_with_metadata = Vec::new();
        for (param, mut df) in dfs.into_iter() {
            let metadata = self.get_metadata(param)?;
            let series = Series::new("metadata", vec![to_string(&metadata)?; df.height()]);
            df.with_column(series)?;
            dfs_with_metadata.push(df);
        }
        log::info!("batch_get_series_all() successfully retrieved all dataframes from EOD");
        Ok(dfs_with_metadata)
    }
}
