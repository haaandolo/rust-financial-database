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
                    isin: None,
                    currency: None,
                };
                Ok(series_metadata)
            }
            "CC" => {
                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    isin: None,
                    currency: None,
                };
                Ok(series_metadata)
            }
            "BOND" => {
                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    isin: None,
                    currency: None,
                };
                Ok(series_metadata)
            }
            "FOREX" => {
                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    isin: None,
                    currency: None,
                };
                Ok(series_metadata)
            }
            _ => {
                // let response_string = self.client
                //         .get(format!(
                //             "https://eodhistoricaldata.com/api/fundamentals/{}.{}?api_token={}&fmt=json&filter=General",
                //             ticker.ticker, ticker.exchange, self.api_token,
                //         ))
                //         .send();
                //         // .await
                //         // .unwrap_or_else(|_| panic!("batch_get_metadata() failed to unwrap response {}", ticker.ticker))
                //         // .text();
                //         // .await
                //         // .unwrap_or_else(|_| panic!("batch_get_metadata() failed to unwrap response {}", ticker.ticker));

                // let response_hashmap: HashMap<String, Value> =
                //     serde_json::from_str(&response_string)
                //         .expect("batch_get_metadata() Failed to deserialise response_text");

                // let isin_value = response_hashmap
                //     .get("ISIN")
                //     .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
                //     .unwrap_or_else(|| {
                //         eprintln!(
                //             "batch_get_metadata() failed to retrieve ISIN for ticker: {}",
                //             ticker.ticker
                //         );
                //         ""
                //     })
                //     .trim_matches('"')
                //     .to_string();

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
                //     isin: Some(isin_value),
                //     currency: Some(ticker_denomination),
                // };

                let series_metadata = OhlcvMetaData {
                    metadata_collection_name: ticker.series_collection_name.to_string(),
                    ticker: ticker.ticker.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.exchange.to_string(),
                    isin: None,
                    currency: None,
                };

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
            let end_date = get_current_date_string();
            let granularity = ticker.series_collection_name.chars().last();
            match granularity {
                Some('d') => {
                    let from_date = &ticker.from.to_string()[..10];
                    let url = format!(
                        "https://eodhistoricaldata.com/api/eod/{}.{}?api_token={}&fmt=json&from={}&to={}",
                        ticker.ticker, ticker.exchange, self.api_token, from_date, end_date
                    );
                    urls.push((ticker, url));
                }
                Some('h') | Some('m') => {
                    let end_date = get_current_datetime_bson();
                    let collection_name_split = &ticker
                        .series_collection_name
                        .split('_')
                        .collect::<Vec<&str>>();
                    let interval = collection_name_split.last().expect(
                        "batch_get_series_all() failed to get interval from collection_name",
                    );
                    let timestamps_tuple = get_timestamps_tuple(ticker.from, end_date, interval)?;
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

        let dfs = async_http_request(self.client.clone(), urls_unique)
            .await
            .expect("batch_get_series_all() failed to get response from async_http_request()");

        let mut dfs_with_metadata = Vec::new();
        for (param, mut df) in dfs.into_iter() {
            let metadata = self.get_metadata(param)?;
            let series = Series::new("metadata", vec![to_string(&metadata)?; df.height()]);
            df.with_column(series)?;
            dfs_with_metadata.push(df);
        }

        Ok(dfs_with_metadata)
    }
}

/*-------------------------------- OLD GET BATCH DATA FUNCTIONS ------------------------------------ */
// pub async fn batch_get_fundamental_data(
//     &self,
//     tickers_exchanges: Vec<(&str, &str)>,
// ) -> Result<Vec<DataFrame>> {
//     let mut urls = Vec::new();
//     tickers_exchanges.iter().for_each(|ticker| {
//         let url = format!(
//             "https://eodhistoricaldata.com/api/fundamentals/{}.{}?api_token={}&fmt=json",
//             ticker.ticker, ticker_exchange.1, self.api_token,
//         );
//         urls.push((ticker, url));
//     });

//     let response_vec_fundamental_data = self.async_http_request(urls).await?;

//     Ok(response_vec_fundamental_data)
// }

// pub async fn batch_get_ohlcv(
// &self,
// tickers_exchanges: Vec<(&str, &str, &str)>, // (ticker, exchange, start_date)
// ) -> Result<Vec<DataFrame>> {
// let end_date = get_current_date_string();
// let mut urls = Vec::new();
// tickers_exchanges.iter().for_each(|ticker| {
// let url = format!(
// "https://eodhistoricaldata.com/api/eod/{}.{}?api_token={}&fmt=json&from={}&to={}",
// ticker.ticker, ticker_exchange.1, self.api_token, ticker_exchange.2, end_date
// );
// urls.push((ticker, url));
// });

// let tickers_exchanges_trunc: Vec<(&str, &str)> = tickers_exchanges
// .iter()
// .map(|ticker| (ticker_exchange.0, ticker_exchange.1))
// .collect();

// let response_vec_api = async_http_request(self.client.clone(), urls).await?;
// let metadata_vec_response = self.batch_get_metadata(&tickers_exchanges_trunc).await?;
// assert_eq!(response_vec_api.len() == metadata_vec_response.len());
// let dfs = add_metadata_to_df(response_vec_api, metadata_vec_response).await?;

// Ok(dfs)
// }

// pub async fn batch_get_intraday_data(
// self,
// tickers_exchanges: Vec<(&str, &str, &str, &str)>, // (ticker, exchange, start_date, interval)
// ) -> Result<Vec<DataFrame>> {
// let mut urls = Vec::new();
// tickers_exchanges
// .iter()
// .for_each(|ticker| {
// let start_date_timestamp = string_to_timestamp(ticker.2);
// let end_date_timestamp = get_current_timestamp();
// let url = format!(
// "https://eodhistoricaldata.com/api/intraday/{}.{}?api_token={}&interval={}&fmt=json&from={}&to={}",
// ticker.ticker,
// ticker.exchange,
// self.api_token,
// ticker.series_collection_name,
// start_date_timestamp,
// end_date_timestamp
// );
// urls.push((ticker, url));
// });

// let tickers_exchanges_new: Vec<(&str, &str)> = tickers_exchanges
// .iter()
// .map(|ticker| (ticker_exchange.0, ticker_exchange.1))
// .collect();

// let response_vec_intraday_data = async_http_request(self.client.clone(), urls).await?;
// let metadata_vec_response = self.batch_get_metadata(&tickers_exchanges_new).await?;
// assert_eq!(response_vec_intraday_data.len() == metadata_vec_response.len());
// let dfs = add_metadata_to_df(response_vec_intraday_data, metadata_vec_response).await?;

// Ok(dfs)
// }

// pub async fn batch_get_live_lagged_data(
// &self,
// tickers_exchanges: Vec<(&str, &str)>, // (ticker, exchange)
// ) -> Result<Vec<DataFrame>> {
// let mut urls = Vec::new();
// tickers_exchanges.iter().for_each(|ticker| {
// let url = format!(
// "https://eodhistoricaldata.com/api/real-time/{}.{}?api_token={}&fmt=json",
// ticker.ticker, ticker_exchange.1, self.api_token,
// );
// urls.push((ticker, url));
// });

// let response_vec_live_lagged_data = async_http_request(self.client.clone(), urls).await?;
// let metadata_vec_response = self.batch_get_metadata(&tickers_exchanges).await?;
// assert_eq!(response_vec_live_lagged_data.len() == metadata_vec_response.len());
// let dfs = add_metadata_to_df(response_vec_live_lagged_data, metadata_vec_response).await?;

// Ok(dfs)
// }

/*-------------------------------------------- FUNCTION INPUTS --------------------------------------------- */
// let eod_client =EodApi::new().await;
// let eod_ohlcv = eod_client
//     .batch_get_ohlcv(vec![
//         ("AAPL", "US", "2023-12-10"),
//         // ("AAPL", "US", "2023-12-10"),
//         // ("AAPL", "US", "2023-12-10"),
//         ("BTC-USD", "CC", "2023-12-10"),
//         // ("BTC-USD", "CC", "2023-12-10"),
//         // ("BTC-USD", "CC", "2023-12-10"),
//     ])
//     .await
//     .unwrap();
// // println!("{:#?}", eod_ohlcv);
// let mongo_client = MongoDbClient::new().await;
// mongo_client.insert_series(eod_ohlcv).await.unwrap();

// let eod_intra = eod_client
//     .batch_get_intraday_data(vec![
//         ("AAPL", "US", "2023-10-01", "5m"),
//         ("AAPL", "US", "2023-10-01", "5m"),
//         ("AAPL", "US", "2023-10-01", "5m"),
//         ("BTC-USD", "CC", "2023-10-01", "5m"),
//         ("BTC-USD", "CC", "2023-10-01", "5m"),
//         ("BTC-USD", "CC", "2023-10-01", "5m"),
//     ])
//     .await
//     .unwrap();
// // println!("{:#?}", eod_intra);
// let mongo_client = MongoDbClient::new().await;
// mongo_client.insert_series(eod_intra).await.unwrap();

// let eod_live = eod_client
//     .batch_get_live_lagged_data(vec![
//         ("AAPL", "US"),
//         ("AAPL", "US"),
//         ("AAPL", "US"),
//         ("BTC-USD", "CC"),
//         ("BTC-USD", "CC"),
//         ("BTC-USD", "CC"),
//     ])
//     .await;
// println!("{:#?}", eod_live.unwrap());
