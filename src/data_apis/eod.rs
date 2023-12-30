use anyhow::Result;
use dotenv::dotenv;
use polars::frame::DataFrame;
use polars::prelude::*;
use reqwest::Client;
use serde_json::{Value, to_string};
use std::io::Cursor;
use std::{collections::HashMap, env};

use crate::models::eod_models::OhlcvMetaData;
use crate::utility_functions::{
    add_metadata_to_df, async_http_request, get_current_date_string, get_current_timestamp,
    string_to_timestamp,
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

    pub async fn batch_get_ohlcv(
        &self,
        tickers_exchanges: Vec<(&str, &str, &str)>, // (ticker, exchange, start_date)
    ) -> Result<Vec<DataFrame>> {
        let end_date = get_current_date_string();
        let mut urls = Vec::new();
        tickers_exchanges.iter().for_each(|ticker_exchange| {
            let url = format!(
                "https://eodhistoricaldata.com/api/eod/{}.{}?api_token={}&fmt=json&from={}&to={}",
                ticker_exchange.0, ticker_exchange.1, self.api_token, ticker_exchange.2, end_date
            );
            urls.push(url);
        });

        let tickers_exchanges_trunc: Vec<(&str, &str)> = tickers_exchanges
            .iter()
            .map(|ticker_exchange| (ticker_exchange.0, ticker_exchange.1))
            .collect();

        let response_vec_api = async_http_request(self.client.clone(), urls).await?;
        let metadata_vec_response = self.batch_get_metadata(&tickers_exchanges_trunc).await?;
        assert!(response_vec_api.len() == metadata_vec_response.len());
        let dfs = add_metadata_to_df(response_vec_api, metadata_vec_response).await?;

        Ok(dfs)
    }

    pub async fn batch_get_intraday_data(
        self,
        tickers_exchanges: Vec<(&str, &str, &str, &str)>, // (ticker, exchange, start_date, interval)
    ) -> Result<Vec<DataFrame>> {
        let mut urls = Vec::new();
        tickers_exchanges
            .iter()
            .for_each(|ticker_exchange| {
                let start_date_timestamp = string_to_timestamp(ticker_exchange.2);
                let end_date_timestamp = get_current_timestamp();
                let url = format!(
                    "https://eodhistoricaldata.com/api/intraday/{}.{}?api_token={}&interval={}&fmt=json&from={}&to={}",
                    ticker_exchange.0,
                    ticker_exchange.1,
                    self.api_token,
                    ticker_exchange.3,
                    start_date_timestamp,
                    end_date_timestamp
                );
                urls.push(url);
            });

        let tickers_exchanges_new: Vec<(&str, &str)> = tickers_exchanges
            .iter()
            .map(|ticker_exchange| (ticker_exchange.0, ticker_exchange.1))
            .collect();

        let response_vec_intraday_data = async_http_request(self.client.clone(), urls).await?;
        let metadata_vec_response = self.batch_get_metadata(&tickers_exchanges_new).await?;
        assert!(response_vec_intraday_data.len() == metadata_vec_response.len());
        let dfs = add_metadata_to_df(response_vec_intraday_data, metadata_vec_response).await?;

        Ok(dfs)
    }

    pub async fn batch_get_live_lagged_data(
        &self,
        tickers_exchanges: Vec<(&str, &str)>, // (ticker, exchange)
    ) -> Result<Vec<DataFrame>> {
        let mut urls = Vec::new();
        tickers_exchanges.iter().for_each(|ticker_exchange| {
            let url = format!(
                "https://eodhistoricaldata.com/api/real-time/{}.{}?api_token={}&fmt=json",
                ticker_exchange.0, ticker_exchange.1, self.api_token,
            );
            urls.push(url);
        });

        let response_vec_live_lagged_data = async_http_request(self.client.clone(), urls).await?;
        let metadata_vec_response = self.batch_get_metadata(&tickers_exchanges).await?;
        assert!(response_vec_live_lagged_data.len() == metadata_vec_response.len());
        let dfs = add_metadata_to_df(response_vec_live_lagged_data, metadata_vec_response).await?;

        Ok(dfs)
    }

    pub async fn batch_get_metadata(
        &self,
        tickers_exchanges: &[(&str, &str)],
    ) -> Result<Vec<OhlcvMetaData>> {
        let mut metadata_vec = Vec::new();
        for ticker_exchange in tickers_exchanges {
            match ticker_exchange.1 {
                "COM" => {
                    let series_metadata = OhlcvMetaData {
                        data_type: "commodities_series".to_string(),
                        ticker: ticker_exchange.0.to_string(),
                        source: "eod".to_string(),
                        exchange: ticker_exchange.1.to_string(),
                        isin: None,
                        currency: None,
                    };
                    metadata_vec.push(series_metadata);
                }
                "CC" => {
                    let series_metadata = OhlcvMetaData {
                        data_type: "crypto_series".to_string(),
                        ticker: ticker_exchange.0.to_string(),
                        source: "eod".to_string(),
                        exchange: ticker_exchange.1.to_string(),
                        isin: None,
                        currency: None,
                    };
                    metadata_vec.push(series_metadata);
                }
                "BOND" => {
                    let series_metadata = OhlcvMetaData {
                        data_type: "bond_series".to_string(),
                        ticker: ticker_exchange.0.to_string(),
                        source: "eod".to_string(),
                        exchange: ticker_exchange.1.to_string(),
                        isin: None,
                        currency: None,
                    };
                    metadata_vec.push(series_metadata);
                }
                "FOREX" => {
                    let series_metadata = OhlcvMetaData {
                        data_type: "forex_series".to_string(),
                        ticker: ticker_exchange.0.to_string(),
                        source: "eod".to_string(),
                        exchange: ticker_exchange.1.to_string(),
                        isin: None,
                        currency: None,
                    };
                    metadata_vec.push(series_metadata);
                }
                _ => {
                    let response_string = self.client
                        .get(format!(
                            "https://eodhistoricaldata.com/api/fundamentals/{}.{}?api_token={}&fmt=json&filter=General",
                            ticker_exchange.0, ticker_exchange.1, self.api_token,
                        ))
                        .send()
                        .await?
                        .text()
                        .await?;

                    let response_hashmap: HashMap<String, Value> =
                        serde_json::from_str(&response_string)
                            .expect("batch_get_metadata() Failed to deserialise response_text");

                    let isin_value = response_hashmap
                        .get("ISIN")
                        .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
                        .unwrap_or_else(|| {
                            eprintln!(
                                "batch_get_metadata() failed to retrieve ISIN for ticker: {}",
                                ticker_exchange.0
                            );
                            ""
                        })
                        .trim_matches('"')
                        .to_string();

                    let ticker_denomination = response_hashmap
                        .get("CurrencyCode")
                        .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
                        .unwrap_or_else(|| {
                            eprintln!(
                                "batch_get_metadata() failed to retrieve currecny for ticker: {}",
                                ticker_exchange.0
                            );
                            ""
                        })
                        .trim_matches('"')
                        .to_string();

                    let series_metadata = OhlcvMetaData {
                        data_type: "equity_series".to_string(),
                        ticker: ticker_exchange.0.to_string(),
                        source: "eod".to_string(),
                        exchange: ticker_exchange.1.to_string(),
                        isin: Some(isin_value),
                        currency: Some(ticker_denomination),
                    };

                    metadata_vec.push(series_metadata);
                }
            }
        }
        Ok(metadata_vec)
    }

    pub async fn get_metadata(
        &self,
        ticker: &(&str, &str, &str, &str, &str),
    ) -> Result<OhlcvMetaData> {
        match ticker.1 {
            "COM" => {
                let series_metadata = OhlcvMetaData {
                    data_type: "commodities_series".to_string(),
                    ticker: ticker.0.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.1.to_string(),
                    isin: None,
                    currency: None,
                };
                Ok(series_metadata)
            }
            "CC" => {
                let series_metadata = OhlcvMetaData {
                    data_type: "crypto_series".to_string(),
                    ticker: ticker.0.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.1.to_string(),
                    isin: None,
                    currency: None,
                };
                Ok(series_metadata)
            }
            "BOND" => {
                let series_metadata = OhlcvMetaData {
                    data_type: "bond_series".to_string(),
                    ticker: ticker.0.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.1.to_string(),
                    isin: None,
                    currency: None,
                };
                Ok(series_metadata)
            }
            "FOREX" => {
                let series_metadata = OhlcvMetaData {
                    data_type: "forex_series".to_string(),
                    ticker: ticker.0.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.1.to_string(),
                    isin: None,
                    currency: None,
                };
                Ok(series_metadata)
            }
            _ => {
                let response_string = self.client
                    .get(format!(
                        "https://eodhistoricaldata.com/api/fundamentals/{}.{}?api_token={}&fmt=json&filter=General",
                        ticker.0, ticker.1, self.api_token,
                    ))
                    .send()
                    .await?
                    .text()
                    .await?;

                let response_hashmap: HashMap<String, Value> =
                    serde_json::from_str(&response_string)
                        .expect("batch_get_metadata() Failed to deserialise response_text");

                let isin_value = response_hashmap
                    .get("ISIN")
                    .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
                    .unwrap_or_else(|| {
                        eprintln!(
                            "batch_get_metadata() failed to retrieve ISIN for ticker: {}",
                            ticker.0
                        );
                        ""
                    })
                    .trim_matches('"')
                    .to_string();

                let ticker_denomination = response_hashmap
                    .get("CurrencyCode")
                    .map(|v| v.as_str().unwrap_or_default()) // Handle potential non-string values
                    .unwrap_or_else(|| {
                        eprintln!(
                            "batch_get_metadata() failed to retrieve currecny for ticker: {}",
                            ticker.0
                        );
                        ""
                    })
                    .trim_matches('"')
                    .to_string();

                let series_metadata = OhlcvMetaData {
                    data_type: "equity_series".to_string(),
                    ticker: ticker.0.to_string(),
                    source: "eod".to_string(),
                    exchange: ticker.1.to_string(),
                    isin: Some(isin_value),
                    currency: Some(ticker_denomination),
                };
                Ok(series_metadata)
            }
        }
    }

    pub async fn get_ohlcv(
        &self,
        ticker: &(&str, &str, &str, &str, &str), // (ticker, exchange, start_date, collection_name, api)
    ) -> Result<DataFrame> {
        let end_date = get_current_date_string();
        let url = format!(
            "https://eodhistoricaldata.com/api/eod/{}.{}?api_token={}&fmt=json&from={}&to={}",
            ticker.0, ticker.1, self.api_token, ticker.2, end_date
        );

        let response_text: String = self.client.clone().get(url).send().await?.text().await?;
        let cursor = Cursor::new(response_text);
        let mut df = JsonReader::new(cursor)
            .finish()
            .expect("Eod get_ohlc() failed to read Cursor to Dataframe");

        let metadata = self.get_metadata(ticker).await?;
        let series = Series::new("metadata", vec![to_string(&metadata)?; df.height()]);
        df.with_column(series)?;

        Ok(df)
    }
}

// pub async fn batch_get_fundamental_data(
//     &self,
//     tickers_exchanges: Vec<(&str, &str)>,
// ) -> Result<Vec<DataFrame>> {
//     let mut urls = Vec::new();
//     tickers_exchanges.iter().for_each(|ticker_exchange| {
//         let url = format!(
//             "https://eodhistoricaldata.com/api/fundamentals/{}.{}?api_token={}&fmt=json",
//             ticker_exchange.0, ticker_exchange.1, self.api_token,
//         );
//         urls.push(url);
//     });

//     let response_vec_fundamental_data = self.async_http_request(urls).await?;

//     Ok(response_vec_fundamental_data)
// }
