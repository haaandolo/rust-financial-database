use anyhow::Result;
use dotenv::dotenv;
use futures::future;
use polars::{
    frame::DataFrame,
    prelude::{JsonReader, SerReader},
};
use reqwest::Client;
use serde_json::json;
use std::env;

use std::io::Cursor;
// use crate::securities::Equities;
use crate::utility_functions::string_to_timestamp;

pub struct WrapperFunctions {
    client: Client,
    api_token: String,
}

impl WrapperFunctions {
    pub async fn new() -> Self {
        dotenv().ok();
        let eod_api_token = env::var("API_TOKEN").expect("Failed tp parse API_TOKEN from .env");
        Self {
            client: Client::new(),
            api_token: eod_api_token,
        }
    }

    pub async fn async_http_request(&self, urls: Vec<String>) -> Result<Vec<DataFrame>> {
        let bodies = future::join_all(urls.into_iter().map(|url| {
            let client = self.client.clone();
            async move {
                let resp = client.get(url).send().await?;
                resp.bytes().await
            }
        }))
        .await;

        let mut response_vec = Vec::new();
        for body in bodies {
            match body {
                Ok(body) => {
                    let body_string = String::from_utf8_lossy(&body).to_string();
                    let cursor = Cursor::new(body_string);
                    let df = JsonReader::new(cursor)
                        .finish()
                        .expect("async_http_request() failed to read Cursor to Dataframe");
                    response_vec.push(df);
                }
                Err(e) => eprintln!("Got an error: {}", e),
            }
        }

        Ok(response_vec)
    }

    pub async fn batch_get_ohlcv(
        &self,
        tickers_exchanges: Vec<(&str, &str)>,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<DataFrame>> {

        let mut urls = Vec::new();
        tickers_exchanges.iter()
            .for_each(|ticker_exchange| {
                let url = format!(
                    "https://eodhd.com/api/eod/{}.{}?api_token={}&fmt=json&from={}&to={}",
                    ticker_exchange.0,
                    ticker_exchange.1,
                    self.api_token,
                    start_date,
                    end_date
                );
                urls.push(url);
            });

        let response_vec_api = self
            .async_http_request(urls)
            .await
            .expect("batch_get_ohlcv() failed to unwrap response_vec_api");

        Ok(response_vec_api)
    }

    pub async fn get_intraday_data(
        self,
        ticker: &str,
        exchange: &str,
        start_date: &str,
        end_date: &str,
        interval: &str,
    ) -> Result<DataFrame> {
        let params = json! ({
            "api_token": self.api_token,
            "interval": interval,
            "fmt": "json",
            "from": string_to_timestamp(start_date),
            "to": string_to_timestamp(end_date)
        });

        let response_text = self
            .client
            .get(format!(
                "https://eodhd.com/api/intraday/{}.{}",
                ticker, exchange
            ))
            .query(&params)
            .send()
            .await?
            .text()
            .await?;

        let cursor = Cursor::new(response_text);
        let df = JsonReader::new(cursor)
            .finish()
            .expect("get_intraday_data() failed to convert Cursor to Dataframe");
        Ok(df)
    }

    pub async fn batch_get_intraday_data(
        self,
        tickers_exchanges: Vec<(&str, &str)>,
        start_date: &str,
        end_date: &str,
        interval: &str,
    ) -> Result<Vec<DataFrame>> {
        let mut urls = Vec::new();
        let start_date_timestamp = string_to_timestamp(start_date);
        let end_date_timestamp = string_to_timestamp(end_date);
        tickers_exchanges
            .iter()
            .for_each(|ticker_exchange| {
                let url = format!(
                    "https://eodhd.com/api/intraday/{}.{}?api_token={}&interval={}&fmt=json&from={}&to={}",
                    ticker_exchange.0,
                    ticker_exchange.1,
                    self.api_token,
                    interval,
                    start_date_timestamp,
                    end_date_timestamp
                );
                urls.push(url);
            });

        let response_vec_intraday_data = self
            .async_http_request(urls)
            .await
            .expect("batch_get_ohlcv() failed to unwrap response_vec_api");

        Ok(response_vec_intraday_data)
    }
}

// use crate::utility_functions::string_to_datetime;

// use dotenv::dotenv;
// use std::env;
// use polars::prelude::{JsonReader, DataFrame, PolarsError, DataType, TimeUnit, col, IntoLazy, StrptimeOptions, lit, SerReader};
// use serde::{Serialize, Deserialize};
// use mongodb::bson;
// use anyhow::Result;
// use struct_iterable::Iterable;
// use std::io::Cursor;

// /* --------------- REQUIRED TYPES --------------- */
// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct ApiResponse {
//     date: String,
//     open: f32,
//     high: f32,
//     low: f32,
//     close: f32,
//     adjusted_close: f32,
//     volume: i32,
// }

// #[derive(Debug, Serialize, Deserialize, Clone, Iterable)]
// pub struct OhlcvMetadata {
//     pub isin: String,
//     pub ticker: String,
//     pub source: String,
//     pub exchange: String,
// }

// #[derive(Debug, Serialize, Deserialize, Clone, Iterable)]
// pub struct DocumentMetaData {
//     pub isin: String,
//     pub ticker: String,
//     pub source: String,
//     pub exchange: String,
// }
// /* --------------- FUNCTIONS --------------- */
// pub async fn create_reqwest_client() -> reqwest::Client {
//     return reqwest::Client::new();
// }

// pub async fn format_ohlc_df(df: DataFrame) -> Result<DataFrame, PolarsError> {
//     let df_formatted = df.lazy()
//         .select([
//             col("date").str().to_datetime(Some(TimeUnit::Microseconds), None, StrptimeOptions::default(), lit("raise")),
//             col("open").cast(DataType::Float32).alias("open"),
//             col("high").cast(DataType::Float32).alias("high"),
//             col("low").cast(DataType::Float32).alias("low"),
//             col("close").cast(DataType::Float32).alias("close"),
//             col("adjusted_close").cast(DataType::Float32).alias("adjusted_close"),
//             col("volume").cast(DataType::Int64).alias("volume"),
//         ])
//         .collect();
//     return df_formatted
// }

// pub async fn metadata_info() -> Result<OhlcvMetadata> {
//     let metadata: OhlcvMetadata = OhlcvMetadata {
//         isin: "123-456-789".to_string(),
//         ticker: "AAPL".to_string(),
//         exchange: "NASDAQ".to_string(),
//         source: "eod".to_string(),
//     };
//     log::info!("Sucessfully retrieved metadata info");
//     Ok(metadata)
// }

// pub async fn get_ohlc(client: &reqwest::Client, ticker: &str, exchange: &str, start_date: &str, end_date: &str) -> Result<Vec<Ohlcv>> {
//     // get ticker metadata
//     let metadata: OhlcvMetadata  = metadata_info().await
//         .expect("get_ohlv() failed to get metadata_info()");

//     // hit api
//     dotenv().ok();
//     let api_token = env::var("API_TOKEN").expect("Failed tp parse API_TOKEN from .env");
//     let param = vec![
//         ("api_token", api_token),
//         ("fmt", "json".to_string()),
//         ("from", start_date.to_string()),
//         ("to", end_date.to_string())
//     ];

//     let response_text: String = client
//         .get(format!("https://eodhd.com/api/eod/{}.{}", ticker, exchange))
//         .query(&param)
//         .send()
//         .await?
//         .text()
//         .await?;
//     log::info!("Sucessfully hit EOD OHLCV api for {}", &ticker);

//     // get response and formatt into dersired structure
//     let response: vec<apiresponse> = serde_json::from_str(&response_text)
//         .expect("failed to deserialize ohlcv api text response to apiresponse struct");
//     log::info!("Sucessfully parse API response to APIResponse struct");

//     let mut response_formatted: Vec<Ohlcv> = Vec::new();
//     for ohlcv in response.iter() {
//         response_formatted.push(Ohlcv {
//             datetime: string_to_datetime(ohlcv.date.as_str()).await,
//             open: ohlcv.open,
//             high: ohlcv.high,
//             low: ohlcv.low,
//             close: ohlcv.close,
//             adjusted_close: ohlcv.adjusted_close,
//             volume: ohlcv.volume,
//             metadata: metadata.clone() // GET RID OF THIS CLONE
//         })
//     }
//     log::info!("Sucessfully parse APIResponse struct to Vec<Ohlcv>");
//     Ok(response_formatted)
// }

// /*
//     Get fundamental data
// */
// pub async fn get_ticker_generals(client: &reqwest::Client, ticker: &str, exchange: &str) -> Result<DataFrame> {
//     // hit api
//     dotenv().ok();
//     let api_token = env::var("API_TOKEN").expect("Failed tp parse API_TOKEN from .env");
//     let param = vec![
//         ("api_token", api_token),
//         ("fmt", "json".to_string()),
//         ("filter", "General".to_string())
//     ];

//     let response_text: String = client
//         .get(format!("https://eodhd.com/api/fundamentals/{}.{}", ticker, exchange))
//         .query(&param)
//         .send()
//         .await?
//         .text()
//         .await?;

//     let cursor = Cursor::new(&response_text);
//     let df = JsonReader::new(cursor).finish().unwrap();
//     println!("{:?}", &df);
//     // println!("{:#?}", df.column("Exchange")?.get(0)?.to_string().trim_matches('"'));
//     Ok(df)
// }

// /* --------------- TEST --------------- */
// #[cfg(test)]
// mod test { }

// // pub async fn get_ohlc2(client: &reqwest::Client, ticker: &str, exchange: &str, start_date: &str, end_date: &str) -> Result<DataFrame, reqwest::Error> {
// //     dotenv().ok();
// //     let api_token = env::var("API_TOKEN").unwrap();
// //     let param = vec![
// //         ("api_token", api_token),
// //         ("fmt", "json".to_string()),
// //         ("from", start_date.to_string()),
// //         ("to", end_date.to_string())
// //     ];

// //     let response_text = client
// //         .get(format!("https://eodhd.com/api/eod/{ticker}.{exchange}"))
// //         .query(&param)
// //         .send()
// //         .await?
// //         .text()
// //         .await?;

// //     let cursor = Cursor::new(response_text);
// //     let df = JsonReader::new(cursor).finish().unwrap();
// //     let df_formatted = format_ohlc_df(df).await;

// //     Ok(df_formatted)
// // }

// // pub async fn metadata_info(client: &reqwest::Client, ticker: &str, exchange: &str) -> Result<OhlcvMetadata> {
// //     let metadata_general = get_ticker_generals(client, ticker, exchange)
// //         .await
// //         .expect("metadata_info() could not get metadata info. failed at get_ticker_general() function");

// //     println!("{:#?}", metadata_general.column("Exchange")?.get(0)?.to_string().trim_matches('"'));
// //     let metadata: OhlcvMetadata = OhlcvMetadata {
// //         isin: "123-456-789".to_string(),
// //         ticker: "AAPL".to_string(),
// //         exchange: "NASDAQ".to_string(),
// //         source: "eod".to_string(),
// //     };
// //     log::info!("Sucessfully retrieved metadata info");
// //     Ok(metadata)
// // }
