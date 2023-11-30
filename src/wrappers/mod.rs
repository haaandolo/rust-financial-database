use anyhow::Result;
use dotenv::dotenv;
use futures::{stream, StreamExt};
use reqwest::Client;
use std::env;
use tokio;

// use crate::models::{ApiResponse, Ohlcv};
// use crate::securities::Equities;
// use crate::utility_functions::string_to_datetime;

pub struct WrapperFunctions {
    client: Client,
    api_token: String,
}

impl WrapperFunctions {
    const PARALLEL_REQUESTS: usize = 2;

    pub async fn new() -> Self {
        dotenv().ok();
        let eod_api_token = env::var("API_TOKEN").expect("Failed tp parse API_TOKEN from .env");
        Self {
            client: Client::new(),
            api_token: eod_api_token,
        }
    }

    pub async fn parallel_http_request(&self, urls: Vec<String>) -> Result<()> {
        let bodies = stream::iter(urls)
            .map(|url| {
                let client = self.client.clone();
                tokio::spawn(async move {
                    let response = client.get(url).send().await?;
                    response.bytes().await
                })
            })
            .buffer_unordered(Self::PARALLEL_REQUESTS);

        bodies
            .for_each(|body| async {
                match body {
                    Ok(Ok(body)) => println!("OKOKOKOK {:?}", body),
                    Ok(Err(e)) => eprintln!("Got a reqwest::Error: {}", e),
                    Err(e) => eprintln!("Got a tokio::JoinError {}", e),
                }
            })
            .await;
        Ok(())
    }

    pub async fn batch_get_ohlcv(
        &self,
        ticker: Vec<&str>,
        exchange: Vec<&str>,
        start_date: &str,
        end_date: &str,
    ) -> Result<()> {

        // let equities_client = Equities::new().await;
        // let metadata = equities_client
        //     .get_series_metadata(ticker, exchange)
        //     .await
        //     .unwrap_or_else(|_| {
        //         panic!(
        //             "batch_get_ohlcv() failed on get_series_metadata() for {:?}",
        //             ticker
        //         )
        //     });

        let param = vec![
            ("api_token", self.api_token.clone()), // GET RID OF THIS CLONE
            ("fmt", "json".to_string()),
            ("from", start_date.to_string()),
            ("to", end_date.to_string()),
        ];

        let param_string = param
            .iter()
            .map(|(key, value)| format!("{}={}", key, value))
            .collect::<Vec<String>>()
            .join("&");

        // let results: Vec<String> = Vec::new();

        let zipped = ticker.iter().zip(exchange.iter()).collect::<Vec<_>>();

        for (sub_ticker, sub_exchange) in zipped {
            let url = format!("https://eodhd.com/api/eod/{}.{}{}", sub_ticker, sub_exchange, param_string);
            println!("{}", url)
        }

        // let response_text: String = self
        //     .client
        //     .get(format!("https://eodhd.com/api/eod/{}.{}", ticker, exchange))
        //     .query(&param)
        //     .send()
        //     .await?
        //     .text()
        //     .await?;

        // let response: Vec<ApiResponse> = serde_json::from_str(&response_text)
        //     .expect("Failed to deserialize OHLCV api text response to APIResponse struct");

        // let mut response_formatted: Vec<Ohlcv> = Vec::new();
        // for ohlcv in response.iter() {
        //     response_formatted.push(Ohlcv {
        //         datetime: string_to_datetime(ohlcv.date.as_str()).await,
        //         open: ohlcv.open,
        //         high: ohlcv.high,
        //         low: ohlcv.low,
        //         close: ohlcv.close,
        //         adjusted_close: ohlcv.adjusted_close,
        //         volume: ohlcv.volume,
        //         metadata: metadata.clone(), // GET RID OF THIS CLONE
        //     })
        // }

        // println!("{:#?}", response_formatted);

        Ok(())
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
//     let response: Vec<ApiResponse> = serde_json::from_str(&response_text)
//         .expect("Failed to deserialize OHLCV api text response to APIResponse struct");
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
