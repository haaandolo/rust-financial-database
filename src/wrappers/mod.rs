use crate::utility_functions::string_to_datetime;

use dotenv::dotenv;
use std::{env, collections::HashMap };
use polars::prelude::*;
use serde::{Serialize, Deserialize};
use mongodb::bson;
use anyhow::Result;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ohlcv {
    datetime: bson::DateTime,
    open: f32,
    high: f32,
    low: f32,
    close: f32,
    adjusted_close: f32,
    volume: i32,
    metadata: HashMap<String, String>
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApiResponse {
    date: String,
    open: f32,
    high: f32,
    low: f32,
    close: f32,
    adjusted_close: f32,
    volume: i32,
}

pub async fn create_reqwest_client() -> reqwest::Client {
    return reqwest::Client::new();
}

pub async fn format_ohlc_df(df: DataFrame) -> Result<DataFrame, PolarsError> {
    let df_formatted = df.lazy()
        .select([
            col("date").str().to_datetime(Some(TimeUnit::Microseconds), None, StrptimeOptions::default(), lit("raise")),
            col("open").cast(DataType::Float32).alias("open"),
            col("high").cast(DataType::Float32).alias("high"),
            col("low").cast(DataType::Float32).alias("low"),
            col("close").cast(DataType::Float32).alias("close"),
            col("adjusted_close").cast(DataType::Float32).alias("adjusted_close"),
            col("volume").cast(DataType::Int64).alias("volume"),
        ])
        .collect();
    return df_formatted
}

pub async fn metadata_info() -> HashMap<String, String> {
    let metadata = HashMap::from([
        ("isin".to_string(), "123-456-789".to_string()), // GET RID OF to_string() AND CONVERT string TO &str IN OHLCV STRUCT
        ("ticker".to_string(), "MDMA".to_string()), // GET RID OF to_string() AND CONVERT string TO &str IN OHLCV STRUCT
        ("exchange".to_string(), "NASDAQ".to_string()),
        ("source".to_string(), "eod".to_string()),
    ]);
    log::info!("Sucessfully retrieved metadata info");
    return metadata
}

pub async fn get_ohlc(client: &reqwest::Client, ticker: &str, exchange: &str, start_date: &str, end_date: &str) -> Result<Vec<Ohlcv>> {
    // get ticker metadata
    let metadata: HashMap<String, String>  = metadata_info().await;

    // hit api
    dotenv().ok();
    let api_token = env::var("API_TOKEN").expect("Failed tp parse API_TOKEN from .env");
    let param = vec![
        ("api_token", api_token),
        ("fmt", "json".to_string()),
        ("from", start_date.to_string()),
        ("to", end_date.to_string())
    ];

    let response_text: String = client
        .get(format!("https://eodhd.com/api/eod/{}.{}", ticker, exchange))
        .query(&param)
        .send()
        .await?
        .text()
        .await?;
    log::info!("Sucessfully hit EOD OHLCV api");

    // get response and formatt into dersired structure
    let response: Vec<ApiResponse> = serde_json::from_str(&response_text)
        .expect("Failed to deserialize OHLCV api text response to APIResponse struct");
    log::info!("Sucessfully parse API response to APIResponse struct");

    let mut response_formatted: Vec<Ohlcv> = Vec::new();
    for ohlcv in response.iter() {
        response_formatted.push(Ohlcv {
            datetime: string_to_datetime(ohlcv.date.as_str()).await,
            open: ohlcv.open,
            high: ohlcv.high,
            low: ohlcv.low,
            close: ohlcv.close,
            adjusted_close: ohlcv.adjusted_close,
            volume: ohlcv.volume,
            metadata: metadata.clone() // GET RID OF THIS CLONE
        })
    }
    log::info!("Sucessfully parse APIResponse struct to Vec<Ohlcv>");
    Ok(response_formatted)
}

// pub async fn get_ohlc2(client: &reqwest::Client, ticker: &str, exchange: &str, start_date: &str, end_date: &str) -> Result<DataFrame, reqwest::Error> {
//     dotenv().ok();
//     let api_token = env::var("API_TOKEN").unwrap();
//     let param = vec![
//         ("api_token", api_token),
//         ("fmt", "json".to_string()),
//         ("from", start_date.to_string()),
//         ("to", end_date.to_string())
//     ];

//     let response_text = client
//         .get(format!("https://eodhd.com/api/eod/{ticker}.{exchange}"))
//         .query(&param)
//         .send()
//         .await?
//         .text()
//         .await?;

//     let cursor = Cursor::new(response_text);
//     let df = JsonReader::new(cursor).finish().unwrap();
//     let df_formatted = format_ohlc_df(df).await;

//     Ok(df_formatted)
// }