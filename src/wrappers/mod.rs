// file contains functions that can be reused throughout the system
// for example, API's provide data for multiple different asset types
// and security types. Instead of repeating code, we can have wrapper
// functions.

use dotenv::dotenv;
use std::env;
use polars::prelude::*;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use std::io::Cursor;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetaDataStruct {
    data_type: String,
    cussip: String
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ohlcv {
    date: DateTime<Utc>,
    open: f32,
    high: f32,
    low: f32,
    close: f32,
    adjusted_close: f32,
    volume: f32,
    metadata: Option<MetaDataStruct>
}

pub async fn create_reqwest_client() -> reqwest::Client {
    return reqwest::Client::new();
}

pub async fn format_ohlc_df(df: DataFrame) -> DataFrame {
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
    return df_formatted.unwrap()
                         
}

pub async fn metadata_info() -> MetaDataStruct {
    let metadata: MetaDataStruct = MetaDataStruct { data_type: "stock".to_string(), cussip: "123-456-789".to_string() };
    return metadata
}

pub async fn get_ohlc(client: &reqwest::Client, ticker: &str, exchange: &str, start_date: &str, end_date: &str) -> Result<Vec<Ohlcv>, reqwest::Error> {
    // get ticker metadata
    let metadata: MetaDataStruct = metadata_info().await;

    dotenv().ok();
    let api_token = env::var("API_TOKEN").unwrap();
    let param = vec![
        ("api_token", api_token),
        ("fmt", "json".to_string()),
        ("from", start_date.to_string()),
        ("to", end_date.to_string())
    ];

    let response_text: String = client
        .get(format!("https://eodhd.com/api/eod/{ticker}.{exchange}"))
        .query(&param)
        .send()
        .await?
        .text()
        .await?;

    let mut response_ohlv_obj: Vec<Ohlcv> = serde_json::from_str(&response_text).unwrap();
    response_ohlv_obj.iter_mut().for_each(|ohlcv| {
        ohlcv.metadata = Some(metadata.clone()); // GET RID OF THIS CLONE
    });

    Ok(response_ohlv_obj)
}

pub async fn get_ohlc2(client: &reqwest::Client, ticker: &str, exchange: &str, start_date: &str, end_date: &str) -> Result<DataFrame, reqwest::Error> {
    dotenv().ok();
    let api_token = env::var("API_TOKEN").unwrap();
    let param = vec![
        ("api_token", api_token),
        ("fmt", "json".to_string()),
        ("from", start_date.to_string()),
        ("to", end_date.to_string())
    ];

    let response_text = client
        .get(format!("https://eodhd.com/api/eod/{ticker}.{exchange}"))
        .query(&param)
        .send()
        .await?
        .text()
        .await?;

    let cursor = Cursor::new(response_text);
    let df = JsonReader::new(cursor).finish().unwrap();
    let df_formatted = format_ohlc_df(df).await;

    Ok(df_formatted)
}