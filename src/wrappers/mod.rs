// file contains functions that can be reused throughout the system
// for example, API's provide data for multiple different asset types
// and security types. Instead of repeating code, we can have wrapper
// functions.
pub mod models;

use dotenv::dotenv;
use std::env;
use serde_json::{Value, Error, json};
use polars::prelude::*;
use std::io::Cursor;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetaDataStruct {
    data_type: String,
    cussip: String
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

pub async fn metadata_series(length: usize) -> Vec<MetaDataStruct> {
    let metadata = MetaDataStruct { data_type: "stock".to_string(), cussip: "123-456-789".to_string() };
    let metadata_list = vec![metadata; length];
    return metadata_list
}

pub async fn get_ohlc(client: &reqwest::Client, ticker: &str, exchange: &str, start_date: &str, end_date: &str) -> Result<DataFrame, reqwest::Error> {
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

pub async fn get_ticker_fundamentals() -> Result<(), reqwest::Error> {
    dotenv().ok();
    // let api_token = env::var("API_TOKEN").unwrap();
    // let param = vec![("api_token", "demo")];
    let example = reqwest::Client::new()
        .get(format!("https://eodhd.com/?api_token=demo"))
        // .query(&param)
        .send()
        .await?
        .text()
        .await?;
    let json: Result<Value, Error> = serde_json::from_str(&example);
    println!("$$$$$$$$$$$$$$$$");
    println!("{:#?}", json.unwrap());
    Ok(())
}