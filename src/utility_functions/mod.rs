use anyhow::Result;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use futures::future;
use mongodb::bson;
use polars::prelude::*;
use reqwest::Client;
use serde_json::to_string;
use std::io::Cursor;

use crate::models::eod_models::SeriesMetaData;

/*------------------------------ DATE UTILITY FUNCTIONS ------------------------------*/
pub async fn string_to_datetime(date: &str) -> bson::DateTime {
    match date {
        _ if date.len() <= 10 => {
            let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .expect("Could not parse date string in %Y-%m-%d to NativeDate object");
            let datetime = date.and_hms_opt(0, 0, 0)
                .expect("Could not convert NativeDate to NativeDateTime object for date string in format %Y-%m-%d");
            let datetime_utc: DateTime<Utc> = Utc.from_utc_datetime(&datetime);
            bson::DateTime::from_chrono(datetime_utc)
        }
        _ => bson::DateTime::parse_rfc3339_str("1998-02-12T00:01:00.023Z").unwrap(),
    }
}

pub fn string_to_timestamp(date: &str) -> i64 {
    let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .expect("Could not parse date string in %Y-%m-%d to NativeDate object");
    let datetime = date.and_hms_opt(0, 0, 0).expect(
        "Could not convert NativeDate to NativeDateTime object for date string in format %Y-%m-%d",
    );
    let datetime_utc: DateTime<Utc> = Utc.from_utc_datetime(&datetime);
    datetime_utc.timestamp()
}

pub fn get_current_date_string() -> String {
    let current_date = Utc::now();
    let current_date_string = current_date.format("%Y-%m-%d").to_string();
    current_date_string
}

pub fn get_current_timestamp() -> i64 {
    let current_date = Utc::now();
    current_date.timestamp()
}

/*------------------------------ NETWORK UTILITY FUNCTIONS ------------------------------*/
pub async fn async_http_request(client: Client, urls: Vec<String>) -> Result<Vec<DataFrame>> {
    let bodies = future::join_all(urls.into_iter().map(|url| {
        let client = client.clone();
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

/*------------------------------ DATA WRANGLING UTILITY FUNCTIONS ------------------------------*/
pub async fn add_metadata_to_df(
    dfs: Vec<DataFrame>,
    metadata_vec: Vec<SeriesMetaData>,
) -> Result<Vec<DataFrame>> {
    let mut dfs_clean = Vec::new();
    for (mut df, metadata) in dfs.into_iter().zip(metadata_vec.into_iter()) {
        // CHANGE "to_string(&metadata)?" TO HASHMAP IF WE NEED FOR DATABASE READS
        let series = Series::new("metadata", vec![to_string(&metadata)?; df.height()]);
        df.with_column(series)?;
        dfs_clean.push(df)
    }
    Ok(dfs_clean)
}
