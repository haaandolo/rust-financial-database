use anyhow::Result;
use bson::DateTime as BsonDateTime;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc, Weekday};
use futures::future;
use mongodb::bson;
use polars::prelude::*;
use reqwest::Client;
use serde_json::to_string;
use std::{io::Cursor, collections::HashMap};

use crate::models::eod_models::{OhlcvMetaData, MongoTickerParams};

/*------------------------------ DATE UTILITY FUNCTIONS ------------------------------*/
pub fn string_to_datetime(date: &str) -> bson::DateTime {
    match date {
        // _ if date == "no_date" => (),
        _ if date.len() <= 10 => {
            let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .expect("Could not parse date string in %Y-%m-%d to NativeDate object");
            let datetime = date.and_hms_opt(0, 0, 0)
                .expect("Could not convert NativeDate to NativeDateTime object for date string in format %Y-%m-%d");
            let datetime_utc: DateTime<Utc> = Utc.from_utc_datetime(&datetime);
            bson::DateTime::from_chrono(datetime_utc)
        }
        _ => {
            let datetime = NaiveDateTime::parse_from_str(date, "%Y-%m-%d %H:%M:%S").expect(
                "Could not parse date string in %Y-%m-%d %H:%M:%S to NativeDateTime object",
            );
            let datetime_utc: DateTime<Utc> = Utc.from_utc_datetime(&datetime);
            bson::DateTime::from_chrono(datetime_utc)
        }
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

pub fn get_current_datetime_bson() -> bson::DateTime {
    let current_date = Utc::now();
    bson::DateTime::from_chrono(current_date)
}

fn is_weekday(date: NaiveDate) -> bool {
    let weekday = date.weekday();
    weekday != Weekday::Sat && weekday != Weekday::Sun
}

pub fn has_business_day_between(date1: BsonDateTime, date2: BsonDateTime) -> bool {
    let date1: DateTime<Utc> = date1.into();
    let date2: DateTime<Utc> = date2.into();
    let mut current_date = date1.date_naive();
    let end_date = date2.date_naive();
    while current_date <= end_date {
        if is_weekday(current_date) {
            return false;
        }
        current_date = current_date.succ_opt().unwrap();
    }
    true
}

pub fn get_timestamps_tuple(
    from: BsonDateTime,
    to: BsonDateTime,
    granularity: &str,
) -> Result<Vec<(i64, i64)>> {
    let mut duration = 0;
    match granularity.chars().last().unwrap() {
        'm' => duration = 120,
        'h' => duration = 350, // CHANGE TO 7200 ONCE ON PAID
        _ => eprintln!("Invalid granularity"),
    }

    let start_date: DateTime<Utc> = from.into();
    let end_date: DateTime<Utc> = to.into();
    let mut current_date = start_date;

    let mut date_tuples = Vec::new();
    while current_date < end_date {
        let next_date = current_date + Duration::days(duration);
        if next_date > end_date {
            break;
        }
        let current_date_timestamp = current_date.timestamp();
        let next_date_timestamp = (next_date - Duration::seconds(1)).timestamp();
        date_tuples.push((current_date_timestamp, next_date_timestamp));
        current_date = next_date;
    }

    Ok(date_tuples)
}

/*------------------------------ NETWORK UTILITY FUNCTIONS ------------------------------*/
pub async fn async_http_request(client: Client, urls: HashMap<String, MongoTickerParams>) -> Result<Vec<(MongoTickerParams, DataFrame)>> {
    let bodies = future::join_all(urls.into_iter().map(|(url, param)| {
        let client = client.clone();
        async move {
            let resp = client.get(url).send().await.unwrap();
            let result = resp.bytes().await;
            (param, result)
        }
    }))
    .await;

    let mut response_vec = Vec::new();
    for (param, body) in bodies {
        match body {
            Ok(body) => {
                let body_string = String::from_utf8_lossy(&body).into_owned();
                if body_string != "[]" {
                    let cursor = Cursor::new(body_string);
                    let df = JsonReader::new(cursor).finish();
                    match df {
                        Ok(df) => response_vec.push((param, df)),
                        Err(e)=> eprintln!("async_http_request() Could not parse response to DataFrame: {}", e),
                    }
                }
            }
            Err(e) => eprintln!("Got an error: {}", e),
        }
    }

    Ok(response_vec)
}

/*------------------------------ DATA WRANGLING UTILITY FUNCTIONS ------------------------------*/
pub fn add_metadata_to_df(
    dfs: Vec<DataFrame>,
    metadata_vec: Vec<OhlcvMetaData>,
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
