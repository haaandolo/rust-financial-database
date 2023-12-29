use struct_iterable::Iterable;
use serde::{Deserialize, Serialize};
use bson::DateTime;

#[derive(Debug, Serialize, Deserialize, Clone, Iterable)]
pub struct OhlcvMetaData {
    pub data_type: String,
    pub ticker: String,
    pub source: String,
    pub exchange: String,
    pub isin: Option<String>,
    pub currency: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApiResponse {
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub adjusted_close: f64,
    pub volume: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ohlcv {
    pub datetime: String, 
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub adjusted_close: f64,
    pub volume: i32,
    pub metadata: String,
}

// MONGODB MODELS
pub enum OhlcGranularity {
    Hours,
    Minutes,
    Seconds
}
#[derive(Debug, Serialize, Deserialize, Clone)]

pub struct TimeseriesMetaDataStruct {
    pub ticker: String,
    pub exchange: String,
    pub colletion_name: String,
    pub source: String,
    pub from: DateTime,
    pub to: DateTime,
    pub last_updated: DateTime,
}