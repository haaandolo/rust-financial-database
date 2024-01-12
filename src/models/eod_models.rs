use serde::{Deserialize, Serialize};
use bson::DateTime;

/*
    MongoDb Models
*/
#[derive(Debug, Serialize, Clone)]
pub struct Ohlcv {
    pub datetime: DateTime, 
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub adjusted_close: f64,
    pub volume: i32,
    pub metadata: OhlcvMetaData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeseriesMetaDataStruct {
    pub ticker: String,
    pub exchange: String,
    pub series_collection_name: String,
    pub source: String,
    pub from: DateTime,
    pub to: DateTime,
    pub last_updated: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OhlcvMetaData {
    pub metadata_collection_name: String,
    pub ticker: String,
    pub source: String,
    pub exchange: String,
    pub currency: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MongoTickerParams {
    pub ticker: String,
    pub exchange: String,
    pub series_collection_name: String,
    pub source: String,
    pub from: DateTime,
    pub to: DateTime,
}


#[derive(Debug, Deserialize, Clone)]
pub struct ReadSeriesFromMongoDb {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub adjusted_close: Option<f64>,
    pub volume: Option<i64>,
    pub datetime: DateTime,
    pub metadata: OhlcvMetaData,
}