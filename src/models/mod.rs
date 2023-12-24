use struct_iterable::Iterable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Iterable)]
pub struct OhlcvMetadata {
    pub data_type: String,
    pub isin: String,
    pub ticker: String,
    pub source: String,
    pub exchange: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApiResponse {
    pub date: String,
    pub open: f32,
    pub high: f32,
    pub low: f32,
    pub close: f32,
    pub adjusted_close: f32,
    pub volume: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ohlcv {
    pub datetime: bson::DateTime,
    pub open: f32,
    pub high: f32,
    pub low: f32,
    pub close: f32,
    pub adjusted_close: f32,
    pub volume: i32,
    pub metadata: OhlcvMetadata,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum EodExchangeTypes {
    US, // US is only used for US equities and is an aggregate of 60 US exchanges, there are more
    COMM, // commodities
    CC, // crypto currencies
    BOND, 
    FOREX
}