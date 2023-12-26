use struct_iterable::Iterable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Iterable)]
pub struct SeriesMetaData {
    pub data_type: String,
    pub ticker: String,
    pub source: String,
    pub exchange: String,
    pub isin: Option<String>,
    pub currency: Option<String>,
}