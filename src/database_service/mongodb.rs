use anyhow::Result;
use bson::{doc, Bson, DateTime, Document};
use dotenv::dotenv;
use mongodb::Collection;
use mongodb::{Client, Database};
use polars::prelude::*;
use serde_json::to_string;
use std::env;

use crate::models::eod_models::{Ohlcv, SeriesMetaData};
use crate::utility_functions::string_to_datetime;
use polars::frame::row::Row;
pub struct MongoDbClient {
    client: Client,
    db: Database,
}

impl MongoDbClient {
    pub async fn new() -> Self {
        dotenv().ok();
        let client_url = env::var("MONGODB_URI").expect("Could not parse MongoDB URI from .env");
        let database_name =
            env::var("MONGODB_NAME").expect("Could not parse MongoDB name from .env");
        let mongo_client = Client::with_uri_str(client_url)
            .await
            .expect("Could not create MongoDB Client from DB URI");
        log::info!("Established Client for MongoDB!");
        Self {
            client: mongo_client.clone(),
            db: mongo_client.database(database_name.as_str()),
        }
    }

    pub async fn insert_series(&self, dfs: Vec<DataFrame>) -> Result<()> {
        for df in dfs.iter() {
            let col_names = df.get_column_names();
            let mut doc_vec = Vec::new();
            for i in 0..df.height() {
                let row = df.get_row(i).unwrap();
                let mut doc_row = Document::new();
                for (i, name) in col_names.iter().enumerate().take(df.width()) {
                    match (*name, row.0.get(i)) {
                        (_, Some(AnyValue::Null)) => {
                            doc_row.insert(name.to_string(), Bson::Null);
                        }
                        ("open", Some(AnyValue::Float64(number)))
                        | ("high", Some(AnyValue::Float64(number)))
                        | ("low", Some(AnyValue::Float64(number)))
                        | ("close", Some(AnyValue::Float64(number)))
                        | ("adjusted_close", Some(AnyValue::Float64(number))) => {
                            doc_row.insert(name.to_string(), Bson::Double(*number));
                        }
                        ("volume", Some(AnyValue::Int64(number))) => {
                            doc_row.insert(name.to_string(), *number);
                        }
                        ("datetime", Some(AnyValue::Utf8(string))) => {
                            let datetime = string_to_datetime(string).await;
                            doc_row.insert(name.to_string(), datetime);
                        }
                        ("date", Some(AnyValue::Utf8(string))) => {
                            let date = string_to_datetime(string).await;
                            doc_row.insert(name.to_string(), date);
                        }
                        ("metadata", Some(AnyValue::Utf8(string))) => {
                            let metadata: SeriesMetaData =
                                serde_json::from_str(string).expect("Could not parse metadata!");
                            let metadata_bson = bson::to_bson(&metadata)
                                .expect("Could not convert metadata to Bson!");
                            doc_row.insert(name.to_string(), metadata_bson);
                        }
                        _ => log::error!("insert_series() Could not parse column!"),
                    }
                }
                // if document row datetime is greater than metadata datetime, push row to doc_vec
                doc_vec.push(doc_row);
            }
            let collection = self.db.collection::<Document>("equity_spot_1d");
            collection.insert_many(doc_vec, None).await?;
        }
        Ok(())
    }
}
