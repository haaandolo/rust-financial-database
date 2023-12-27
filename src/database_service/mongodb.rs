use anyhow::Result;
use bson::{doc, Bson, DateTime, Document};
use dotenv::dotenv;
use mongodb::{Client, Database};
use polars::prelude::*;
use serde_json::to_string;
use std::env;

use crate::models::eod_models::Ohlcv;
use polars::frame::row::Row;
// use crate::utility_functions::clean_df_before_insert;
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
                    match row.0.get(i) {
                        Some(AnyValue::Float64(number)) => {
                            doc_row.insert(name.to_string(), Bson::Double(*number));
                        }
                        Some(AnyValue::Utf8(string)) => {
                            doc_row.insert(name.to_string(), Bson::String(string.to_string()));
                        }
                        Some(AnyValue::Int32(number)) => {
                            doc_row.insert(name.to_string(), Bson::Int32(*number));
                        }
                        Some(AnyValue::Int64(number)) => {
                            doc_row.insert(name.to_string(), Bson::Int64(*number));
                        }
                        Some(AnyValue::Null) => {
                            doc_row.insert(name.to_string(), Bson::Null);
                        }
                        _ => println!("Could not parse value to Bson type!"),
                    }
                }
                doc_vec.push(doc_row);
            }
            let collection = self.db.collection::<Document>("equity_spot_1d");
            collection.insert_many(doc_vec, None).await?;
        }
        Ok(())
    }
}
