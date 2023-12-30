use anyhow::Result;
use bson::{doc, Bson, Document};
use chrono::Utc;
use dotenv::dotenv;
use mongodb::{
    bson,
    options::{
        CreateCollectionOptions, FindOptions, TimeseriesGranularity, TimeseriesOptions,
        UpdateOptions,
    },
    Client,
};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::env;

use crate::data_apis::EodApi;
use crate::models::eod_models::{OhlcvMetaData, TimeseriesMetaDataStruct};
use crate::utility_functions::{get_current_datetime_bson, string_to_datetime};

pub struct MongoDbClient {
    client: Client,
    db_name: String,
    db_metadata_name: String,
}

impl MongoDbClient {
    pub async fn new() -> Self {
        dotenv().ok();
        let client_url = env::var("MONGODB_URI").expect("Could not parse MongoDB URI from .env");
        let mongo_client = Client::with_uri_str(client_url)
            .await
            .expect("Could not create MongoDB Client from DB URI");
        let database_name =
            env::var("MONGODB_NAME").expect("Could not parse MongoDB name from .env");
        let database_metadata_name = env::var("MONGODB_METADATA_NAME")
            .expect("Could not parse MongoDB metadata name from .env");
        log::info!("Established Client for MongoDB!");
        Self {
            client: mongo_client.clone(),
            db_name: database_name,
            db_metadata_name: database_metadata_name,
        }
    }

    pub async fn create_series_collection(
        &self,
        ticker: &str,
        collection_name: &str,
    ) -> Result<bool> {
        let series_db = self.client.clone().database(&self.db_name);
        let timeseries_options = TimeseriesOptions::builder()
            .time_field("datetime".to_string())
            .meta_field(Some("metadata".to_string()))
            .granularity(match ticker.chars().last() {
                Some('h') | Some('d') => Some(TimeseriesGranularity::Hours),
                Some('m') => Some(TimeseriesGranularity::Minutes),
                Some('s') => Some(TimeseriesGranularity::Seconds),
                _ => None,
            })
            .build();
        let options = CreateCollectionOptions::builder()
            .timeseries(timeseries_options)
            .build();
        series_db
            .create_collection(&collection_name, Some(options))
            .await
            .expect("Failed to create timeseries collection");
        log::info!(
            "Sucessfully created timeseries collection: {}",
            &collection_name
        );
        Ok(true)
    }

    pub async fn create_metadata_collection(&self, collection_name: &str) -> Result<bool> {
        let metadata_db = self.client.clone().database(&self.db_metadata_name);
        metadata_db
            .create_collection(&collection_name, None)
            .await
            .expect("Failed to create metadata collection");
        log::info!(
            "Sucessfully created metadata collection: {}",
            &collection_name
        );
        Ok(true)
    }

    pub async fn insert_metadata(
        &self,
        ticker: &(&str, &str, &str, &str, &str), // (ticker, exchange, start_date, collection_name, api)
        collection_name: &str,
    ) -> Result<bool> {
        let metadata_db = self.client.clone().database(&self.db_metadata_name);
        let metadata = TimeseriesMetaDataStruct {
            ticker: ticker.0.to_string(),
            exchange: ticker.1.to_string(),
            collection_name: ticker.3.to_string(),
            source: ticker.4.to_string(),
            from: string_to_datetime(ticker.2).await,
            to: get_current_datetime_bson(),
            last_updated: get_current_datetime_bson(),
        };

        let collection = metadata_db.collection(collection_name);
        collection
            .insert_one(metadata, None)
            .await
            .expect("Could not insert metadata into MongoDB!");

        Ok(true)
    }

    pub async fn ensure_series_collection_exists(
        &self,
        tickers: &[(&str, &str, &str, &str, &str)], // (ticker, exchange, start_date, collection_name, api)
    ) -> Result<bool> {
        let metadata_db = self.client.clone().database(&self.db_metadata_name);
        for ticker in tickers.iter() {
            let metadata_names = metadata_db.list_collection_names(None).await?;
            let collection_name = ticker.3.to_string();
            match metadata_names.contains(&collection_name) {
                true => (),
                false => {
                    self.create_series_collection(ticker.3, &collection_name)
                        .await?;
                    self.create_metadata_collection(&collection_name).await?;
                    // self.insert_metadata(ticker, &collection_name)
                    //     .await?;
                }
            }
        }
        Ok(true)
    }

    pub async fn get_data_from_apis(
        &self,
        tickers: &[(&str, &str, &str, &str, &str)], // (ticker, exchange, start_date, collection_name, api)
    ) -> Result<()> {
        // sort tickers by api source (eod, binance, etc.). Output will be a tuple:
        // ("eod", Vec<(ticker, exchange, start_date, collection_name, api)>)
        // ("binance", Vec<(ticker, exchange, start_date, collection_name, api)>)
        let mut sorted_tickers = Vec::new();
        let datasource_apis: HashSet<&str> = tickers.iter().map(|tuple| tuple.4).collect();
        datasource_apis.into_iter().for_each(|datasource| {
            let filtered_tickers = tickers
                .iter()
                .filter(|tuple| {
                    let tuple = **tuple;
                    tuple.4 == datasource
                }) // tuple.4 == datasource)
                .collect::<Vec<_>>();
            let filtered_tickers_tup = (datasource, filtered_tickers);
            sorted_tickers.push(filtered_tickers_tup);
        });

        for datasource in sorted_tickers.into_iter() {
            match datasource {
                ("eod", _) => {
                    let eod_client = EodApi::new().await;
                    let ticker_infos = datasource.1;
                    let _ = eod_client.batch_get_series_all(&ticker_infos).await;
                    println!("eod");
                }
                _ => log::error!("Datasource: {} is not supported!", datasource.0),
            }
        }

        Ok(())
    }

    pub async fn read_series(&self, tickers: Vec<(&str, &str, &str, &str, &str)>) -> Result<()> {
        let ensure_collection_exists = self.ensure_series_collection_exists(&tickers).await?;
        assert!(ensure_collection_exists, "{}", true);

        self.get_data_from_apis(&tickers).await?;
        Ok(())
    }

    pub async fn insert_series(&self, dfs: Vec<DataFrame>) -> Result<()> {
        // iterate through each df
        for df in dfs.iter() {
            let col_names = df.get_column_names();
            let mut doc_vec = Vec::new();
            // iterate through each row of df
            for i in 0..df.height() {
                let row = df.get_row(i).unwrap();
                let mut doc_row = Document::new();
                // iterate through each element within row of df
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
                            let metadata: OhlcvMetaData =
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
            let collection = self
                .client
                .clone()
                .database(&self.db_name)
                .collection::<Document>("equity_spot_1d");
            collection.insert_many(doc_vec, None).await?;
        }
        Ok(())
    }
}
