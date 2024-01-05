use anyhow::Result;
use bson::{doc, Bson, Document};
use chrono::Utc;
use dotenv::dotenv;
use futures::TryStreamExt;
use mongodb::{
    bson,
    options::{
        CreateCollectionOptions, FindOptions, TimeseriesGranularity, TimeseriesOptions,
        UpdateOptions, FindOneOptions
    },
    Client,
};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::{rc::Rc, env};


use crate::data_apis::EodApi;
use crate::models::eod_models::{OhlcvMetaData, TimeseriesMetaDataStruct, MongoTickerParams, Ohlcv};
use crate::utility_functions::{get_current_datetime_bson, string_to_datetime, has_business_day_between};

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
        collection_name: &str,
    ) -> Result<bool> {
        let series_db = self.client.clone().database(&self.db_name);
        let timeseries_options = TimeseriesOptions::builder()
            .time_field("datetime".to_string())
            .meta_field(Some("metadata".to_string()))
            .granularity(match collection_name.chars().last() {
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
        Ok(true)
    }

    pub async fn insert_metadata(
        &self,
        ticker: &MongoTickerParams,
    ) -> Result<TimeseriesMetaDataStruct> {
        let metadata_db = self.client.clone().database(&self.db_metadata_name);
        let collection = metadata_db.collection(&ticker.series_collection_name);
        let current_date = get_current_datetime_bson();
        let start_date = string_to_datetime("1970-01-01");
        let metadata = TimeseriesMetaDataStruct {
            ticker: ticker.ticker.clone(),
            exchange: ticker.exchange.clone(),
            series_collection_name: ticker.series_collection_name.clone(),
            source: ticker.source.clone(),
            from: start_date,
            to: current_date,
            last_updated: current_date,
        };

        collection
            .insert_one(metadata.clone(), None)
            .await
            .expect("Could not insert metadata into MongoDB!");

        Ok(metadata)
    }

    pub async fn ensure_series_collection_exists(
        &self,
        tickers: &[MongoTickerParams], 
    ) -> Result<bool> {
        let metadata_db = self.client.clone().database(&self.db_metadata_name);
        for ticker in tickers.iter() {
            let metadata_names = metadata_db.list_collection_names(None).await?;
            let collection_name = &ticker.series_collection_name;
            match metadata_names.contains(collection_name) {
                true => (),
                false => {
                    self.create_series_collection(collection_name)
                        .await.unwrap_or_else(|_| panic!("Could not create collection: {} for {}", collection_name, ticker.ticker));
                    self.create_metadata_collection(collection_name).await
                        .unwrap_or_else(|_| panic!("Could not create metadata collection: {} for {}", collection_name, ticker.ticker));
                }
            }
        }
        Ok(true)
    }

    pub async fn get_data_from_apis(
        &self,
        tickers: Vec<MongoTickerParams>,
    ) -> Result<Vec<DataFrame>> {
        // sort tickers by api source (eod, binance, etc.). Output will be a tuple: ("eod", Vec<MongoTickerParams>)
        let mut sorted_tickers = Vec::new();
        let datasource_apis: HashSet<&String> = tickers.iter().map(|tuple| &tuple.source).collect();
        for datasource in datasource_apis.iter() {
            let filtered_tickers = tickers
                .iter()
                .filter(|params| {
                    let params = *params;
                    params.source == **datasource
                })
                .cloned()
                .collect::<Vec<MongoTickerParams>>();
            let filtered_tickers_tup = (datasource.as_str(), filtered_tickers);
            sorted_tickers.push(filtered_tickers_tup);
        }

        let mut dfs = Vec::new();
        for datasource in sorted_tickers.into_iter() {
            match datasource {
                ("eod", _) => {
                    let eod_client = EodApi::new().await;
                    let ticker_infos = datasource.1;
                    let eod_dfs = eod_client.batch_get_series_all(ticker_infos).await
                        .expect("get_data_from_apis() Could not get data from EOD API!");
                    dfs.extend(eod_dfs);
                }
                _ => log::error!("Datasource: {} is not supported!", datasource.0),
            }
        }

        Ok(dfs)
    }

    // pub async fn read_series(&self, tickers: Vec<(&str, &str, &str, &str, &str)>) -> Result<()> {
    //     Ok(())
    // }

    pub async fn insert_series(&self, dfs: Vec<DataFrame>) -> Result<bool> {
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
                            let datetime = string_to_datetime(string);
                            doc_row.insert(name.to_string(), Bson::DateTime(datetime));
                        }
                        ("date", Some(AnyValue::Utf8(string))) => {
                            let date = string_to_datetime(string);
                            doc_row.insert("datetime".to_string(), Bson::DateTime(date));
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

            let document = doc_vec.pop().expect("Could not pop collection name!");
            let metadata = document.get("metadata").expect("Could not get metadata!");
            let metadata = metadata.as_document().expect("Metadata is not a document!");
            let collection_name = metadata.get("metadata_collection_name").expect("Could not get metadata_collection_name!");
            let collection_name = collection_name.as_str().expect("Could not convert metadata_collection_name to string!"); 

            let collection = self
                .client
                .clone()
                .database(&self.db_name)
                .collection::<Document>(collection_name);
            collection.insert_many(doc_vec, None).await
                .expect("insert_many() failed at inserting docs into collection");
        }

        Ok(true)
    }

    pub async fn update_metadata_dates(&self, tickers: &[MongoTickerParams]) -> Result<()> {
        let metadata_db = self.client.clone().database(&self.db_metadata_name);
        let series_db = self.client.clone().database(&self.db_name);
        for ticker in tickers.iter() {
            let metadata_collection = metadata_db.collection::<TimeseriesMetaDataStruct>(&ticker.series_collection_name);
            let mut series_metadata = metadata_collection
                .find(None, None)
                .await?;

            let mut metadata_vec = Vec::new();
            while let Some(result) = series_metadata.try_next().await? {
                metadata_vec.push(result);
            }

            for metadata in metadata_vec.iter() {
                let series_filter = doc! {
                    "metadata.ticker": &metadata.ticker,
                    "metadata.exchange": &metadata.exchange,
                    "metadata.metadata_collection_name": &metadata.series_collection_name,
                    "metadata.source": &metadata.source,
                };

                let metadata_filter = doc! {
                    "ticker": &metadata.ticker,
                    "exchange": &metadata.exchange,
                    "series_collection_name": &metadata.series_collection_name,
                    "source": &metadata.source,
                };

                let mut min_max_dates = HashMap::new();
                // get max date
                let series_collection = series_db.collection::<Document>(&metadata.series_collection_name);
                let max_date_options= FindOneOptions::builder().sort(doc! { "datetime": -1 }).build();
                let max_date_result = series_collection.find_one(Some(series_filter.clone()), max_date_options).await?;
                if let Some(max_date_row) = max_date_result {
                    let _max_date= max_date_row.get("datetime").expect("Could not get datetime!");
                    min_max_dates.insert("max_date", _max_date.clone()); // FIX THIS CLONE
                }

                // get min date
                let min_date_options= FindOneOptions::builder().sort(doc! { "datetime": 1 }).build();
                let min_date_result = series_collection.find_one(Some(series_filter), min_date_options).await?;
                if let Some(min_date_row) = min_date_result {
                    let _min_date = min_date_row.get("datetime").expect("Could not get datetime!");
                    min_max_dates.insert("min_date", _min_date.clone()); // FIX THIS CLONE
                }

                metadata_collection
                .update_one(
                    metadata_filter,
                    doc! {
                        "$set": {
                            "from": min_max_dates.get("min_date").expect("Could not get max_date!"),
                            "to": min_max_dates.get("max_date").expect("Could not get max_date!"),
                            "last_updated": get_current_datetime_bson(),
                        }
                    },
                    None,
                )
                .await?;
            }
        }  
        Ok(())
    }

    pub async fn run(&self, tickers: Vec<(&str, &str, &str, &str, &str, &str)>) -> Result<()> {
        // convert str to mongodb params
        let tickers = tickers.into_iter()
            .map(|(ticker, exchange, collection_name,source, from, to)| MongoTickerParams {
                ticker: ticker.to_string(),
                exchange: exchange.to_string(),
                series_collection_name: collection_name.to_string(),
                source: source.to_string(),
                from: string_to_datetime(from),
                to: string_to_datetime(to),
            })
            .collect::<Vec<MongoTickerParams>>();
        let tickers = Rc::new(tickers);

        // ensure collection exits
        let ensure_collection_exists = self.ensure_series_collection_exists(&tickers).await?;
        assert!(ensure_collection_exists, "ensure_series_collection_exists() failed!");

        // segragate tickers into new and existing
        let mut new_tickers = Vec::new();
        let mut existing_tickers = Vec::new();
        for ticker in tickers.iter() {
            let metadata_collection = self
                .client
                .clone()
                .database(&self.db_metadata_name)
                .collection::<TimeseriesMetaDataStruct>(&ticker.series_collection_name);

            let metadata_filter = doc! {
                "ticker": &ticker.ticker,
                "exchange": &ticker.exchange,
                "series_collection_name": &ticker.series_collection_name,
                "source": &ticker.source,
            };

            let mut series_metadata = metadata_collection
                .find(metadata_filter.clone(), None)
                .await?;

            let mut metadata_vec = Vec::new();
            while let Some(result) = series_metadata.try_next().await? {
                metadata_vec.push(result);
            }

            match metadata_vec.len() {
                0 => {
                    let inserted_metadata = self.insert_metadata(ticker).await?;
                    let ticker_param_updated = MongoTickerParams {
                        ticker: inserted_metadata.ticker,
                        exchange: inserted_metadata.exchange,
                        series_collection_name: inserted_metadata.series_collection_name,
                        source: inserted_metadata.source,
                        from: inserted_metadata.from,
                        to: inserted_metadata.to,
                    };
                    new_tickers.push(ticker_param_updated);
                },
                1 => {
                    let metadata = metadata_vec.pop().expect("Could not pop metadata!");
                    let new_from = metadata.to;
                    let current_date = get_current_datetime_bson();
                    let is_day_between = has_business_day_between(new_from, current_date);
                    if is_day_between {
                        let ticker_param_updated = MongoTickerParams {
                            ticker: metadata.ticker,
                            exchange: metadata.exchange,
                            series_collection_name: metadata.series_collection_name,
                            source: metadata.source,
                            from: new_from,
                            to: current_date,
                        };
                        existing_tickers.push(ticker_param_updated);
                    }
                }    
                _ => println!("run() found more than one metadata document for ticker: {}, exchange: {}, source: {}", ticker.ticker, ticker.exchange, ticker.source),
            }
        }

        // for new tickers, create metadata and get data and insert into db
        let dfs_new = self.get_data_from_apis(new_tickers).await?;
        self.insert_series(dfs_new).await?;

        // for existing tickers, update metadata and get data and insert into db
        let dfs_existing = self.get_data_from_apis(existing_tickers).await?;
        self.insert_series(dfs_existing).await?;

        // update metadata dates
        self.update_metadata_dates(&tickers).await?;

        // read series

        Ok(())
    }
}
