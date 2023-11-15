use crate::utility_functions::string_to_datetime;
use crate::wrappers::Ohlcv;

use mongodb::{ Client, Collection, bson, options::{CreateCollectionOptions, TimeseriesGranularity, TimeseriesOptions, FindOptions}};
use dotenv::dotenv;
use std::env;
use bson::{doc, DateTime};
use futures::StreamExt;
use anyhow::Result;

const DATABASE_NAME: &str = "molly_db";
pub enum OhlcGranularity {
    Hours,
    Minutes,
    Seconds
}

pub struct TimeseriesMetaDataStruct {
    series_type: String,
    isin: String,
    ticker: String,
    source: String,
    exchange: String,
    series_start: DateTime,
    series_end: DateTime,
    last_updated: DateTime
}

/*
    Establish connection to MongoDB
*/
pub async fn connection() -> Result<Client>  {
    dotenv().ok();
    let client_url = env::var("MONGODB_URI").expect("Could not parse MongoDB URI from .env");
    let client = Client::with_uri_str(client_url).await.expect("Could not create MongoDB Client from DB URI");
    log::info!("Established Client for MongoDB!");
    Ok(client)
}

/*
    Ensure collection and sister metadata collection exists
*/
pub async fn ensure_collection_exists(client: &Client, dtype: &str, dformat: &str, dfreq: &str, granularity: OhlcGranularity) {
    let db = client.database(DATABASE_NAME);
    let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
    let metadata_collection_name = format!("{}_{}_{}_meta", dtype, dformat, dfreq);
    let collections = db.list_collection_names(None).await.expect("Failed to list collection");

    // create collection and metadata sister collection if it doesn't exist in DB
    match collections.contains(&collection_name) {
        true => (),
        false => {
            // create collection
            log::warn!("{}", format!("Timeseries collection and sister metadata collection for {} does not exist", collection_name));
            let timeseries_options = TimeseriesOptions::builder()
                .time_field("datetime".to_string())
                .meta_field(Some("metadata".to_string()))
                .granularity(
                    match granularity {
                        OhlcGranularity::Hours => Some(TimeseriesGranularity::Hours),
                        OhlcGranularity::Minutes => Some(TimeseriesGranularity::Minutes),
                        OhlcGranularity::Seconds => Some(TimeseriesGranularity::Seconds),
                    }
                )
                .build();
            let options = CreateCollectionOptions::builder()
                .timeseries(timeseries_options)
                .build();
            db.create_collection(&collection_name, Some(options)).await
                .expect("Failed to create timeseries collection");
            log::info!("Sucessfully created timeseries collection: {}", &collection_name);

            // create metadata collection
            db.create_collection(&metadata_collection_name, None).await
                .expect(format!("Failed to create metadata collection for {}", &metadata_collection_name).as_str());
            log::info!("Successfully created timeseries collection metadata: {}", &metadata_collection_name);
        }
    };
}

/*
    Function creates a collection if collection doesn't exist and then inserts records
*/

// MAKE THIS FUNCTION RETURN A BOOL ONCE DONE
pub async fn insert_timeseries(client: &Client, records: Vec<Ohlcv>, dtype: &str, dformat: &str, dfreq: &str, granularity: OhlcGranularity) -> Result<()> {
    
    let db = client.database(DATABASE_NAME);
    let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
    let collection_metadata_name = format!("{}_{}_{}_meta", dtype, dformat, dfreq);
    if records.len() == 0 {
        log::error!("insert_timeseries() got a records length of zero");
        println!("{:#?}", false) // change this to return
    }

    ensure_collection_exists(client, dtype, dformat, dfreq, granularity).await;
    let timeseries_start = records.get(0)
        .expect("insert_timeseries() could not get start of series")
        .datetime; // 2023-10-02 0:00:00.0 +00:00:00
    let timeseries_end = records.last()
        .expect("insert_timeseries() could not get end of series")
        .datetime; // 2023-11-01 0:00:00.0 +00:00:00,

    // create unique series identifier from the timeseries metadata
    let mut metadata_series_filter = doc! {};
    let _ = &records.last()
        .expect("insert_timeseries() could not get end of series")
        .metadata
        .iter()
        .map(|(key, value)| metadata_series_filter.insert(key, value));

    // use unique identifier to search for metadata for corresponding series
    let metadata_series_count = db.collection::<Collection<TimeseriesMetaDataStruct>>(&collection_metadata_name)
        .count_documents(metadata_series_filter, None)
        .await
        .expect("insert_timeseries() errored when counting metadocument");

    match metadata_series_count {
        0 => {
            todo!()
        },
        1 => {
            todo!()
        },
        _ => {
            log::error!("insert_timeseries() has more than one metadata document associated with {} with unique id", collection_name) // IMPORTANT: change unique id for something dynamic
        }
    }

    println!("ts start: {:#?}", timeseries_start);
    println!("ts end: {:#?}", timeseries_end);

    // // insert if collection exists
    // let my_collection: Collection<Ohlcv> = db.collection(&collection_name);
    // my_collection.insert_many(records, None)
    //     .await
    //     .expect(format!("Failed to insert OHLCV to {} collection", &collection_name).as_str());
    // log::info!("Successfully inserted collection {}", &collection_name);
    Ok(())
}

/*
    Read in collection from DB based on filters and serialise into a dataframe
*/
pub async fn read_many(client: &Client, start_date: &str, end_date: &str, ticker: &str, dtype: &str, dformat: &str, dfreq: &str) -> Result<Vec<Ohlcv>> {
    let db = client.database(DATABASE_NAME);
    let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
    let collections = db.list_collection_names(None).await.expect("Failed to list collection");
    
    // check collection exists
    match collections.contains(&collection_name) {
        true => {
            log::info!("Found collection: {}", collection_name);
            let my_collection = db.collection(&collection_name);
            let start_date = string_to_datetime(start_date).await;
            let end_date = string_to_datetime(end_date).await;
            let filter = doc! { 
                "metadata.ticker": ticker, // in future make this filter condition more dynamic
                "datetime": { 
                    "$gte": start_date,
                    "$lte": end_date
                }
            };
            let options = FindOptions::builder()
                .sort(doc! { "datetime": 1 })
                .build();
            let mut cursor = my_collection.find(filter, options)
                .await
                .expect("Failed to unwrap Ohlcv Cursor, check filter condition is valid");
            let mut results: Vec<Ohlcv> = Vec::new();
            while let Some(my_collection) = cursor.next().await {
                results.push(my_collection?)
            }
            log::info!("Sucessfully read documents from DB");
            Ok(results)
        },
        false => {
            panic!("{} collection does not exist. Please enter correct collection name.", collection_name) // change this to recoverable error
        }
    }
}



// /*
//     Function creates a collection if collection doesn't exist and then inserts records
// */
// pub async fn insert_timeseries(client: &Client, records: Vec<Ohlcv>, dtype: &str, dformat: &str, dfreq: &str, granularity: OhlcGranularity) -> Result<()> {
//     if records.len() == 0 {
//         log::error!("insert_timeseries() got a records length of zero");
//         println!("{:#?}", false) // change this to return
//     }
//     let db = client.database(DATABASE_NAME);
//     let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
//     let collection_metadata_name = format!("{}_{}_{}_meta", dtype, dformat, dfreq);
//     let collections = db.list_collection_names(None).await.expect("Failed to list collection");

//     // create collection if it doesn't exist in DB
//     match collections.contains(&collection_name) {
//         true => (),
//         false => {
//             log::error!("{}", format!("Collection {} does not exist", collection_name));
//             let timeseries_options = TimeseriesOptions::builder()
//                 .time_field("datetime".to_string())
//                 .meta_field(Some("metadata".to_string()))
//                 .granularity(
//                     match granularity {
//                         OhlcGranularity::Hours => Some(TimeseriesGranularity::Hours),
//                         OhlcGranularity::Minutes => Some(TimeseriesGranularity::Minutes),
//                         OhlcGranularity::Seconds => Some(TimeseriesGranularity::Seconds),
//                     }
//                 )
//                 .build();
//             let options = CreateCollectionOptions::builder()
//                 .timeseries(timeseries_options)
//                 .build();
//             db.create_collection(&collection_name, Some(options)).await
//                 .expect("Failed to create timeseries collection");
//             log::info!("{}", format!("Sucessfully create timeseries collection: {}", &collection_name))
//         }
//     };
    
//     // insert if collection exists
//     let my_collection: Collection<Ohlcv> = db.collection(&collection_name);
//     my_collection.insert_many(records, None)
//         .await
//         .expect(format!("Failed to insert OHLCV to {} collection", &collection_name).as_str());
//     log::info!("Successfully inserted collection");
//     Ok(())
// }