use crate::utility_functions::string_to_datetime;
use crate::wrappers::Ohlcv;

use mongodb::{ Client, Collection, bson, options::{CreateCollectionOptions, TimeseriesGranularity, TimeseriesOptions, FindOptions}};
use dotenv::dotenv;
use std::env;
use bson::doc;
use futures::StreamExt;
use anyhow::Result;

const DATABASE_NAME: &str = "molly_db";
pub enum OhlcGranularity {
    Hours,
    Minutes,
    Seconds
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
            log::error!("{}", format!("Timeseries collection and sister metadata collection for {} collection does not exist", collection_name));
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
            log::info!("{}", format!("Sucessfully created timeseries collection: {}", &collection_name));

            // create metadata collection
            db.create_collection(&metadata_collection_name, None).await
                .expect(format!("Failed to create metadata collection for {}", &metadata_collection_name).as_str());
            log::info!("{}", format!("Successfully created timeseries collection metadata: {}", &metadata_collection_name));
        }
    };
}

/*
    Function creates a collection if collection doesn't exist and then inserts records
*/
pub async fn insert_timeseries(client: &Client, records: Vec<Ohlcv>, dtype: &str, dformat: &str, dfreq: &str, granularity: OhlcGranularity) -> Result<()> {
    
    if records.len() == 0 {
        log::error!("insert_timeseries() got a records length of zero");
        println!("{:#?}", false) // change this to return
    }
    ensure_collection_exists(client, dtype, dformat, dfreq, granularity).await;
    
    let db = client.database(DATABASE_NAME);
    let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);

    // insert if collection exists
    let my_collection: Collection<Ohlcv> = db.collection(&collection_name);
    my_collection.insert_many(records, None)
        .await
        .expect(format!("Failed to insert OHLCV to {} collection", &collection_name).as_str());
    log::info!("Successfully inserted collection");
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