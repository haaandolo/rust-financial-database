pub mod mongodb;
pub use mongodb::MongoDbClient;

// use crate::{utility_functions::string_to_datetime, wrappers::{OhlcvMetadata, DocumentMetaData}};
// use crate::wrappers::Ohlcv;

// // use chrono::format::format;
// use mongodb::{ Client, bson, options::{CreateCollectionOptions, TimeseriesGranularity, TimeseriesOptions, FindOptions, UpdateOptions}};
// use dotenv::dotenv;
// // use std::collections::HashMap;
// use std::env;
// use bson::{doc, DateTime, Document};
// use futures::StreamExt;
// use anyhow::Result;
// use chrono::Utc;
// use serde::{Serialize, Deserialize};
// use struct_iterable::Iterable;

// const DATABASE_NAME: &str = "molly_db";
// pub enum OhlcGranularity {
//     Hours,
//     Minutes,
//     Seconds
// }
// #[derive(Debug, Serialize, Deserialize, Clone)]

// pub struct TimeseriesMetaDataStruct {
//     series_type: String,
//     isin: String,
//     ticker: String,
//     source: String,
//     exchange: String,
//     time_start: DateTime,
//     time_end: DateTime,
//     last_updated: DateTime
// }

// // pub struct DocumentStruct {}

// /*
//     Establsh connection to MongoDB
// */
// pub async fn connection() -> Result<Client>  {
//     dotenv().ok();
//     let client_url = env::var("MONGODB_URI").expect("Could not parse MongoDB URI from .env");
//     let client = Client::with_uri_str(client_url).await.expect("Could not create MongoDB Client from DB URI");
//     log::info!("Established Client for MongoDB!");
//     Ok(client)
// }

// /*
//     Ensure collection and sister metadata collection exists
// */
// pub async fn ensure_timeseries_collection_exists(client: &Client, dtype: &str, dformat: &str, dfreq: &str, granularity: OhlcGranularity) {
//     let db = client.database(DATABASE_NAME);
//     let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
//     let metadata_collection_name = format!("{}_{}_{}_meta", dtype, dformat, dfreq);
//     let collections = db.list_collection_names(None).await.expect("Failed to list collection");

//     // create collection and metadata sister collection if it doesn't exist in DB
//     match collections.contains(&collection_name) {
//         true => (),
//         false => {
//             // create collection
//             log::warn!("{}", format!("Timeseries collection and sister metadata collection for {} does not exist", collection_name));
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
//             log::info!("Sucessfully created timeseries collection: {}", &collection_name);

//             // create metadata collection
//             db.create_collection(&metadata_collection_name, None).await
//                 .expect(format!("Failed to create metadata collection for {}", &metadata_collection_name).as_str());
//             log::info!("Successfully created timeseries collection metadata: {}", &metadata_collection_name);
//         }
//     };
// }

// /*
//     Checks the continuity of timeseries
// */
// pub async fn check_continuity(record_start: & DateTime, record_end: &DateTime, new_start: & DateTime, new_end: &DateTime) -> bool {
//     if new_start <= record_end && record_start <= new_end {
//         return true
//     }
//     return false
// }

// /*
//     Function creates a collection if collection doesn't exist and then inserts records
// */
// // MAKE THIS FUNCTION RETURN A BOOL ONCE DONE
// pub async fn insert_timeseries(client: &Client, records: Vec<Ohlcv>, dtype: &str, dformat: &str, dfreq: &str, granularity: OhlcGranularity) -> Result<()> {
//     // get database and relevant names 
//     let db = client.database(DATABASE_NAME);
//     let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
//     let collection_metadata_name = format!("{}_{}_{}_meta", dtype, dformat, dfreq);
//     let collection = db.collection::<Ohlcv>(&collection_name);
//     let collection_metadata = db.collection::<TimeseriesMetaDataStruct>(&collection_metadata_name);

//     if records.len() == 0 {
//         log::error!("insert_timeseries() got a records length of zero");
//     }

//     // ensure collections exists. Note, these variables get used a lot, so use references
//     ensure_timeseries_collection_exists(client, dtype, dformat, dfreq, granularity).await;
//     let timeseries_start = records.get(0)
//         .expect("insert_timeseries() could not get start of series")
//         .datetime; // 2023-10-02 0:00:00.0 +00:00:00
//     let timeseries_end = records.last()
//         .expect("insert_timeseries() could not get end of series")
//         .datetime; // 2023-11-01 0:00:00.0 +00:00:00,
//     let timeseries_metadata = &records.last()
//         .expect("insert_timeseries() could not get end of series")
//         .metadata;

//     // create unique series identifier from the timeseries metadata. Note, the timeseries
//     // metadata field is a unique identifier. Hence, we can retrieve this and use it to 
//     // find the sister metadata document associated with the series
//     let mut metadata_series_filter = doc! {};
//     let _ = &timeseries_metadata.iter()
//         .map(|(key, value )| metadata_series_filter.insert(key, value.downcast_ref::<&str>()));

//     // use unique identifier to search for metadata for corresponding series
//     let metadata_series_count = collection_metadata
//         .count_documents(metadata_series_filter.clone(), None)
//         .await
//         .expect("insert_timeseries() errored when counting metadocument");

//     // if metadata count is zero then just insert. else if count is one, then documents 
//     // associated with series already exists in collection. Hence, we use the metadata
//     // to insert relevant rows. if count is greater than one then log a critical error.
//     match metadata_series_count {
//         0 => {
//             // just insert if no collection or metadata exists
//             let timeseries_metadata = TimeseriesMetaDataStruct {
//                 series_type: "ticker-series".to_string(),
//                 isin: timeseries_metadata.isin.to_string(),
//                 ticker: timeseries_metadata.ticker.to_string(),
//                 source: timeseries_metadata.source.to_string(),
//                 exchange: timeseries_metadata.source.to_string(),
//                 time_start: timeseries_start,
//                 time_end: timeseries_end,
//                 last_updated: bson::DateTime::from_chrono(Utc::now())
//             };
//             collection.insert_many(records, None)
//                 .await
//                 .expect(format!("Failed to insert OHLCV to {} collection", &collection_name).as_str());
//             collection_metadata.insert_one(timeseries_metadata, None)
//                 .await
//                 .expect(format!("insert_timeseries failed to insert metadata for collection {}", &collection_metadata_name).as_str());
//            },
//         1 => {
//             // if meta record for collection exists then timeseries already in db
//             let timeseries_metadata: TimeseriesMetaDataStruct = collection_metadata 
//                 .find_one(metadata_series_filter.clone(), None)
//                 .await
//                 .expect(format!("insert_timeseries() could not unwrap Result for {:#?}", &timeseries_metadata).as_str())
//                 .expect(format!("insert_timeseries() could not unwrap Option for {:#?}", &timeseries_metadata).as_str());
//             let timeseries_metadata_start = timeseries_metadata.time_start;
//             let timeseries_metadata_end = timeseries_metadata.time_end;
//             let timeseries_continuity_check = check_continuity(
//                 &timeseries_metadata_start, &timeseries_metadata_end, &timeseries_start, &timeseries_end
//             ).await;
//             match timeseries_continuity_check {
//                 true => {
//                     // filter input record for rows that are greater than end date stated in metadata
//                     let timeseries_filtered: Vec<Ohlcv> = records
//                         .into_iter()
//                         .filter(|record| &record.datetime > &timeseries_metadata_end || &record.datetime < &timeseries_metadata_start)
//                         .collect();
//                     if timeseries_filtered.len() == 0 {
//                         log::error!("insert_timeseries() db already has timeseries data for specified date range for {:#?} ", &timeseries_metadata);
//                         panic!("insert_timeseries() timeseries_filtered has a length of zero") // CHANGE THIS INTO SOMETHING THAT WONT CRASH PROGRAM. CURRENTLY CANT INSERT EMPTY ARRAY INTO INSERT_MANY()
//                     }
//                     collection.insert_many(timeseries_filtered, None)
//                         .await
//                         .expect(format!("insert_timeseries() failed to insert OHLCV to {} collection. Metadata: {:#?}", &collection_name, &timeseries_metadata).as_str());
//                     let update_options = UpdateOptions::builder().upsert(false).build();
//                     collection_metadata.update_one(
//                         metadata_series_filter.clone(), 
//                         doc! {"$set": { "time_start": &timeseries_start,"time_end": &timeseries_end, "last_updated": bson::DateTime::from_chrono(Utc::now())}}, 
//                         Some(update_options)) 
//                         .await
//                         .expect(format!("insert_timeseries() could not unwrap Result for {:#?}", &timeseries_metadata).as_str());
//                 },
//                 false => {
//                     log::error!("insert_timeseries() is trying to insert discontiguous timeseries for {:#?}", &timeseries_metadata)
//                 }
//             }
//         },
//         _ => {
//             log::error!("insert_timeseries() has more than one metadata document associated with {:#?} with unique id", &timeseries_metadata) 
//         }
//     }
//     Ok(())
// }

// /*
//     Function to ensure document collection exists
//  */
// pub async fn ensure_document_collection_exists(client: &Client, dtype: &str, dformat: &str, dfreq: &str) {
//     let db = client.database(DATABASE_NAME);
//     let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
//     let collections = db.list_collection_names(None).await.expect("ensure_document_collection_exists() failed to list collection");

//     // create collection and metadata sister collection if it doesn't exist in DB
//     match collections.contains(&collection_name) {
//         true => (),
//         false => {
//             log::warn!("Document collection did not exist for {}", &collection_name);
//             db.create_collection(&collection_name, None).await
//                 .expect(format!("ensure_document_collection_exists failed to make collection: {}", &collection_name).as_str());
//             log::info!("ensure_document_collection_exists made collection {}", &collection_name);
//         }
//     }
// }

// /*
//     Read in collection from DB based on filters and serialise into a dataframe
// */
// pub async fn read_timeseries(client: &Client, start_date: & str, end_date: & str, metadata: OhlcvMetadata, dtype: &str, dformat: &str, dfreq: &str, granularity: OhlcGranularity) -> Result<Vec<Ohlcv>> {
//     // get database info
//     ensure_timeseries_collection_exists(client, dtype, dformat, dfreq, granularity).await;
//     let db = client.database(DATABASE_NAME);
//     let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
//     let collection_metadata_name = format!("{}_{}_{}_meta", dtype, dformat, dfreq);
//     let collection = db.collection::<Ohlcv>(&collection_name);
//     let collection_metadata = db.collection::<TimeseriesMetaDataStruct>(&collection_metadata_name);

//     // get series metadata
//     let mut metadata_series_filter = doc!{};
//     let _ = metadata.iter()
//         .map(|(key, value)| metadata_series_filter.insert(key, value.downcast_ref::<&str>()));
//     let metadata_series_count = collection_metadata
//         .count_documents(metadata_series_filter.clone(), None)
//         .await
//         .expect("insert_timeseries() errored when counting metadocument");    

//     // depending on metadata_series_count, return series or return error
//     match metadata_series_count {
//         0 => {
//             log::info!("read_timeseries() got metadata_series_count of zero for {:#?}", &metadata_series_filter);
//             panic!("read_timeseries() got metadata_series_count of zero")
//         },
//         1 => {
//             log::info!("Found collection: {}", collection_name);
//             let start_date = string_to_datetime(start_date).await;
//             let end_date = string_to_datetime(end_date).await;
//             let filter = doc! { 
//                 "metadata": metadata_series_filter, 
//                 "datetime": {
//                     "$gte": start_date,
//                     "$lte": end_date
//                 }
//             };
//             let options = FindOptions::builder()
//                 .sort(doc! { "datetime": 1 })
//                 .build();
//             let mut cursor = collection.find(filter, options)
//                 .await
//                 .expect("Failed to unwrap Ohlcv Cursor, check filter condition is valid");
//             let mut results: Vec<Ohlcv> = Vec::new();
//             while let Some(collection) = cursor.next().await {
//                 results.push(collection?)
//             }
//             log::info!("Sucessfully read documents from DB");
//             Ok(results)
//         },
//         _ => {
//             log::error!("Got a collection greater than one for: {}", &metadata_series_filter);
//             panic!("read_timeseries got a metadata_series_count count greater than one") 
//         }
//     }
// }

// /*
//     Function to insert documents, this is for non timeseries based data i.e. financial statements
// */
// pub async fn insert_document<T>(client: &Client, dtype: &str, dformat: &str, dfreq: &str, document_data: Vec<String>, metadata: DocumentMetaData) -> Result<()> {
//     ensure_document_collection_exists(client, dtype, dformat, dfreq).await;
//     let db = client.database(DATABASE_NAME);
//     let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
//     let collection = db.collection::<Document>(&collection_name);

//     // get filter data filter condition. Note, this is the same as the metadata field
//     // when inserting documents
//     let mut document_filter = doc! {};
//     let _ = &metadata.iter()
//         .map(|(key, value )|document_filter.insert(key, value.downcast_ref::<&str>()));

//     // use unique identifier to search for metadata for corresponding series
//     let document_count = collection
//         .count_documents(document_filter.clone(), None)
//         .await
//         .expect("insert_timeseries() errored when counting metadocument");
    
//     // serialise document data to bson object
//     let document_serialised = bson::to_bson(&document_data)
//         .expect(format!("insert_document() could not serialise document data for {:?}", &document_filter).as_str());
//     let document_bson = document_serialised.as_document()
//         .expect(format!("insert_document() could not convert {:?} to document", &document_filter).as_str()); // CHANGE TO {:#?} ?
//     let document_record = doc! {
//         "metadata": &document_filter,
//         "data": document_bson,
//         "last_updated": bson::DateTime::from_chrono(Utc::now()),
//     };

//     match document_count {
//         0 => {
//             collection.insert_one(document_record, None).await
//                 .expect(format!("insert_document() failed to insert document for {:?}", &document_filter).as_str()); // CHANGE TO {:#?} ?
//             log::info!("Successfully insert document {:?}", &document_filter)
//         },
//         1 => {
//             let update_options = UpdateOptions::builder().upsert(false).build();
//             collection.update_one(
//                 document_filter.clone(),
//                 doc! {"$set": { "data": document_record, "last_updated": bson::DateTime::from_chrono(Utc::now())}},
//                 Some(update_options))
//             .await
//             .expect(format!("insert_document() could not update document {:?}", &document_filter).as_str()); // CHANGE TO {:#?}
//             log::info!("Sucessfully updated document {:?}", &document_filter)
//         },
//         _ => {
//             log::error!("Got a document count of greater than one for: {:#?}", &collection_name)
//         }
//     }
//     Ok(())
// }