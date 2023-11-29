// use molly_db::database_service;
use molly_db::securities::Equities;
use molly_db::wrappers::WrapperFunctions;

#[tokio::main]
async fn main() {
   let thing = Equities::new().await;
   let thing2 = thing.get_series_metadata("AAPL", "US").await;
   println!("{:?}", thing2.unwrap());

   let wrapper_client = WrapperFunctions::new().await;
   let wrapper_test = wrapper_client.batch_get_ohlcv("AAPL", "US", "2023-10-01", "2023-11-01").await;
   println!("{:?}", wrapper_test.unwrap());
}

// finish batch_get_ohlcv() and parallel_http_request() 

// use molly_db::database_service;
// use molly_db::wrappers;

// use env_logger::Env;
// use molly_db::wrappers::OhlcvMetadata;
// use tokio;

// #[tokio::main]
// async fn main() {
//     // configure env_logger
//     env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
//     // let request_client = wrappers::create_reqwest_client().await;

//     // connections
//     let client = database_service::connection()
//         .await
//         .expect("Failed to establish connection to DB");

//     //GET OHLCV data
//     let eod_client = wrappers::create_reqwest_client().await;
//     let ohlcv = wrappers::get_ohlc(&eod_client, "AAPL", "US", "2023-10-01", "2023-11-20")
//         .await
//         .unwrap();
//     // println!("{:#?}", &ohlcv);

//     // INSERT_MANY records into the database
//     let _ = database_service::insert_timeseries(
//         &client,
//         ohlcv,
//         "equity",
//         "spot",
//         "1d",
//         database_service::OhlcGranularity::Hours,
//     )
//     .await;

//     // READ_MANY records from the database
//     let metadata: OhlcvMetadata = wrappers::metadata_info().await.unwrap();
//     let records = database_service::read_timeseries(
//         &client,
//         "2023-10-04",
//         "2023-11-01",
//         metadata,
//         "equity",
//         "spot",
//         "1d",
//         database_service::OhlcGranularity::Hours,
//     )
//     .await;
//     println!("{:#?}", records.unwrap());

//     // GET TICKER FUNDAMENTALS
//     // let fundmental_doc = wrappers::get_ticker_generals(&request_client, "AAPL", "US").await;
//     // println!("{:#?}", fundmental_doc.unwrap())
// }

// // change api key to be demo
// // make more apis
// // finishh insert_doc() function and test it
// // start read_doc() function
// // change String -> some smart pointer
// // standardise error messages e.g {:#?} or {:?} and format!()
// // standardise function outputs ie Result<()>
