// use molly_db::database_service;
// use molly_db::securities::{Equities, Crypto};
// use molly_db::wrappers::WrapperFunctions;
use molly_db::data_apis::EodApi;
use molly_db::database_service::mongodb::MongoDbClient;

#[tokio::main]
async fn main() {
    let eod_client =EodApi::new().await;
    // let eod_ohlcv = eod_client
    //     .batch_get_ohlcv(vec![
    //         ("AAPL", "US", "2023-12-10"),
    //         // ("AAPL", "US", "2023-12-10"),
    //         // ("AAPL", "US", "2023-12-10"),
    //         ("BTC-USD", "CC", "2023-12-10"),
    //         // ("BTC-USD", "CC", "2023-12-10"),
    //         // ("BTC-USD", "CC", "2023-12-10"),
    //     ])
    //     .await
    //     .unwrap();
    // // println!("{:#?}", eod_ohlcv);

    // let mongo_client = MongoDbClient::new().await;
    // mongo_client.insert_series(eod_ohlcv).await.unwrap();

    let eod_intra = eod_client
        .batch_get_intraday_data(vec![
            ("AAPL", "US", "2023-10-01", "5m"),
            ("AAPL", "US", "2023-10-01", "5m"),
            ("AAPL", "US", "2023-10-01", "5m"),
            ("BTC-USD", "CC", "2023-10-01", "5m"),
            ("BTC-USD", "CC", "2023-10-01", "5m"),
            ("BTC-USD", "CC", "2023-10-01", "5m"),
        ])
        .await
        .unwrap();
    // println!("{:#?}", eod_intra);
    let mongo_client = MongoDbClient::new().await;
    mongo_client.insert_series(eod_intra).await.unwrap();

    // let eod_live = eod_client
    //     .batch_get_live_lagged_data(vec![
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("BTC-USD", "CC"),
    //         ("BTC-USD", "CC"),
    //         ("BTC-USD", "CC"),
    //     ])
    //     .await;
    // println!("{:#?}", eod_live.unwrap());
}

// USE RAYLON
// FUNCTION TO CLEAN DATAFRAME I.E. DROP NULLS AND CHANGE TYPES OF COLUMNS
