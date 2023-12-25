// use molly_db::database_service;
// use molly_db::securities::{Equities, Crypto};
// use molly_db::wrappers::WrapperFunctions;
use molly_db::data_apis::EodApi;

#[tokio::main]
async fn main() {
    let eod_client =EodApi::new().await;
    // let eod_ohlcv = eod_client
    //     .batch_get_ohlcv(vec![
    //         ("AAPL", "US", "2023-10-01"),
    //         ("AAPL", "US", "2023-10-01"),
    //         ("AAPL", "US", "2023-10-01"),
    //         ("BTC-USD", "CC", "2023-10-01"),
    //         ("BTC-USD", "CC", "2023-10-01"),
    //         ("BTC-USD", "CC", "2023-10-01"),
    //     ])
    //     .await;
    // println!("{:#?}", eod_ohlcv.unwrap());

    // let eod_intra = eod_client
    //     .batch_get_intraday_data(vec![
    //         ("AAPL", "US", "2023-10-01", "5m"),
    //         ("AAPL", "US", "2023-10-01", "5m"),
    //         ("AAPL", "US", "2023-10-01", "5m"),
    //         ("BTC-USD", "CC", "2023-10-01", "5m"),
    //         ("BTC-USD", "CC", "2023-10-01", "5m"),
    //         ("BTC-USD", "CC", "2023-10-01", "5m"),
    //     ])
    //     .await;
    // println!("{:#?}", eod_intra.unwrap());

    let eod_live = eod_client
        .batch_get_live_lagged_data(vec![
            ("AAPL", "US"),
            ("AAPL", "US"),
            ("AAPL", "US"),
            ("BTC-USD", "CC"),
            ("BTC-USD", "CC"),
            ("BTC-USD", "CC"),
        ])
        .await;
    println!("{:#?}", eod_live.unwrap());
}
