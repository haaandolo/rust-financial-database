// use molly_db::database_service;
// use molly_db::securities::{Equities, Crypto};
// use molly_db::wrappers::WrapperFunctions;
use molly_db::database_service::mongodb::MongoDbClient;

#[tokio::main]
async fn main() {
    let mongo_client = MongoDbClient::new().await;
    let create_collections = mongo_client
        .run(vec![
            ("AAPL", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
            ("AAPL", "US", "equity_spot_1d", "eod", "2023-10-10", "2024-01-01"),
            // ("AAPL", "US",  "equity_spot_1d", "binance", "2023-10-10", "2024-01-01"),
            // ("BTC-USD", "CC", "crypto_spot_1d", "fb", "2023-10-10", "2024-01-01"),
            ("BTC-USD", "CC", "crypto_spot_1d", "eod", "2023-10-10", "2024-01-01"),
            // ("BTC-USD", "CC",  "crypto_spot_1d", "binance", "2023-10-10", "2024-01-01"),
            // ("AAPL", "US", "equity_spot_1m", "eod", "2023-10-10", "2024-01-01"),
            // ("AAPL", "US",  "equity_spot_1h", "eod", "2023-10-10", "2024-01-01"),
            // ("AAPL", "US",  "equity_spot_live", "eod", "2023-10-10", "2024-01-01"),
            // ("BTC-USD", "CC", "crypto_spot_live", "eod", "2023-10-10", "2024-01-01"),
        ])
        .await;
    println!("{:#?}", create_collections);
}

// fix metadata 
// intraday data throttle
