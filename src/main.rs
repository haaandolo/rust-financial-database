use molly_db::database_service::mongodb::MongoDbClient;

#[tokio::main]
async fn main() {
    env_logger::init();
    let mongo_client = MongoDbClient::new().await;
    let create_collections = mongo_client
        .run(vec![
            // ("AAPL", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
            (
                "AAPL",
                "US",
                "equity_spot_1d",
                "eod",
                "2023-10-10",
                "2024-01-01",
            ),
            (
                "BTC-USD",
                "CC",
                "crypto_spot_1d",
                "eod",
                "2023-10-10",
                "2024-01-01",
            ),
            (
                "BTC-USD",
                "CC",
                "crypto_spot_1d",
                "eod",
                "2023-08-10",
                "2024-01-01",
            ),
            (
                "AAPL",
                "US",
                "equity_spot_5m",
                "eod",
                "2023-10-10",
                "2024-01-01",
            ),
            (
                "AAPL",
                "US",
                "equity_spot_1h",
                "eod",
                "2022-10-10",
                "2023-10-10",
            ),
            // ("AAPL", "US",  "equity_spot_live", "eod", "2023-10-10", "2024-01-01"),
            // ("BTC-USD", "CC", "crypto_spot_live", "eod", "2023-10-10", "2024-01-01"),
        ])
        .await;
    println!("{:#?}", create_collections);
}


// documentation
// optimizations
// fix cargo audit
// make json of all metadata objects for backup

// Note: still need to figure out intraday api batch limit in get_timestamps_tuple() function
