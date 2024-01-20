// use molly_db::database_service::mongodb::MongoDbClient;
// use molly_db::database_service::mongodb::MongoDbClient;
use molly_db::database::database_service::MongoDbClient;

#[tokio::main]
async fn main() {
    env_logger::init();
    let mongo_client = MongoDbClient::new().await;
    let create_collections = mongo_client
        .run(vec![
            // (
            //     "AAPL",
            //     "US",
            //     "equity_spot_1d",
            //     "eod",
            //     "2023-10-10",
            //     "2024-01-01",
            // ),
            // (
            //     "BTC-USD",
            //     "CC",
            //     "crypto_spot_1d",
            //     "eod",
            //     "2023-10-10",
            //     "2024-01-01",
            // ),
            // (
            //     "BTC-USD",
            //     "CC",
            //     "crypto_spot_1d",
            //     "eod",
            //     "2023-08-10",
            //     "2024-01-01",
            // ),
            // (
            //     "AAPL",
            //     "US",
            //     "equity_spot_5m",
            //     "eod",
            //     "2023-10-10",
            //     "2024-01-01",
            // ),
            // (
            //     "AAPL",
            //     "US",
            //     "equity_spot_1h",
            //     "eod",
            //     "2022-10-10",
            //     "2023-10-10",
            // ),
                ("AAPL", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("MSFT", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("AMZN", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("GOOGL", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("META", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("TSLA", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("JNJ", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("JPM", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("PG", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("NVDA", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("V", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("MA", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("HD", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("UNH", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("BAC", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("INTC", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("KO", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("PFE", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("CSCO", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("VZ", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("IBM", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("CVX", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("WMT", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("ADBE", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("T", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("DIS", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("BA", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("GS", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("MMM", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01"),
                ("GE", "US", "equity_spot_1d", "eod", "1970-01-01", "2024-01-01")
                ])
                .await
                .unwrap();
    println!("{:#?}", create_collections);
}

// documentation
// optimizations
// fix cargo audit
// make json of all metadata objects for backup
// mock database for testing

// Note: still need to figure out intraday api batch limit in get_timestamps_tuple() function
