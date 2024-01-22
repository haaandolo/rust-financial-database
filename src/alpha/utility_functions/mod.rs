use crate::database::database_service::MongoDbClient;
use anyhow::Result;
use chrono::{Days, NaiveDateTime};
use polars::{lazy::dsl::col, prelude::*};
use tokio;

pub fn get_data() -> Result<Vec<(String, DataFrame)>> {
    #[tokio::main]
    async fn get_data() -> Result<Vec<(String, DataFrame)>> {
        let mongo_client = MongoDbClient::new().await;
        let dfs = mongo_client
            .run(vec![
                (
                    "AAPL",
                    "US",
                    "equity_spot_1d",
                    "eod",
                    "1970-01-01",
                    "2024-01-01",
                ),
                (
                    "MSFT",
                    "US",
                    "equity_spot_1d",
                    "eod",
                    "1970-01-01",
                    "2024-01-01",
                ),
                (
                    "AMZN",
                    "US",
                    "equity_spot_1d",
                    "eod",
                    "1970-01-01",
                    "2024-01-01",
                ),
                (
                    "GOOGL",
                    "US",
                    "equity_spot_1d",
                    "eod",
                    "1970-01-01",
                    "2024-01-01",
                ),
            ])
            .await
            .unwrap();
        Ok(dfs)
    }
    get_data()
}

#[derive(Debug, Clone)]
pub struct Alpha {
    pub dfs: Vec<(String, DataFrame)>,
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
}

impl Alpha {
    pub fn new(_dfs: Vec<(String, DataFrame)>, _start: NaiveDateTime, _end: NaiveDateTime) -> Self {
        Self {
            dfs: _dfs,
            start: _start,
            end: _end,
        }
    }

    // make this funciton more dynamic
    fn generate_date_range(&self) -> Result<Vec<NaiveDateTime>> {
        let mut date_range = Vec::new();
        let mut current_date = self.start;
        while current_date <= self.end {
            date_range.push(current_date);
            current_date = current_date + Days::new(1)
        }
        Ok(date_range)
    }

    fn compute_meta_info(self) -> Result<()> {
        let date_range = self.generate_date_range().unwrap();
        for (_ticker, df) in self.dfs.into_iter() {
            let daterange_df = df!("datetime" => &date_range).unwrap();
            let df_joined = daterange_df
                .left_join(&df, ["datetime"], ["datetime"])?
                .fill_null(FillNullStrategy::Forward(None))?
                .fill_null(FillNullStrategy::Backward(None))?;

            let df_joined = df_joined
                .lazy()
                .with_columns(vec![
                    col("adjusted_close").pct_change(lit(1)).alias("return"),
                    // col("adjusted_close").shift(lit(1)).alias("shifted"),
                    col("adjusted_close")
                        .map(
                            |s| {
                                // let rolling_options = RollingOptionsFixedWindow {
                                //     window_size: 5,
                                //     min_periods: 5,
                                //     weights: None,
                                //     center: false,
                                //     fn_params: None
                                // };
                                let bool_series = &s
                                    .not_equal(&s.shift(1))?
                                    .into_series()
                                    .fill_null(FillNullStrategy::Backward(None))?;
                                // .rolling_sum(rolling_options)?;
                                Ok(Some(Series::new("", bool_series)))
                            },
                            GetOutput::from_type(DataType::Float64),
                        )
                        .alias("eligible"),
                ])
                .collect()?;

            println!("{:#?}", df_joined);
        }
        Ok(())
    }

    pub fn run(self) -> Result<()> {
        let _ = self.compute_meta_info();
        Ok(())
    }
}
