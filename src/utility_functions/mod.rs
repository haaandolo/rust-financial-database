use chrono::{NaiveDate, Utc, TimeZone, DateTime};
use mongodb::bson;

pub async fn string_to_datetime(date: &str) -> bson::DateTime {
    match date {
        _ if date.len() <= 10 => {
            let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .expect("Could not parse date string in %Y-%m-%d to NativeDate object");
            let datetime = date.and_hms_opt(0, 0, 0)
                .expect("Could not convert NativeDate to NativeDateTime object for date string in format %Y-%m-%d");
            let datetime_utc: DateTime<Utc> = Utc.from_utc_datetime(&datetime);
            bson::DateTime::from_chrono(datetime_utc)
        },
        _ => bson::DateTime::parse_rfc3339_str("1998-02-12T00:01:00.023Z").unwrap()
    }
}
