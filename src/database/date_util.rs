use std::time::SystemTime;

use chrono::{DateTime, Utc};

/// Converts unix timestamp seconds into DateTime Strings
/// Accepts a given `format` string or uses standard default.
pub fn system_time_to_string(system_time: SystemTime, format: Option<&str>) -> String {
    let date_format = format.unwrap_or("%Y-%m-%d %H:%M:%S");
    let datetime_utc: DateTime<Utc> = system_time.into();
    datetime_utc.format(date_format).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn date_conversions() {
        // Time Zero
        assert_eq!(
            system_time_to_string(SystemTime::UNIX_EPOCH, None),
            "1970-01-01 00:00:00"
        );

        // First Ethereum Block: https://etherscan.io/block/1
        assert_eq!(
            system_time_to_string(
                SystemTime::UNIX_EPOCH + Duration::from_secs(1438262788),
                None
            ),
            "2015-07-30 13:26:28"
        );

        // Different Formats
        assert_eq!(
            system_time_to_string(
                SystemTime::UNIX_EPOCH,
                Some("Date(%Y-%m-%d)-Time(%H:%M:%S)")
            ),
            "Date(1970-01-01)-Time(00:00:00)"
        );
    }
}
