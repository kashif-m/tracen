use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    Day,
    Week,
    Month,
}

const MINUTE_MS: i64 = 60 * 1000;
const DAY_MS: i64 = 24 * 60 * 60 * 1000;

pub fn bucket_ts(ts_ms: i64, granularity: Granularity, offset_minutes: i32) -> i64 {
    match granularity {
        Granularity::Day => round_to_local_day(ts_ms, offset_minutes),
        Granularity::Week => round_to_local_week(ts_ms, offset_minutes),
        Granularity::Month => round_to_local_month(ts_ms, offset_minutes),
    }
}

pub fn round_to_local_day(ts_ms: i64, offset_minutes: i32) -> i64 {
    let offset = offset_minutes as i64 * MINUTE_MS;
    let local = ts_ms.saturating_add(offset);
    let rounded_local = (local / DAY_MS) * DAY_MS;
    rounded_local.saturating_sub(offset)
}

pub fn round_to_local_week(ts_ms: i64, offset_minutes: i32) -> i64 {
    let start_of_day = round_to_local_day(ts_ms, offset_minutes);
    let offset = offset_minutes as i64 * MINUTE_MS;

    // Jan 1, 1970 was Thursday (4)
    let local_date_ms = start_of_day.saturating_add(offset);
    let days_since_epoch = local_date_ms / DAY_MS;
    let day_of_week = ((days_since_epoch + 4) % 7) as i32; // 0 = Sunday

    // Convert to Monday-based (Monday=0, Sunday=6)
    // Sunday(0) -> 6
    // Monday(1) -> 0
    // ...
    // Thursday(4) -> 3
    let monday_offset = (day_of_week + 6) % 7;

    start_of_day.saturating_sub(monday_offset as i64 * DAY_MS)
}

pub fn round_to_local_month(ts_ms: i64, offset_minutes: i32) -> i64 {
    use chrono::{Datelike, LocalResult, TimeZone, Utc};

    let dt = match Utc.timestamp_millis_opt(ts_ms) {
        LocalResult::Single(value) => value,
        _ => return round_to_local_day(ts_ms, offset_minutes),
    };
    let local_proxy = match dt.checked_add_signed(chrono::Duration::minutes(offset_minutes as i64))
    {
        Some(value) => value,
        None => return round_to_local_day(ts_ms, offset_minutes),
    };

    let year = local_proxy.year();
    let month = local_proxy.month();

    let start_of_month_local = match Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0) {
        LocalResult::Single(value) => value,
        _ => return round_to_local_day(ts_ms, offset_minutes),
    };

    start_of_month_local
        .timestamp_millis()
        .saturating_sub(offset_minutes as i64 * MINUTE_MS)
}

#[cfg(test)]
mod tests {
    use super::{round_to_local_day, round_to_local_month};

    #[test]
    fn round_to_local_month_handles_out_of_range_timestamps() {
        let bucket = round_to_local_month(i64::MAX, 330);
        assert_eq!(bucket, round_to_local_day(i64::MAX, 330));
    }
}
