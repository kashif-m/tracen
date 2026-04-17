use crate::time_bucket::round_to_local_day;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeatmapPoint {
    pub date: String,   // YYYY-MM-DD
    pub timestamp: i64, // Start of day ms
    pub count: i32,     // Number of events
    pub level: i32,     // 0-4 intensity
}

pub struct Heatmap;

impl Heatmap {
    pub fn calculate(timestamps: &[i64], offset_minutes: i32) -> Vec<HeatmapPoint> {
        let day_buckets: Vec<i64> = timestamps
            .iter()
            .map(|&ts| round_to_local_day(ts, offset_minutes))
            .collect();
        Self::calculate_from_day_buckets(&day_buckets, offset_minutes)
    }

    pub fn calculate_from_day_buckets(
        day_buckets: &[i64],
        offset_minutes: i32,
    ) -> Vec<HeatmapPoint> {
        if day_buckets.is_empty() {
            Vec::new()
        } else {
            let mut counts: std::collections::HashMap<i64, i32> = std::collections::HashMap::new();

            for &day in day_buckets {
                *counts.entry(day).or_insert(0) += 1;
            }

            // Determine max for scaling (simple linear or quantile?)
            // GitHub uses quartiles. We'll use simple linear max for now.
            let max_count = counts.values().max().copied().unwrap_or(1) as f32;

            let mut points: Vec<HeatmapPoint> = counts
                .into_iter()
                .map(|(ts, count)| {
                    // Calculate level 0-4
                    // 0 = 0 (not in map)
                    // 1-4 scaled
                    let ratio = count as f32 / max_count;
                    let level = if ratio <= 0.0 {
                        0
                    } else if ratio <= 0.25 {
                        1
                    } else if ratio <= 0.50 {
                        2
                    } else if ratio <= 0.75 {
                        3
                    } else {
                        4
                    };

                    // `timestamp` is stored as a UTC instant representing local day start.
                    // Re-apply offset for display-date serialization so YYYY-MM-DD matches local day.
                    let local_proxy = Utc
                        .timestamp_millis_opt(ts + (offset_minutes as i64 * 60 * 1000))
                        .unwrap();
                    let date_str = local_proxy.format("%Y-%m-%d").to_string();

                    HeatmapPoint {
                        date: date_str,
                        timestamp: ts,
                        count,
                        level,
                    }
                })
                .collect();

            points.sort_by_key(|p| p.timestamp);
            points
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn groups_events_by_day_and_sorts() {
        let day_ms = 86_400_000;
        let ts = [0, 1_000, day_ms + 10, day_ms + 1_000, day_ms + 2_000];
        let points = Heatmap::calculate(&ts, 0);
        assert_eq!(points.len(), 2);
        assert!(points[0].timestamp < points[1].timestamp);
        assert_eq!(points[0].count, 2);
        assert_eq!(points[1].count, 3);
        assert_eq!(points[1].level, 4);
    }

    #[test]
    fn empty_input_produces_no_points() {
        let points = Heatmap::calculate(&[], 330);
        assert!(points.is_empty());
    }

    #[test]
    fn positive_offset_serializes_local_day_date() {
        // 2026-03-07 04:00:00 UTC == 2026-03-07 09:30:00 IST.
        let event_ts = Utc
            .with_ymd_and_hms(2026, 3, 7, 4, 0, 0)
            .unwrap()
            .timestamp_millis();
        let points = Heatmap::calculate(&[event_ts], 330);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].date, "2026-03-07");
    }

    #[test]
    fn calculate_from_day_buckets_preserves_bucket_without_rebucketing() {
        let bucket = Utc
            .with_ymd_and_hms(2026, 3, 7, 18, 30, 0)
            .unwrap()
            .timestamp_millis();
        let points = Heatmap::calculate_from_day_buckets(&[bucket, bucket], 330);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp, bucket);
        assert_eq!(points[0].count, 2);
    }
}
