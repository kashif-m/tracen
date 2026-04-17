use crate::time_bucket::round_to_local_day;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreakResult {
    pub current_streak: i32,
    pub longest_streak: i32,
    pub total_active_days: i32,
    pub last_active_ts: Option<i64>,
    pub longest_start_ts: Option<i64>,
    pub longest_end_ts: Option<i64>,
}

pub struct StreakCalculator;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

impl StreakCalculator {
    pub fn calculate(timestamps: &[i64], offset_minutes: i32) -> StreakResult {
        if timestamps.is_empty() {
            StreakResult {
                current_streak: 0,
                longest_streak: 0,
                total_active_days: 0,
                last_active_ts: None,
                longest_start_ts: None,
                longest_end_ts: None,
            }
        } else {
            // 1. Deduplicate days
            let mut days: Vec<i64> = timestamps
                .iter()
                .map(|&ts| round_to_local_day(ts, offset_minutes))
                .collect();
            days.sort_unstable();
            days.dedup();

            let total_active_days = days.len() as i32;

            // 2. Iterate to find streaks and longest streak date range.
            let mut max_streak = 1;
            let mut max_start = days[0];
            let mut max_end = days[0];

            let mut segment_start = days[0];
            let mut segment_end = days[0];
            let mut segment_len = 1;

            for &day in days.iter().skip(1) {
                if day - segment_end == DAY_MS {
                    segment_end = day;
                    segment_len += 1;
                    continue;
                }

                if segment_len > max_streak || (segment_len == max_streak && segment_end > max_end)
                {
                    max_streak = segment_len;
                    max_start = segment_start;
                    max_end = segment_end;
                }

                segment_start = day;
                segment_end = day;
                segment_len = 1;
            }

            if segment_len > max_streak || (segment_len == max_streak && segment_end > max_end) {
                max_streak = segment_len;
                max_start = segment_start;
                max_end = segment_end;
            }

            // 3. Check if current streak is still active
            // (last active day must be today or yesterday for streak to continue)
            let today = round_to_local_day(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0),
                offset_minutes,
            );

            let current_streak = if let Some(&last) = days.last() {
                let gap = today - last;
                // If last active day was today or yesterday, streak continues
                if gap < DAY_MS * 2 {
                    segment_len
                } else {
                    0 // Streak broken
                }
            } else {
                0
            };

            StreakResult {
                current_streak,
                longest_streak: max_streak,
                total_active_days,
                last_active_ts: days.last().copied(),
                longest_start_ts: Some(max_start),
                longest_end_ts: Some(max_end),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_streak_exposes_start_and_end_days() {
        let base = 1_704_067_200_000; // 2024-01-01 UTC
        let day = DAY_MS;
        let timestamps = vec![
            base,
            base + day,
            base + 5 * day,
            base + 6 * day,
            base + 7 * day,
        ];

        let result = StreakCalculator::calculate(&timestamps, 0);
        assert_eq!(result.longest_streak, 3);
        assert_eq!(result.longest_start_ts, Some(base + 5 * day));
        assert_eq!(result.longest_end_ts, Some(base + 7 * day));
    }

    #[test]
    fn longest_streak_tie_prefers_more_recent_segment() {
        let base = 1_704_067_200_000; // 2024-01-01 UTC
        let day = DAY_MS;
        let timestamps = vec![base, base + day, base + 4 * day, base + 5 * day];

        let result = StreakCalculator::calculate(&timestamps, 0);
        assert_eq!(result.longest_streak, 2);
        assert_eq!(result.longest_start_ts, Some(base + 4 * day));
        assert_eq!(result.longest_end_ts, Some(base + 5 * day));
    }
}
