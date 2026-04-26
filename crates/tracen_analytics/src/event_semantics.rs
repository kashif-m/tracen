use serde_json::{Map, Value};

use crate::{round_to_local_day, round_to_local_month, round_to_local_week};

/// Ensure event payload carries canonical local time buckets derived from ts + offset.
pub fn normalize_event_payload_buckets(payload: &mut Value, ts_ms: i64, offset_minutes: i32) {
    let day_bucket = round_to_local_day(ts_ms, offset_minutes);
    let week_bucket = round_to_local_week(ts_ms, offset_minutes);
    let month_bucket = round_to_local_month(ts_ms, offset_minutes);

    let object = ensure_object(payload);
    object.insert("day_bucket".to_string(), Value::from(day_bucket));
    object.insert("week_bucket".to_string(), Value::from(week_bucket));
    object.insert("month_bucket".to_string(), Value::from(month_bucket));
}

fn ensure_object(payload: &mut Value) -> &mut Map<String, Value> {
    if !payload.is_object() {
        *payload = Value::Object(Map::new());
    }
    payload
        .as_object_mut()
        .expect("payload should be object after normalization")
}

#[cfg(test)]
mod tests {
    use super::normalize_event_payload_buckets;
    use serde_json::json;

    #[test]
    fn normalize_event_payload_buckets_overwrites_existing_bucket_fields() {
        let mut payload = json!({
            "day_bucket": 1,
            "week_bucket": 2,
            "month_bucket": 3,
            "exercise": "bench"
        });

        normalize_event_payload_buckets(&mut payload, 1_710_000_000_000, 330);

        let obj = payload.as_object().expect("object payload");
        assert_ne!(obj.get("day_bucket").and_then(|v| v.as_i64()), Some(1));
        assert_ne!(obj.get("week_bucket").and_then(|v| v.as_i64()), Some(2));
        assert_ne!(obj.get("month_bucket").and_then(|v| v.as_i64()), Some(3));
        assert_eq!(obj.get("exercise").and_then(|v| v.as_str()), Some("bench"));
    }

    #[test]
    fn normalize_event_payload_buckets_handles_non_object_payloads() {
        let mut payload = json!("bad");
        normalize_event_payload_buckets(&mut payload, 1_710_000_000_000, 0);

        let obj = payload.as_object().expect("object payload");
        assert!(obj.get("day_bucket").and_then(|v| v.as_i64()).is_some());
        assert!(obj.get("week_bucket").and_then(|v| v.as_i64()).is_some());
        assert!(obj.get("month_bucket").and_then(|v| v.as_i64()).is_some());
    }
}
