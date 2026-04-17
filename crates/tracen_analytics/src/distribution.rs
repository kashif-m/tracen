use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionItem {
    pub label: String,
    pub value: f32,
    pub percentage: f32,
}

pub struct Distribution;

impl Distribution {
    pub fn calculate(items: Vec<(String, f32)>) -> Vec<DistributionItem> {
        let total: f32 = items.iter().map(|(_, v)| v).sum();

        let mut result: Vec<DistributionItem> = items
            .into_iter()
            .map(|(label, value)| {
                let percentage = if total > 0.0 {
                    (value / total) * 100.0
                } else {
                    0.0
                };
                DistributionItem {
                    label,
                    value,
                    percentage,
                }
            })
            .collect();

        // Sort by value desc
        result.sort_by(|a, b| {
            b.value
                .partial_cmp(&a.value)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_percentages_and_sorts_descending() {
        let result = Distribution::calculate(vec![
            ("arms".to_string(), 20.0),
            ("legs".to_string(), 50.0),
            ("chest".to_string(), 30.0),
        ]);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].label, "legs");
        assert!((result[0].percentage - 50.0).abs() < f32::EPSILON);
        let total_percentage: f32 = result.iter().map(|item| item.percentage).sum();
        assert!((total_percentage - 100.0).abs() < 0.001);
    }

    #[test]
    fn handles_zero_total_without_nan() {
        let result = Distribution::calculate(vec![("a".to_string(), 0.0), ("b".to_string(), 0.0)]);
        assert!(result.iter().all(|item| item.percentage == 0.0));
    }
}
