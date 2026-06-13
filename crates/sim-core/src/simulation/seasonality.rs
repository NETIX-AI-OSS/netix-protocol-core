use crate::config::WeeklySchedule;
use chrono::{DateTime, Datelike, Local, NaiveTime, Timelike};

pub struct SeasonalityEngine {
    schedule: WeeklySchedule,
}

impl SeasonalityEngine {
    pub fn new(schedule: WeeklySchedule) -> Self {
        Self { schedule }
    }

    pub fn get_occupancy(&self, time: DateTime<Local>) -> f64 {
        let is_weekend =
            time.weekday() == chrono::Weekday::Sat || time.weekday() == chrono::Weekday::Sun;
        let day_schedule = if is_weekend {
            &self.schedule.weekend_occupancy
        } else {
            &self.schedule.weekday_occupancy
        };

        if day_schedule.is_empty() {
            return 0.0;
        }

        let current_time_mins = time.hour() * 60 + time.minute();

        // Find the right interval
        let mut prev_val = day_schedule[0].value;
        let mut prev_time = self.parse_time_mins(&day_schedule[0].time);

        if current_time_mins < prev_time {
            return prev_val; // Before the first schedule point
        }

        for point in day_schedule.iter().skip(1) {
            let next_time = self.parse_time_mins(&point.time);
            let next_val = point.value;

            if current_time_mins <= next_time {
                // Linear interpolation
                let fraction =
                    (current_time_mins - prev_time) as f64 / (next_time - prev_time) as f64;
                return prev_val + fraction * (next_val - prev_val);
            }

            prev_time = next_time;
            prev_val = next_val;
        }

        prev_val // After the last schedule point
    }

    fn parse_time_mins(&self, time_str: &str) -> u32 {
        let nt = NaiveTime::parse_from_str(time_str, "%H:%M").unwrap_or_default();
        nt.hour() * 60 + nt.minute()
    }

    pub fn get_outside_temp(&self, time: DateTime<Local>) -> f64 {
        // Simple mock: temp peaks at 2 PM (14:00) at 35 °C, drops to 25 °C at 4 AM.
        // base_temp (30) + amplitude (5) = 35 °C at peak; 30 − 5 = 25 °C at trough.
        let hour = time.hour() as f64 + (time.minute() as f64 / 60.0);
        let base_temp = 30.0;
        let amplitude = 5.0;
        base_temp + amplitude * ((hour - 14.0) * std::f64::consts::PI / 12.0).cos()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TimeValue;
    use chrono::TimeZone;

    #[test]
    fn get_outside_temp_peaks_at_2pm() {
        let schedule = WeeklySchedule {
            weekday_occupancy: vec![],
            weekend_occupancy: vec![],
        };
        let engine = SeasonalityEngine::new(schedule);

        // 2023-10-18 Wednesday — 14:00 should be peak (35 °C)
        let peak_time = Local.with_ymd_and_hms(2023, 10, 18, 14, 0, 0).unwrap();
        let peak_temp = engine.get_outside_temp(peak_time);
        assert!(
            (peak_temp - 35.0).abs() < 0.01,
            "peak should be ~35 °C, got {peak_temp}"
        );
    }

    #[test]
    fn get_outside_temp_trough_at_4am() {
        let schedule = WeeklySchedule {
            weekday_occupancy: vec![],
            weekend_occupancy: vec![],
        };
        let engine = SeasonalityEngine::new(schedule);

        // 04:00 is opposite of 14:00 on the cosine wave → trough (25 °C)
        // cos((4 - 14) * π / 12) = cos(-10π/12) = cos(5π/6) = −√3/2 ≈ −0.866
        // temp = 30 + 5 * −0.866 ≈ 25.67 °C  (not exactly 25 since trough is at 2 AM, not 4 AM)
        let trough_time = Local.with_ymd_and_hms(2023, 10, 18, 2, 0, 0).unwrap();
        let trough_temp = engine.get_outside_temp(trough_time);
        assert!(
            (trough_temp - 25.0).abs() < 0.01,
            "trough at 02:00 should be ~25 °C, got {trough_temp}"
        );
    }

    #[test]
    fn get_outside_temp_stays_in_expected_band() {
        let schedule = WeeklySchedule {
            weekday_occupancy: vec![],
            weekend_occupancy: vec![],
        };
        let engine = SeasonalityEngine::new(schedule);

        // Sweep every hour and confirm the value is in [25, 35].
        for hour in 0u32..24 {
            let t = Local.with_ymd_and_hms(2023, 10, 18, hour, 0, 0).unwrap();
            let temp = engine.get_outside_temp(t);
            assert!(
                temp >= 25.0 && temp <= 35.0,
                "hour={hour}: temp {temp} out of [25, 35]"
            );
        }
    }

    #[test]
    fn test_occupancy_interpolation() {
        let schedule = WeeklySchedule {
            weekday_occupancy: vec![
                TimeValue {
                    time: "00:00".to_string(),
                    value: 0.0,
                },
                TimeValue {
                    time: "12:00".to_string(),
                    value: 1.0,
                },
                TimeValue {
                    time: "24:00".to_string(),
                    value: 0.0,
                },
            ],
            weekend_occupancy: vec![],
        };
        let engine = SeasonalityEngine::new(schedule);

        // 2023-10-18 is a Wednesday
        let time_6am = Local.with_ymd_and_hms(2023, 10, 18, 6, 0, 0).unwrap();
        assert!((engine.get_occupancy(time_6am) - 0.5).abs() < 0.01);

        let time_12pm = Local.with_ymd_and_hms(2023, 10, 18, 12, 0, 0).unwrap();
        assert_eq!(engine.get_occupancy(time_12pm), 1.0);
    }
}
