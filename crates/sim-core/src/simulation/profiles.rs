use std::collections::HashMap;

use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PointValue {
    Real(f32),
    Boolean(bool),
    Unsigned(u32),
}

impl PointValue {
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            PointValue::Real(v) => Some(*v),
            PointValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            PointValue::Unsigned(u) => Some(*u as f32),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProfileSpec {
    Constant {
        value: f32,
    },
    Sine {
        base: f32,
        amplitude: f32,
        period_secs: f32,
        #[serde(default)]
        phase_secs: f32,
    },
    OccupancyLinked {
        base: f32,
        peak_delta: f32,
        #[serde(default)]
        noise: f32,
    },
    TempControl {
        setpoint: f32,
        gain: f32,
        #[serde(default)]
        outside_influence: f32,
        #[serde(default)]
        noise: f32,
        #[serde(default)]
        initial: Option<f32>,
    },
    Integrator {
        rate_source: String,
        #[serde(default = "default_one")]
        scale: f32,
    },
    RandomWalk {
        base: f32,
        step: f32,
        min: f32,
        max: f32,
    },
    BinarySchedule {
        on_when_occupancy_gt: f32,
    },
    MultiState {
        states: Vec<u32>,
        change_every_secs: f32,
    },
    Ramp {
        start: f32,
        end: f32,
        period_secs: f32,
    },
    DerivedConstant {
        from: String,
    },
    ConstantBool {
        value: bool,
    },
    ConstantState {
        value: u32,
    },
}

fn default_one() -> f32 {
    1.0
}

/// Sample symmetric noise in `[-noise, noise)`.  Returns 0.0 when `noise` is non-positive,
/// avoiding a `gen_range` panic on a zero-width range.
fn sample_noise(rng: &mut impl rand::Rng, noise: f32) -> f32 {
    if noise > 0.0 {
        rng.gen_range(-noise..noise)
    } else {
        0.0
    }
}

#[derive(Debug, Clone)]
pub enum ProfileState {
    Constant(f32),
    ConstantBool(bool),
    ConstantState(u32),
    Sine {
        base: f32,
        amplitude: f32,
        period_secs: f32,
        phase_secs: f32,
    },
    OccupancyLinked {
        base: f32,
        peak_delta: f32,
        noise: f32,
    },
    TempControl {
        setpoint: f32,
        gain: f32,
        outside_influence: f32,
        noise: f32,
        current: f32,
    },
    Integrator {
        rate_source: String,
        scale: f32,
        accumulator: f32,
    },
    RandomWalk {
        step: f32,
        min: f32,
        max: f32,
        current: f32,
    },
    BinarySchedule {
        threshold: f32,
    },
    MultiState {
        states: Vec<u32>,
        change_every_secs: f32,
        idx: usize,
        elapsed: f32,
    },
    Ramp {
        start: f32,
        end: f32,
        period_secs: f32,
    },
    DerivedConstant {
        from: String,
    },
}

pub struct TickCtx<'a> {
    pub dt: f32,
    pub now_secs: f64,
    pub occupancy: f32,
    pub outside_temp: f32,
    pub siblings: &'a HashMap<String, f32>,
}

impl ProfileState {
    pub fn from_spec(spec: &ProfileSpec) -> Self {
        match spec {
            ProfileSpec::Constant { value } => ProfileState::Constant(*value),
            ProfileSpec::ConstantBool { value } => ProfileState::ConstantBool(*value),
            ProfileSpec::ConstantState { value } => ProfileState::ConstantState(*value),
            ProfileSpec::Sine {
                base,
                amplitude,
                period_secs,
                phase_secs,
            } => ProfileState::Sine {
                base: *base,
                amplitude: *amplitude,
                period_secs: *period_secs,
                phase_secs: *phase_secs,
            },
            ProfileSpec::OccupancyLinked {
                base,
                peak_delta,
                noise,
            } => ProfileState::OccupancyLinked {
                base: *base,
                peak_delta: *peak_delta,
                noise: *noise,
            },
            ProfileSpec::TempControl {
                setpoint,
                gain,
                outside_influence,
                noise,
                initial,
            } => ProfileState::TempControl {
                setpoint: *setpoint,
                gain: *gain,
                outside_influence: *outside_influence,
                noise: *noise,
                current: initial.unwrap_or(*setpoint),
            },
            ProfileSpec::Integrator { rate_source, scale } => ProfileState::Integrator {
                rate_source: rate_source.clone(),
                scale: *scale,
                accumulator: 0.0,
            },
            ProfileSpec::RandomWalk {
                base,
                step,
                min,
                max,
            } => ProfileState::RandomWalk {
                step: *step,
                min: *min,
                max: *max,
                current: base.clamp(*min, *max),
            },
            ProfileSpec::BinarySchedule {
                on_when_occupancy_gt,
            } => ProfileState::BinarySchedule {
                threshold: *on_when_occupancy_gt,
            },
            ProfileSpec::MultiState {
                states,
                change_every_secs,
            } => ProfileState::MultiState {
                states: states.clone(),
                change_every_secs: *change_every_secs,
                idx: 0,
                elapsed: 0.0,
            },
            ProfileSpec::Ramp {
                start,
                end,
                period_secs,
            } => ProfileState::Ramp {
                start: *start,
                end: *end,
                period_secs: *period_secs,
            },
            ProfileSpec::DerivedConstant { from } => {
                ProfileState::DerivedConstant { from: from.clone() }
            }
        }
    }

    pub fn initial_value(&self) -> PointValue {
        match self {
            ProfileState::Constant(v) => PointValue::Real(*v),
            ProfileState::ConstantBool(b) => PointValue::Boolean(*b),
            ProfileState::ConstantState(u) => PointValue::Unsigned(*u),
            ProfileState::Sine { base, .. } => PointValue::Real(*base),
            ProfileState::OccupancyLinked { base, .. } => PointValue::Real(*base),
            ProfileState::TempControl { current, .. } => PointValue::Real(*current),
            ProfileState::Integrator { accumulator, .. } => PointValue::Real(*accumulator),
            ProfileState::RandomWalk { current, .. } => PointValue::Real(*current),
            ProfileState::BinarySchedule { .. } => PointValue::Boolean(false),
            ProfileState::MultiState { states, .. } => {
                PointValue::Unsigned(states.first().copied().unwrap_or(1))
            }
            ProfileState::Ramp { start, .. } => PointValue::Real(*start),
            ProfileState::DerivedConstant { .. } => PointValue::Real(0.0),
        }
    }

    pub fn tick(&mut self, ctx: &TickCtx) -> PointValue {
        let mut rng = rand::thread_rng();
        match self {
            ProfileState::Constant(v) => PointValue::Real(*v),
            ProfileState::ConstantBool(b) => PointValue::Boolean(*b),
            ProfileState::ConstantState(u) => PointValue::Unsigned(*u),
            ProfileState::Sine {
                base,
                amplitude,
                period_secs,
                phase_secs,
            } => {
                let omega = std::f64::consts::TAU / (*period_secs as f64).max(1.0);
                let v = *base as f64
                    + (*amplitude as f64) * (omega * (ctx.now_secs + *phase_secs as f64)).sin();
                PointValue::Real(v as f32)
            }
            ProfileState::OccupancyLinked {
                base,
                peak_delta,
                noise,
            } => {
                let noise_v = sample_noise(&mut rng, *noise);
                PointValue::Real(*base + *peak_delta * ctx.occupancy + noise_v)
            }
            ProfileState::TempControl {
                setpoint,
                gain,
                outside_influence,
                noise,
                current,
            } => {
                let target = *setpoint + *outside_influence * (ctx.outside_temp - *setpoint);
                let target = target + ctx.occupancy * 0.5; // mild internal-gain drift
                let diff = target - *current;
                let noise_v = sample_noise(&mut rng, *noise);
                *current += diff * *gain * ctx.dt.max(0.001) + noise_v;
                PointValue::Real(*current)
            }
            ProfileState::Integrator {
                rate_source,
                scale,
                accumulator,
            } => {
                let rate = ctx.siblings.get(rate_source).copied().unwrap_or(0.0);
                *accumulator += rate * ctx.dt * *scale;
                PointValue::Real(*accumulator)
            }
            ProfileState::RandomWalk {
                step,
                min,
                max,
                current,
            } => {
                let delta = rng.gen_range(-*step..*step);
                *current = (*current + delta).clamp(*min, *max);
                PointValue::Real(*current)
            }
            ProfileState::BinarySchedule { threshold } => {
                PointValue::Boolean(ctx.occupancy > *threshold)
            }
            ProfileState::MultiState {
                states,
                change_every_secs,
                idx,
                elapsed,
            } => {
                *elapsed += ctx.dt;
                if *elapsed >= *change_every_secs && !states.is_empty() {
                    *elapsed = 0.0;
                    *idx = (*idx + 1) % states.len();
                }
                let v = states.get(*idx).copied().unwrap_or(1);
                PointValue::Unsigned(v)
            }
            ProfileState::Ramp {
                start,
                end,
                period_secs,
            } => {
                let p = (*period_secs as f64).max(1.0);
                let frac = ((ctx.now_secs % p) / p) as f32;
                PointValue::Real(*start + (*end - *start) * frac)
            }
            ProfileState::DerivedConstant { from } => {
                let v = ctx.siblings.get(from).copied().unwrap_or(0.0);
                PointValue::Real(v)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(dt: f32, occ: f32, ot: f32, siblings: &'a HashMap<String, f32>) -> TickCtx<'a> {
        TickCtx {
            dt,
            now_secs: 0.0,
            occupancy: occ,
            outside_temp: ot,
            siblings,
        }
    }

    #[test]
    fn constant_returns_value() {
        let mut s = ProfileState::from_spec(&ProfileSpec::Constant { value: 42.0 });
        let siblings = HashMap::new();
        assert_eq!(
            s.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
            PointValue::Real(42.0)
        );
    }

    #[test]
    fn occupancy_linked_scales_with_occupancy() {
        let mut s = ProfileState::from_spec(&ProfileSpec::OccupancyLinked {
            base: 10.0,
            peak_delta: 100.0,
            noise: 0.0,
        });
        let siblings = HashMap::new();
        let v = s.tick(&ctx(1.0, 1.0, 25.0, &siblings)).as_f32().unwrap();
        assert!((v - 110.0).abs() < 1e-3);
        let v0 = s.tick(&ctx(1.0, 0.0, 25.0, &siblings)).as_f32().unwrap();
        assert!((v0 - 10.0).abs() < 1e-3);
    }

    #[test]
    fn temp_control_converges_to_setpoint() {
        let mut s = ProfileState::from_spec(&ProfileSpec::TempControl {
            setpoint: 22.0,
            gain: 0.5,
            outside_influence: 0.0,
            noise: 0.0,
            initial: Some(30.0),
        });
        let siblings = HashMap::new();
        for _ in 0..200 {
            s.tick(&ctx(1.0, 0.0, 22.0, &siblings));
        }
        if let ProfileState::TempControl { current, .. } = &s {
            assert!((current - 22.0).abs() < 0.5);
        } else {
            panic!("wrong state");
        }
    }

    #[test]
    fn random_walk_stays_in_bounds() {
        let mut s = ProfileState::from_spec(&ProfileSpec::RandomWalk {
            base: 50.0,
            step: 10.0,
            min: 0.0,
            max: 100.0,
        });
        let siblings = HashMap::new();
        for _ in 0..500 {
            let v = s.tick(&ctx(1.0, 0.0, 25.0, &siblings)).as_f32().unwrap();
            assert!(v >= 0.0 && v <= 100.0);
        }
    }

    #[test]
    fn binary_schedule_threshold() {
        let mut s = ProfileState::from_spec(&ProfileSpec::BinarySchedule {
            on_when_occupancy_gt: 0.1,
        });
        let siblings = HashMap::new();
        assert_eq!(
            s.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
            PointValue::Boolean(false)
        );
        assert_eq!(
            s.tick(&ctx(1.0, 0.5, 25.0, &siblings)),
            PointValue::Boolean(true)
        );
    }

    #[test]
    fn integrator_accumulates_from_sibling() {
        let mut s = ProfileState::from_spec(&ProfileSpec::Integrator {
            rate_source: "power".into(),
            scale: 1.0 / 3600.0,
        });
        let mut siblings = HashMap::new();
        siblings.insert("power".to_string(), 3600.0);
        for _ in 0..10 {
            s.tick(&ctx(1.0, 0.0, 25.0, &siblings));
        }
        if let ProfileState::Integrator { accumulator, .. } = &s {
            assert!((accumulator - 10.0).abs() < 0.01);
        } else {
            panic!("wrong state");
        }
    }

    #[test]
    fn multi_state_cycles() {
        let mut s = ProfileState::from_spec(&ProfileSpec::MultiState {
            states: vec![1, 2, 3],
            change_every_secs: 2.0,
        });
        let siblings = HashMap::new();
        assert_eq!(
            s.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
            PointValue::Unsigned(1)
        );
        assert_eq!(
            s.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
            PointValue::Unsigned(2)
        );
        assert_eq!(
            s.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
            PointValue::Unsigned(2)
        );
        assert_eq!(
            s.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
            PointValue::Unsigned(3)
        );
    }

    #[test]
    fn constant_bool_returns_fixed_boolean() {
        let mut s_true = ProfileState::from_spec(&ProfileSpec::ConstantBool { value: true });
        let mut s_false = ProfileState::from_spec(&ProfileSpec::ConstantBool { value: false });
        let siblings = HashMap::new();
        assert_eq!(
            s_true.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
            PointValue::Boolean(true)
        );
        assert_eq!(
            s_false.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
            PointValue::Boolean(false)
        );
    }

    #[test]
    fn constant_state_returns_fixed_unsigned() {
        let mut s = ProfileState::from_spec(&ProfileSpec::ConstantState { value: 7 });
        let siblings = HashMap::new();
        for _ in 0..5 {
            assert_eq!(
                s.tick(&ctx(1.0, 0.0, 25.0, &siblings)),
                PointValue::Unsigned(7)
            );
        }
    }

    #[test]
    fn ramp_interpolates_from_start_to_end_across_period() {
        let start = 0.0f32;
        let end = 100.0f32;
        let period = 100.0f32;
        let mut s = ProfileState::from_spec(&ProfileSpec::Ramp {
            start,
            end,
            period_secs: period,
        });
        let siblings = HashMap::new();

        // At t=0, frac=0.0 → value should equal start
        let mut c = ctx(1.0, 0.0, 25.0, &siblings);
        c.now_secs = 0.0;
        let v0 = s.tick(&c).as_f32().unwrap();
        assert!((v0 - start).abs() < 1e-3, "at t=0: {v0}");

        // At t=50 (half period), frac=0.5 → value should be midpoint
        c.now_secs = 50.0;
        let v50 = s.tick(&c).as_f32().unwrap();
        assert!((v50 - 50.0).abs() < 1e-3, "at t=50: {v50}");

        // At t=100 (exactly one period), frac wraps to 0 → value equals start again
        c.now_secs = 100.0;
        let v100 = s.tick(&c).as_f32().unwrap();
        assert!((v100 - start).abs() < 1e-3, "at t=100 (wrap): {v100}");
    }

    #[test]
    fn ramp_wraps_at_period_boundary() {
        let mut s = ProfileState::from_spec(&ProfileSpec::Ramp {
            start: 10.0,
            end: 20.0,
            period_secs: 10.0,
        });
        let siblings = HashMap::new();
        // t=15 → t mod 10 = 5, frac = 0.5 → value = 10 + 0.5 * 10 = 15
        let mut c = ctx(1.0, 0.0, 25.0, &siblings);
        c.now_secs = 15.0;
        let v = s.tick(&c).as_f32().unwrap();
        assert!((v - 15.0).abs() < 1e-3, "wrapping ramp at t=15: {v}");
    }

    #[test]
    fn derived_constant_reads_from_sibling() {
        let mut s = ProfileState::from_spec(&ProfileSpec::DerivedConstant { from: "co2".into() });
        let mut siblings = HashMap::new();
        siblings.insert("co2".to_string(), 450.0f32);
        let v = s.tick(&ctx(1.0, 0.0, 25.0, &siblings)).as_f32().unwrap();
        assert!((v - 450.0).abs() < 1e-3);
    }

    #[test]
    fn derived_constant_falls_back_to_zero_when_sibling_missing() {
        let mut s = ProfileState::from_spec(&ProfileSpec::DerivedConstant {
            from: "missing".into(),
        });
        let siblings = HashMap::new();
        let v = s.tick(&ctx(1.0, 0.0, 25.0, &siblings)).as_f32().unwrap();
        assert!((v - 0.0).abs() < 1e-6);
    }

    #[test]
    fn sine_oscillates_around_base() {
        let mut s = ProfileState::from_spec(&ProfileSpec::Sine {
            base: 10.0,
            amplitude: 5.0,
            period_secs: 100.0,
            phase_secs: 0.0,
        });
        let siblings = HashMap::new();
        let mut max = f32::MIN;
        let mut min = f32::MAX;
        for i in 0..100 {
            let mut c = ctx(1.0, 0.0, 25.0, &siblings);
            c.now_secs = i as f64;
            let v = s.tick(&c).as_f32().unwrap();
            max = max.max(v);
            min = min.min(v);
        }
        assert!(max > 13.0 && max <= 15.5);
        assert!(min < 7.0 && min >= 4.5);
    }
}
