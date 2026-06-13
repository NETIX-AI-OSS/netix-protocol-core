use crate::config::{AssetInstanceSpec, BuildingConfig, ConfigError, SimulatorConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresetSize {
    Small,
    Medium,
}

#[derive(Debug, Clone)]
pub struct MinimalConfigOptions {
    pub building_name: String,
    pub location: Option<String>,
    pub size: PresetSize,
    pub include_plant: bool,
    pub include_ahus: bool,
    pub include_vavs: bool,
    pub include_meters: bool,
}

impl Default for MinimalConfigOptions {
    fn default() -> Self {
        Self {
            building_name: "Custom Building".to_string(),
            location: None,
            size: PresetSize::Small,
            include_plant: true,
            include_ahus: true,
            include_vavs: true,
            include_meters: false,
        }
    }
}

impl SimulatorConfig {
    pub fn from_minimal(opts: &MinimalConfigOptions) -> Result<Self, ConfigError> {
        let base = SimulatorConfig::load_default_embedded()?;
        let mut instances = Vec::new();

        let (plant_scale, meter_scale) = match opts.size {
            PresetSize::Small => (1u32, 1u32),
            PresetSize::Medium => (2u32, 2u32),
        };

        if opts.include_plant {
            instances.extend([
                inst("chiller", "CH", "central_plant", plant_scale),
                inst("chw_pump", "CHWP", "central_plant", plant_scale * 2),
                inst("cooling_tower", "CT", "roof", plant_scale),
            ]);
            if opts.size == PresetSize::Medium {
                instances.push(inst("boiler", "BLR", "central_plant", 1));
                instances.push(inst("hhw_pump", "HHWP", "central_plant", 2));
            }
        }

        if opts.include_ahus {
            let large = if opts.size == PresetSize::Small { 2 } else { 6 };
            let small = if opts.size == PresetSize::Small { 0 } else { 2 };
            instances.push(inst("ahu_large", "AHU-L", "mep_rooms", large));
            if small > 0 {
                instances.push(inst("ahu_small", "AHU-S", "floor_mep", small));
            }
        }

        if opts.include_vavs {
            let count = match opts.size {
                PresetSize::Small => 10,
                PresetSize::Medium => 30,
            };
            instances.push(inst("vav_office", "VAV-OFC", "office", count));
            if opts.size == PresetSize::Medium {
                instances.push(inst("fcu_residential", "FCU-RES", "residential", 10));
            }
        }

        if opts.include_meters {
            instances.push(inst(
                "plant_meter",
                "PLANT-MTR",
                "central_plant",
                meter_scale,
            ));
            if opts.size == PresetSize::Medium {
                instances.push(inst("tenant_meter", "TNT-MTR", "tenant", 5));
                instances.push(inst("water_meter", "WTR-MTR", "tenant", 2));
            }
        }

        Ok(SimulatorConfig {
            building: BuildingConfig {
                name: opts.building_name.clone(),
                location: opts.location.clone(),
                timezone: base.building.timezone.clone(),
            },
            seasonality: base.seasonality,
            id_policy: base.id_policy,
            templates: base.templates,
            instances,
            protocols: base.protocols,
        })
    }
}

fn inst(template: &str, prefix: &str, zone: &str, count: u32) -> AssetInstanceSpec {
    AssetInstanceSpec {
        template: template.to_string(),
        name_prefix: prefix.to_string(),
        zone: Some(zone.to_string()),
        count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_config_parses_and_expands() {
        let cfg = SimulatorConfig::from_minimal(&MinimalConfigOptions::default()).unwrap();
        let devs = cfg.expand().unwrap();
        assert!(!devs.is_empty());
        assert!(devs.len() < 100, "small preset should stay compact");
    }

    #[test]
    fn medium_preset_is_larger_than_small() {
        let small = SimulatorConfig::from_minimal(&MinimalConfigOptions {
            size: PresetSize::Small,
            ..Default::default()
        })
        .unwrap()
        .expand()
        .unwrap()
        .len();
        let medium = SimulatorConfig::from_minimal(&MinimalConfigOptions {
            size: PresetSize::Medium,
            include_meters: true,
            ..Default::default()
        })
        .unwrap()
        .expand()
        .unwrap()
        .len();
        assert!(medium > small);
    }

    #[test]
    fn minimal_config_uses_only_known_templates() {
        let cfg = SimulatorConfig::from_minimal(&MinimalConfigOptions {
            include_plant: true,
            include_ahus: true,
            include_vavs: true,
            include_meters: true,
            size: PresetSize::Medium,
            ..Default::default()
        })
        .unwrap();
        for inst in &cfg.instances {
            assert!(
                cfg.templates.contains_key(&inst.template),
                "unknown template {}",
                inst.template
            );
        }
    }

    #[test]
    fn write_config_round_trip() {
        let cfg = SimulatorConfig::from_minimal(&MinimalConfigOptions::default()).unwrap();
        let dir = std::env::temp_dir().join(format!("bacnet-minimal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.yaml");
        SimulatorConfig::write_config(&path, &cfg).unwrap();
        let loaded = SimulatorConfig::load_from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(loaded.building.name, cfg.building.name);
        assert_eq!(loaded.instances.len(), cfg.instances.len());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
