//! Merge browsed/discovered points into the configured point list, keyed by
//! [`PointIdentity`] (device key + addressing).

use std::collections::HashMap;

use crate::model::{PointConfig, PointIdentity};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeImportResult {
    pub points: Vec<PointConfig>,
    pub added: usize,
    pub updated: usize,
}

pub fn merge_imported_points(
    existing: &[PointConfig],
    imported: &[PointConfig],
) -> MergeImportResult {
    let mut points = existing.to_vec();
    let mut index_by_id = HashMap::<PointIdentity, usize>::new();
    for (index, point) in points.iter().enumerate() {
        index_by_id.insert(PointIdentity::from_point(point), index);
    }

    let mut added = 0;
    let mut updated = 0;
    for point in imported {
        let identity = PointIdentity::from_point(point);
        if let Some(&index) = index_by_id.get(&identity) {
            let existing_point = &mut points[index];
            let mut changed = false;
            if !point.tag_path.trim().is_empty() && existing_point.tag_path != point.tag_path {
                existing_point.tag_path = point.tag_path.clone();
                changed = true;
            }
            if !point.device_key.trim().is_empty() && existing_point.device_key != point.device_key
            {
                existing_point.device_key = point.device_key.clone();
                changed = true;
            }
            if changed {
                updated += 1;
            }
        } else {
            index_by_id.insert(identity, points.len());
            points.push(point.clone());
            added += 1;
        }
    }

    MergeImportResult {
        points,
        added,
        updated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_api::Addressing;

    fn point(device: &str, addr: i64, tag: &str) -> PointConfig {
        let mut addressing = Addressing::new();
        addressing.insert("address".into(), serde_json::json!(addr));
        PointConfig {
            device_key: device.to_string(),
            addressing,
            tag_path: tag.to_string(),
            ..PointConfig::default()
        }
    }

    #[test]
    fn merge_adds_new_and_updates_tag_paths() {
        let existing = vec![point("PLC1", 1, "old/path")];
        let imported = vec![
            point("PLC1", 1, "PLC1/SupplyTemp"),
            point("PLC1", 2, "PLC1/Fan"),
        ];
        let result = merge_imported_points(&existing, &imported);
        assert_eq!(result.added, 1);
        assert_eq!(result.updated, 1);
        assert_eq!(result.points.len(), 2);
        assert_eq!(result.points[0].tag_path, "PLC1/SupplyTemp");
    }
}
