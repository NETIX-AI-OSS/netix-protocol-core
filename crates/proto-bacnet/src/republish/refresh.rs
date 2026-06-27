//! BACnet device-table refresh: broadcast Who-Is plus targeted re-resolution.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use bacnet_client::client::BACnetClient;
use bacnet_transport::bip::BipTransport;

use republish_core::model::RefreshOutcome;

use super::ConnCfg;

type BacnetIpClient = BACnetClient<BipTransport>;

const REFRESH_TARGETED_PASSES: usize = 2;
const REFRESH_TARGETED_CHUNK_SIZE: usize = 25;
const REFRESH_TARGETED_WAIT: Duration = Duration::from_millis(1_000);

pub(crate) async fn refresh_device_table(
    client: &BacnetIpClient,
    cfg: &ConnCfg,
    device_instances: &[u32],
) -> Result<RefreshOutcome> {
    let requested = normalize_device_instances(device_instances);
    if requested.is_empty() {
        return Ok(RefreshOutcome::default());
    }

    client.who_is(None, None).await?;
    tokio::time::sleep(Duration::from_millis(cfg.discovery_window_ms)).await;

    let mut unresolved = unresolved_device_instances(client, &requested).await;
    for _ in 0..REFRESH_TARGETED_PASSES {
        if unresolved.is_empty() {
            break;
        }
        for (low_limit, high_limit) in device_instance_ranges(&unresolved) {
            client.who_is(Some(low_limit), Some(high_limit)).await?;
            tokio::time::sleep(REFRESH_TARGETED_WAIT).await;
        }
        unresolved = unresolved_device_instances(client, &requested).await;
    }

    Ok(partition_refresh_outcome(&requested, &unresolved))
}

async fn unresolved_device_instances(client: &BacnetIpClient, requested: &[u32]) -> Vec<u32> {
    let mut unresolved = Vec::new();
    for &device_instance in requested {
        if client.get_device(device_instance).await.is_none() {
            unresolved.push(device_instance);
        }
    }
    unresolved
}

fn normalize_device_instances(device_instances: &[u32]) -> Vec<u32> {
    let mut instances = device_instances.to_vec();
    instances.sort_unstable();
    instances.dedup();
    instances
}

fn device_instance_ranges(device_instances: &[u32]) -> Vec<(u32, u32)> {
    normalize_device_instances(device_instances)
        .chunks(REFRESH_TARGETED_CHUNK_SIZE)
        .filter_map(|chunk| Some((*chunk.first()?, *chunk.last()?)))
        .collect()
}

fn partition_refresh_outcome(requested: &[u32], unresolved: &[u32]) -> RefreshOutcome {
    let unresolved_set = unresolved.iter().copied().collect::<HashSet<_>>();
    let mut resolved = Vec::with_capacity(requested.len());
    let mut missing = Vec::new();
    for &device_instance in requested {
        if unresolved_set.contains(&device_instance) {
            missing.push(device_instance);
        } else {
            resolved.push(device_instance);
        }
    }
    RefreshOutcome {
        resolved,
        unresolved: missing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_chunk_instances() {
        let instances: Vec<u32> = (1..=30).collect();
        let ranges = device_instance_ranges(&instances);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], (1, 25));
        assert_eq!(ranges[1], (26, 30));
    }

    #[test]
    fn partition_splits_resolved_and_unresolved() {
        let outcome = partition_refresh_outcome(&[1, 2, 3], &[2]);
        assert_eq!(outcome.resolved, vec![1, 3]);
        assert_eq!(outcome.unresolved, vec![2]);
    }
}
