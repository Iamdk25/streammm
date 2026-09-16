//! Periodic JPEG snapshots for Cloudflare Worker screen analysis.

use crate::hub::{Hub, Media};
use crate::protocol::{TYPE_FRAG, TYPE_JPEG, TYPE_SNAP};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

/// Decode the latest video unit to a <=1920px JPEG, or passthrough if already JPEG.
pub async fn encode_snapshot(init: Option<&[u8]>, latest: &Media) -> Option<Vec<u8>> {
    if latest.kind == TYPE_JPEG || latest.kind == TYPE_SNAP {
        if latest.data.starts_with(b"\xff\xd8") {
            return Some(latest.data.to_vec());
        }
    }
    if latest.kind != TYPE_FRAG {
        return None;
    }
    let mut input = Vec::with_capacity(
        init.map(|i| i.len()).unwrap_or(0) + latest.data.len(),
    );
    if let Some(i) = init {
        input.extend_from_slice(i);
    }
    input.extend_from_slice(&latest.data);
    let mut child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            "pipe:0",
            "-frames:v",
            "1",
            "-vf",
            "scale=min(1920\\,iw):-2",
            "-q:v",
            "6",
            "-f",
            "mjpeg",
            "pipe:1",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .ok()?;
    let mut stdin = child.stdin.take()?;
    let mut stdout = child.stdout.take()?;
    let write = async move {
        stdin.write_all(&input).await.ok();
        drop(stdin);
    };
    let mut jpeg = Vec::new();
    let read = async {
        stdout.read_to_end(&mut jpeg).await.ok();
    };
    let _ = tokio::join!(write, read);
    let _ = child.wait().await;
    if jpeg.starts_with(b"\xff\xd8") {
        Some(jpeg)
    } else {
        None
    }
}

pub fn spawn(hub: Hub, interval: Arc<AtomicU64>) {
    tokio::spawn(async move {
        let mut snapshotted: Option<u64> = None;
        loop {
            let secs = interval.load(Ordering::Relaxed).max(1);
            tokio::time::sleep(Duration::from_secs(secs)).await;
            let Some(latest) = hub.latest() else {
                continue;
            };
            if latest.kind == TYPE_SNAP {
                continue;
            }
            // A stalled capture keeps serving the same frame. Republishing it
            // would advance the snapshot sequence and re-wake the analysis path
            // for an unchanged screen, so skip until the hub carries new media.
            if snapshotted == Some(latest.seq) {
                continue;
            }
            let init = hub.init_segment();
            match encode_snapshot(init.as_deref(), &latest).await {
                Some(jpeg) => {
                    snapshotted = Some(latest.seq);
                    hub.publish_unit(TYPE_SNAP, Bytes::from(jpeg), 0, 0);
                }
                None => {
                    tracing::debug!("snapshot: encode failed");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TYPE_JPEG;

    #[tokio::test]
    async fn jpeg_passthrough() {
        let m = Media {
            kind: TYPE_JPEG,
            data: Bytes::from_static(b"\xff\xd8\xff\xd9"),
            width: 10,
            height: 10,
            seq: 0,
        };
        let out = encode_snapshot(None, &m).await.unwrap();
        assert_eq!(out, b"\xff\xd8\xff\xd9");
    }

    #[tokio::test]
    async fn spawn_does_not_republish_unchanged_media() {
        use crate::protocol::TYPE_SNAP;
        let hub = Hub::new();
        let sub = hub.subscribe(16);
        hub.publish_unit(TYPE_JPEG, Bytes::from_static(b"\xff\xd8one"), 0, 0);
        spawn(hub.clone(), Arc::new(AtomicU64::new(1)));
        // Two ticks with no new media publish exactly one snapshot: a stalled
        // capture must not keep re-waking the analysis path.
        tokio::time::sleep(Duration::from_millis(2200)).await;
        let mut snaps = 0;
        while let Some(m) = sub.try_recv() {
            if m.kind == TYPE_SNAP {
                snaps += 1;
            }
        }
        assert_eq!(snaps, 1, "stalled capture must not republish snapshots");
        // New media resumes publication.
        hub.publish_unit(TYPE_JPEG, Bytes::from_static(b"\xff\xd8two"), 0, 0);
        tokio::time::sleep(Duration::from_millis(1400)).await;
        let mut more = 0;
        while let Some(m) = sub.try_recv() {
            if m.kind == TYPE_SNAP {
                more += 1;
            }
        }
        assert_eq!(more, 1, "new media must publish a fresh snapshot");
    }
}
