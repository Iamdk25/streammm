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
        loop {
            let secs = interval.load(Ordering::Relaxed).max(1);
            tokio::time::sleep(Duration::from_secs(secs)).await;
            let Some(latest) = hub.latest() else {
                continue;
            };
            if latest.kind == TYPE_SNAP {
                continue;
            }
            let init = hub.init_segment();
            match encode_snapshot(init.as_deref(), &latest).await {
                Some(jpeg) => {
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
        };
        let out = encode_snapshot(None, &m).await.unwrap();
        assert_eq!(out, b"\xff\xd8\xff\xd9");
    }
}
