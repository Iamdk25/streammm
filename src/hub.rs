//! One encode, N subscribers. Drop-oldest when a client queue is full.

use crate::protocol::{TYPE_FRAG, TYPE_INIT, TYPE_JPEG, TYPE_SNAP};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub struct Media {
    pub kind: u8,
    pub data: Bytes,
    pub width: u32,
    pub height: u32,
    /// Hub-assigned identity of this media; unchanged while `latest` is not
    /// replaced, so consumers can tell a fresh frame from a re-served one.
    pub seq: u64,
}

struct Client {
    q: VecDeque<Media>,
    cap: usize,
    notify: Arc<Notify>,
}

struct Inner {
    latest: Option<Media>,
    snap: Option<Media>,
    snap_seq: u64,
    init: Option<Bytes>,
    width: u32,
    height: u32,
    subs: HashMap<u64, Arc<Mutex<Client>>>,
    next_id: u64,
    gen: u64,
    ts: VecDeque<Instant>,
    last_at: Option<Instant>,
    snap_notify: Arc<Notify>,
}

#[derive(Clone)]
pub struct Hub {
    inner: Arc<Mutex<Inner>>,
}

pub struct Subscriber {
    hub: Hub,
    id: u64,
    notify: Arc<Notify>,
    q: Arc<Mutex<Client>>,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                latest: None,
                snap: None,
                snap_seq: 0,
                init: None,
                width: 0,
                height: 0,
                subs: HashMap::new(),
                next_id: 1,
                gen: 0,
                ts: VecDeque::new(),
                last_at: None,
                snap_notify: Arc::new(Notify::new()),
            })),
        }
    }

    pub fn publish_init(&self, data: Bytes, w: u32, h: u32) {
        let media = Media {
            kind: TYPE_INIT,
            data: data.clone(),
            width: w,
            height: h,
            seq: next_seq(),
        };
        let mut g = self.inner.lock();
        g.init = Some(data);
        g.width = w;
        g.height = h;
        g.last_at = Some(Instant::now());
        Self::fanout(&mut g, media);
    }

    pub fn publish_unit(&self, kind: u8, data: Bytes, w: u32, h: u32) {
        let media = Media {
            kind,
            data,
            width: w,
            height: h,
            seq: next_seq(),
        };
        let mut g = self.inner.lock();
        // TYPE_SNAP is for the Worker LLM only. Fan it out (publisher needs it)
        // but never treat it as the live fragment — that would black-screen late joins.
        if kind == TYPE_SNAP {
            g.snap = Some(media.clone());
            g.snap_seq += 1;
            g.snap_notify.notify_waiters();
        }
        if kind != TYPE_INIT && kind != TYPE_SNAP {
            let now = Instant::now();
            g.last_at = Some(now);
            g.ts.push_back(now);
            while g.ts.front().is_some_and(|t| now.duration_since(*t).as_secs_f64() > 2.0) {
                g.ts.pop_front();
            }
            if w > 0 {
                g.width = w;
                g.height = h;
            }
            g.latest = Some(media.clone());
        }
        Self::fanout(&mut g, media);
    }

    fn drop_oldest_keep_init(q: &mut VecDeque<Media>) {
        if let Some(i) = q.iter().position(|m| m.kind != TYPE_INIT) {
            q.remove(i);
        } else {
            q.pop_front();
        }
    }

    fn fanout(g: &mut Inner, media: Media) {
        for c in g.subs.values() {
            let mut client = c.lock();
            if client.q.len() >= client.cap {
                Self::drop_oldest_keep_init(&mut client.q);
            }
            client.q.push_back(media.clone());
            client.notify.notify_one();
        }
    }

    pub fn subscribe(&self, cap: usize) -> Subscriber {
        let notify = Arc::new(Notify::new());
        let client = Arc::new(Mutex::new(Client {
            q: VecDeque::new(),
            cap: cap.max(1),
            notify: notify.clone(),
        }));
        let mut g = self.inner.lock();
        let id = g.next_id;
        g.next_id += 1;
        g.subs.insert(id, client.clone());
        Subscriber {
            hub: self.clone(),
            id,
            notify,
            q: client,
        }
    }

    pub fn init_segment(&self) -> Option<Bytes> {
        self.inner.lock().init.clone()
    }

    pub fn latest(&self) -> Option<Media> {
        self.inner.lock().latest.clone()
    }

    pub fn last_snap(&self) -> Option<Media> {
        self.inner.lock().snap.clone()
    }

    /// Monotonic counter of published TYPE_SNAP units.
    pub fn snap_seq(&self) -> u64 {
        self.inner.lock().snap_seq
    }

    /// Resolves when a snapshot newer than `seq` is published, or after `timeout`.
    /// Returns the snapshot sequence at whichever point ends the wait.
    pub async fn wait_for_snap_after(&self, seq: u64, timeout: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let (cur, notify) = {
                let g = self.inner.lock();
                (g.snap_seq, g.snap_notify.clone())
            };
            if cur != seq {
                return cur;
            }
            tokio::select! {
                _ = notify.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.inner.lock().snap_seq;
                }
            }
        }
    }

    pub fn size(&self) -> (u32, u32) {
        let g = self.inner.lock();
        (g.width, g.height)
    }

    pub fn last_media_age_s(&self) -> Option<f64> {
        self.inner.lock().last_at.map(|t| t.elapsed().as_secs_f64())
    }

    pub fn fps(&self) -> f64 {
        let mut g = self.inner.lock();
        let now = Instant::now();
        while g.ts.front().is_some_and(|t| now.duration_since(*t).as_secs_f64() > 2.0) {
            g.ts.pop_front();
        }
        g.ts.len() as f64 / 2.0
    }

    pub fn clients(&self) -> usize {
        self.inner.lock().subs.len()
    }

    pub fn generation(&self) -> u64 {
        self.inner.lock().gen
    }

    pub fn clear(&self) {
        let mut g = self.inner.lock();
        g.latest = None;
        g.init = None;
        g.ts.clear();
        g.last_at = None;
        g.gen += 1;
        for c in g.subs.values() {
            c.lock().q.clear();
        }
    }

    fn unsubscribe(&self, id: u64) {
        self.inner.lock().subs.remove(&id);
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

impl Subscriber {
    pub fn try_recv(&self) -> Option<Media> {
        self.q.lock().q.pop_front()
    }

    pub async fn recv(&self) -> Option<Media> {
        loop {
            if let Some(m) = self.try_recv() {
                return Some(m);
            }
            self.notify.notified().await;
            if self.q.lock().q.is_empty() && !self.hub.inner.lock().subs.contains_key(&self.id) {
                return None;
            }
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.hub.unsubscribe(self.id);
        self.notify.notify_one();
    }
}

pub fn kind_for_mode(mode: &str, is_init: bool) -> u8 {
    if mode == "mjpeg" {
        TYPE_JPEG
    } else if is_init {
        TYPE_INIT
    } else {
        TYPE_FRAG
    }
}

static FRAME_SEQ: AtomicU64 = AtomicU64::new(0);

/// Identity assigned to each published media unit.
pub fn next_seq() -> u64 {
    FRAME_SEQ.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{TYPE_FRAG, TYPE_SNAP};

    #[test]
    fn drop_oldest_on_full_queue() {
        let hub = Hub::new();
        let sub = hub.subscribe(2);
        hub.publish_unit(TYPE_FRAG, Bytes::from_static(b"a"), 0, 0);
        hub.publish_unit(TYPE_FRAG, Bytes::from_static(b"b"), 0, 0);
        hub.publish_unit(TYPE_FRAG, Bytes::from_static(b"c"), 0, 0);
        let first = sub.try_recv().unwrap();
        let second = sub.try_recv().unwrap();
        assert!(sub.try_recv().is_none());
        assert_eq!(&first.data[..], b"b");
        assert_eq!(&second.data[..], b"c");
    }

    #[test]
    fn empty_hub_has_no_media_age() {
        let hub = Hub::new();
        assert!(hub.last_media_age_s().is_none());
    }

    #[test]
    fn init_stored_and_latest_updated() {
        let hub = Hub::new();
        hub.publish_init(Bytes::from_static(b"init"), 1920, 1080);
        hub.publish_unit(TYPE_FRAG, Bytes::from_static(b"f1"), 0, 0);
        assert_eq!(&hub.init_segment().unwrap()[..], b"init");
        assert_eq!(&hub.latest().unwrap().data[..], b"f1");
        assert_eq!(hub.size(), (1920, 1080));
        assert!(hub.last_media_age_s().unwrap() < 1.0);
    }

    #[test]
    fn drop_oldest_never_drops_init() {
        let hub = Hub::new();
        let sub = hub.subscribe(2);
        hub.publish_init(Bytes::from_static(b"init"), 1920, 1080);
        hub.publish_unit(TYPE_FRAG, Bytes::from_static(b"a"), 0, 0);
        hub.publish_unit(TYPE_FRAG, Bytes::from_static(b"b"), 0, 0);
        hub.publish_unit(TYPE_FRAG, Bytes::from_static(b"c"), 0, 0);
        let mut kinds = Vec::new();
        let mut datas = Vec::new();
        while let Some(m) = sub.try_recv() {
            kinds.push(m.kind);
            datas.push(m.data.clone());
        }
        assert!(
            kinds.contains(&TYPE_INIT),
            "full subscriber queue must keep the init segment, got {kinds:?}"
        );
        assert_eq!(&datas[0][..], b"init");
    }

    #[test]
    fn snapshot_fans_out_but_does_not_replace_latest_fragment() {
        let hub = Hub::new();
        let sub = hub.subscribe(8);
        hub.publish_init(Bytes::from_static(b"init"), 1920, 1080);
        hub.publish_unit(TYPE_FRAG, Bytes::from_static(b"f1"), 0, 0);
        hub.publish_unit(TYPE_SNAP, Bytes::from_static(b"\xff\xd8"), 0, 0);
        assert_eq!(hub.latest().unwrap().kind, TYPE_FRAG);
        assert_eq!(&hub.latest().unwrap().data[..], b"f1");
        let mut kinds = Vec::new();
        while let Some(m) = sub.try_recv() {
            kinds.push(m.kind);
        }
        assert!(kinds.contains(&TYPE_SNAP));
        assert!(kinds.contains(&TYPE_FRAG));
    }

    #[tokio::test]
    async fn wait_for_snap_after_returns_when_new_snap_publishes() {
        let hub = Hub::new();
        let seq0 = hub.snap_seq();
        let h = hub.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            h.publish_unit(TYPE_SNAP, Bytes::from_static(b"\xff\xd8new"), 0, 0);
        });
        let next = hub.wait_for_snap_after(seq0, Duration::from_secs(5)).await;
        assert_ne!(next, seq0);
        assert_eq!(next, hub.snap_seq());
    }

    #[tokio::test]
    async fn wait_for_snap_after_times_out_without_new_snap() {
        let hub = Hub::new();
        let seq0 = hub.snap_seq();
        let start = Instant::now();
        let next = hub.wait_for_snap_after(seq0, Duration::from_millis(120)).await;
        assert_eq!(next, seq0);
        assert!(start.elapsed() >= Duration::from_millis(100));
    }
}
