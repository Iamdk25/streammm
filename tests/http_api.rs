//! HTTP API on the real Axum router (no capture required).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use streamaid::config::Config;
use streamaid::frame::mp4_box;
use streamaid::protocol::TYPE_FRAG;
use streamaid::server::App;
use tokio::net::TcpListener;
use tower::ServiceExt;

fn temp_app() -> (tempfile::TempDir, std::sync::Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let cfg = Config::default();
    streamaid::config::save(&cfg, &path).unwrap();
    let app = App::new(cfg, path);
    (dir, app)
}

async fn body_json(res: axum::response::Response) -> Value {
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn config_post_clamps_and_round_trips() {
    let (_dir, app) = temp_app();
    let router = app.router();
    let res = router
        .clone()
        .oneshot(
            Request::post("/api/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"encoder": {"bitrate_kbps": 50, "gop_frames": 12}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v = body_json(res).await;
    assert_eq!(v["applied"], true);

    let res = router
        .oneshot(Request::get("/api/config").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let cfg = body_json(res).await;
    assert_eq!(cfg["encoder"]["bitrate_kbps"], 2000); // clamped from 50
    assert_eq!(cfg["encoder"]["gop_frames"], 12);
    assert_eq!(cfg["encoder"]["mode"], "ffmpeg");
}

#[tokio::test]
async fn token_rejects_unauthorized_and_accepts_bearer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let mut cfg = Config::default();
    cfg.token = "s3cret".into();
    streamaid::config::save(&cfg, &path).unwrap();
    let app = App::new(cfg, path);
    let router = app.router();

    let res = router
        .clone()
        .oneshot(Request::get("/api/config").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let res = router
        .clone()
        .oneshot(
            Request::get("/api/config")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = router
        .oneshot(
            Request::get("/api/config?token=s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn public_ui_does_not_need_token() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let mut cfg = Config::default();
    cfg.token = "s3cret".into();
    streamaid::config::save(&cfg, &path).unwrap();
    let app = App::new(cfg, path);
    let router = app.router();
    let res = router
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn mp4_stream_is_chunked_no_transform_and_starts_with_init() {
    let (_dir, app) = temp_app();
    let mut init = mp4_box(b"ftyp", b"isom");
    init.extend_from_slice(&mp4_box(b"moov", b"trak"));
    let mut frag = mp4_box(b"moof", b"mfhd");
    frag.extend_from_slice(&mp4_box(b"mdat", b"mdat-bytes"));
    app.hub
        .publish_init(Bytes::from(init.clone()), 1920, 1080);
    app.hub
        .publish_unit(TYPE_FRAG, Bytes::from(frag.clone()), 0, 0);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = app.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    let url = format!("http://{addr}/stream.mp4");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap();
    let res = client.get(&url).send().await.expect("GET /stream.mp4");
    assert_eq!(res.status(), 200);
    let headers = res.headers();
    let cc = headers
        .get("cache-control")
        .unwrap()
        .to_str()
        .unwrap()
        .to_lowercase();
    assert!(cc.contains("no-store"), "{cc}");
    assert!(cc.contains("no-transform"), "{cc}");
    let te = headers
        .get("transfer-encoding")
        .map(|v| v.to_str().unwrap().to_lowercase())
        .unwrap_or_default();
    assert!(
        te.contains("chunked") || res.content_length().is_none(),
        "expected chunked streaming, te={te}"
    );
    use futures_util::StreamExt;
    let mut stream = res.bytes_stream();
    let mut buf = Vec::new();
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(2));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(b)) => {
                        buf.extend_from_slice(&b);
                        if buf.len() >= init.len() {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            _ = &mut deadline => break,
        }
    }
    assert!(buf.len() >= init.len(), "got {} bytes", buf.len());
    assert_eq!(&buf[..init.len()], init.as_slice());
}

#[tokio::test]
async fn concurrent_status_does_not_error() {
    let (_dir, app) = temp_app();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = app.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    let url = format!("http://{addr}/api/status");
    let client = reqwest::Client::new();
    let mut joins = Vec::new();
    for _ in 0..16 {
        let client = client.clone();
        let url = url.clone();
        joins.push(tokio::spawn(async move {
            let mut ok = 0;
            for _ in 0..25 {
                let res = client.get(&url).send().await.unwrap();
                if res.status().is_success() {
                    let v: Value = res.json().await.unwrap();
                    if v.get("capture").is_some() && v.get("stream").is_some() {
                        ok += 1;
                    }
                }
            }
            ok
        }));
    }
    let mut total = 0;
    for j in joins {
        total += j.await.unwrap();
    }
    assert_eq!(total, 16 * 25);
}

fn token_app() -> (tempfile::TempDir, std::sync::Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let mut cfg = Config::default();
    cfg.token = "s3cret".into();
    streamaid::config::save(&cfg, &path).unwrap();
    let app = App::new(cfg, path);
    (dir, app)
}

fn flip_pin(pin: &str) -> String {
    let mut c: Vec<char> = pin.chars().collect();
    let last = c.len() - 1;
    c[last] = if c[last] == '0' { '1' } else { '0' };
    c.into_iter().collect()
}

#[tokio::test]
async fn pin_mint_redeem_session_gates_watch_not_stream_token() {
    let (_dir, app) = token_app();
    let router = app.router();

    let denied = router
        .clone()
        .oneshot(Request::get("/stream.mp4").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let host = router
        .clone()
        .oneshot(
            Request::get("/stream.mp4?token=s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(host.status(), StatusCode::OK);

    let minted = router
        .clone()
        .oneshot(
            Request::post("/api/otp")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(minted.status(), StatusCode::OK);
    let pin_body = body_json(minted).await;
    let pin = pin_body["pin"].as_str().unwrap().to_string();
    assert_eq!(pin.len(), 6);
    assert!(pin.chars().all(|c| c.is_ascii_digit()));

    let shown = router
        .clone()
        .oneshot(
            Request::get("/api/otp")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let shown = body_json(shown).await;
    assert_eq!(shown["pin"], pin);

    let bad = router
        .clone()
        .oneshot(
            Request::post("/api/otp/redeem")
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": flip_pin(&pin)}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::UNAUTHORIZED);

    let ok = router
        .clone()
        .oneshot(
            Request::post("/api/otp/redeem")
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": pin}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);
    let cookies: Vec<String> = ok
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    let set_cookie = cookies.join("; ");
    assert!(set_cookie.contains("streamaid_session="));
    assert!(
        set_cookie.contains("Max-Age=86400"),
        "session cookie must last a day, got {set_cookie}"
    );
    let sess = body_json(ok).await;
    let session = sess["session"].as_str().unwrap().to_string();
    assert!(!session.is_empty());
    assert_eq!(
        sess["expires_in_s"].as_u64().unwrap_or(0),
        86400,
        "redeemed session must last a day"
    );

    let watch = router
        .oneshot(
            Request::get("/stream.mp4")
                .header("Cookie", format!("streamaid_session={session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(watch.status(), StatusCode::OK);
}

#[tokio::test]
async fn pin_regenerate_invalidates_previous() {
    let (_dir, app) = token_app();
    let router = app.router();
    let a = router
        .clone()
        .oneshot(
            Request::post("/api/otp")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let pin_a = body_json(a).await["pin"].as_str().unwrap().to_string();
    let b = router
        .clone()
        .oneshot(
            Request::post("/api/otp")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let pin_b = body_json(b).await["pin"].as_str().unwrap().to_string();
    assert_ne!(pin_a, pin_b);
    let old = router
        .clone()
        .oneshot(
            Request::post("/api/otp/redeem")
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": pin_a}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old.status(), StatusCode::UNAUTHORIZED);
    let new = router
        .oneshot(
            Request::post("/api/otp/redeem")
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": pin_b}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(new.status(), StatusCode::OK);
}

#[tokio::test]
async fn pin_rate_limit_and_expire() {
    use std::sync::Arc;
    use std::time::Duration;
    use streamaid::computer_use::DoneModel;
    use streamaid::input::FakeInjector;
    use streamaid::otp::{FakeClock, PIN_TTL, FAIL_LIMIT, LOCKOUT};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let mut cfg = Config::default();
    cfg.token = "s3cret".into();
    streamaid::config::save(&cfg, &path).unwrap();
    let clock = FakeClock::new();
    let app = App::new_for_test(
        cfg,
        path,
        clock.clone(),
        FakeInjector::new(),
        Arc::new(DoneModel),
    );
    let router = app.router();
    let minted = router
        .clone()
        .oneshot(
            Request::post("/api/otp")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let pin = body_json(minted).await["pin"].as_str().unwrap().to_string();
    let wrong = flip_pin(&pin);
    for _ in 0..FAIL_LIMIT {
        let res = router
            .clone()
            .oneshot(
                Request::post("/api/otp/redeem")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"pin": wrong}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
    let limited = router
        .clone()
        .oneshot(
            Request::post("/api/otp/redeem")
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": wrong}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

    let dir2 = tempfile::tempdir().unwrap();
    let path2 = dir2.path().join("config.json");
    let mut cfg2 = Config::default();
    cfg2.token = "s3cret".into();
    streamaid::config::save(&cfg2, &path2).unwrap();
    let clock2 = FakeClock::new();
    let app2 = App::new_for_test(
        cfg2,
        path2,
        clock2.clone(),
        FakeInjector::new(),
        Arc::new(DoneModel),
    );
    let router2 = app2.router();
    let minted2 = router2
        .clone()
        .oneshot(
            Request::post("/api/otp")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let pin2 = body_json(minted2).await["pin"].as_str().unwrap().to_string();
    clock2.advance(PIN_TTL + Duration::from_secs(1));
    let expired = router2
        .oneshot(
            Request::post("/api/otp/redeem")
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": pin2}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(expired.status(), StatusCode::UNAUTHORIZED);
    let _ = LOCKOUT;
}

#[tokio::test]
async fn computer_use_forbidden_when_disabled_and_stub_when_enabled() {
    use std::sync::Arc;
    use streamaid::computer_use::StubClickTypeModel;
    use streamaid::input::{FakeInjector, Injected};
    use streamaid::otp::FakeClock;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let mut cfg = Config::default();
    cfg.token = "s3cret".into();
    streamaid::config::save(&cfg, &path).unwrap();
    let fake = FakeInjector::new();
    let app = App::new_for_test(
        cfg,
        path,
        FakeClock::new(),
        fake.clone(),
        Arc::new(StubClickTypeModel::default()),
    );
    let router = app.clone().router();
    let minted = router
        .clone()
        .oneshot(
            Request::post("/api/otp")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let pin = body_json(minted).await["pin"].as_str().unwrap().to_string();
    let redeemed = router
        .clone()
        .oneshot(
            Request::post("/api/otp/redeem")
                .header("content-type", "application/json")
                .body(Body::from(json!({"pin": pin}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let session = body_json(redeemed).await["session"].as_str().unwrap().to_string();

    let off = router
        .clone()
        .oneshot(
            Request::post("/api/computer-use")
                .header("Cookie", format!("streamaid_session={session}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"task": "type hello"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(off.status(), StatusCode::FORBIDDEN);
    let err = body_json(off).await;
    assert!(err["error"].as_str().unwrap().contains("disabled"));

    app.cfg.lock().control.ai_enabled = true;
    // Simulate a live capture: the loop paces itself on fresh snapshots, so
    // publish one before the task and keep publishing new ones while it runs.
    use bytes::Bytes;
    use streamaid::protocol::TYPE_SNAP;
    app.hub
        .publish_unit(TYPE_SNAP, Bytes::from_static(b"\xff\xd8snap0"), 0, 0);
    let hub = app.hub.clone();
    let frames = tokio::spawn(async move {
        for _ in 0..6 {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            hub.publish_unit(TYPE_SNAP, Bytes::from_static(b"\xff\xd8snap"), 0, 0);
        }
    });
    let on = router
        .oneshot(
            Request::post("/api/computer-use")
                .header("Cookie", format!("streamaid_session={session}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"task": "type hello"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    frames.abort();
    assert_eq!(on.status(), StatusCode::OK);
    let body = body_json(on).await;
    assert_eq!(body["ok"], true);
    assert_eq!(
        fake.recorded(),
        vec![
            Injected::Click { x: 0.5, y: 0.5 },
            Injected::Type {
                text: "hello".into()
            }
        ]
    );
}

#[tokio::test]
async fn computer_use_grabs_snap_jpeg_not_fragment() {
    use std::sync::{Arc, Mutex};
    use streamaid::computer_use::ActionModel;
    use streamaid::input::{Action, FakeInjector};
    use streamaid::otp::FakeClock;
    use streamaid::protocol::{TYPE_FRAG, TYPE_SNAP};
    use bytes::Bytes;

    struct RecordJpeg {
        got: Mutex<Vec<Vec<u8>>>,
    }
    impl ActionModel for RecordJpeg {
        fn plan(&self, _task: &str, _step: u32, jpeg: &[u8]) -> Vec<Action> {
            self.got.lock().unwrap().push(jpeg.to_vec());
            vec![Action::Done]
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let mut cfg = Config::default();
    cfg.token = "s3cret".into();
    cfg.control.ai_enabled = true;
    streamaid::config::save(&cfg, &path).unwrap();
    let rec = Arc::new(RecordJpeg {
        got: Mutex::new(Vec::new()),
    });
    let app = App::new_for_test(
        cfg,
        path,
        FakeClock::new(),
        FakeInjector::new(),
        rec.clone(),
    );
    app.hub
        .publish_unit(TYPE_FRAG, Bytes::from_static(b"moof-not-a-jpeg"), 1920, 1080);
    app.hub
        .publish_unit(TYPE_SNAP, Bytes::from_static(b"\xff\xd8SNAP"), 0, 0);
    let applied = app.run_computer_use("do it").await;
    assert_eq!(applied, vec![Action::Done]);
    let frames = rec.got.lock().unwrap().clone();
    assert!(!frames.is_empty());
    assert_eq!(frames[0], b"\xff\xd8SNAP");
}

#[tokio::test]
async fn host_cancel_stops_running_ai_loop() {
    use std::sync::Arc;
    use streamaid::computer_use::ActionModel;
    use streamaid::input::{Action, FakeInjector, Injected};
    use streamaid::otp::FakeClock;

    struct WaitThenClick;
    impl ActionModel for WaitThenClick {
        fn plan(&self, _task: &str, step: u32, _jpeg: &[u8]) -> Vec<Action> {
            match step {
                0 => vec![Action::Wait { ms: 4000 }],
                _ => vec![Action::Click { x: 0.1, y: 0.1 }],
            }
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let mut cfg = Config::default();
    cfg.token = "s3cret".into();
    cfg.control.ai_enabled = true;
    streamaid::config::save(&cfg, &path).unwrap();
    let fake = FakeInjector::new();
    let app = App::new_for_test(
        cfg,
        path,
        FakeClock::new(),
        fake.clone(),
        Arc::new(WaitThenClick),
    );
    let router = app.clone().router();
    let run = {
        let app = app.clone();
        tokio::spawn(async move { app.run_computer_use("slow").await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    let cancel = router
        .oneshot(
            Request::post("/api/computer-use/cancel")
                .header("Authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cancel.status(), StatusCode::OK);
    let applied = tokio::time::timeout(std::time::Duration::from_secs(2), run)
        .await
        .expect("join")
        .unwrap();
    assert!(
        !applied.iter().any(|a| matches!(a, Action::Click { .. })),
        "cancel must stop before click, got {applied:?}"
    );
    assert!(!fake
        .recorded()
        .iter()
        .any(|e| matches!(e, Injected::Click { .. })));
}

#[tokio::test]
async fn production_app_wires_llm_model() {
    let (_dir, app) = temp_app();
    assert_eq!(app.model.lock().kind(), "llm");
}
