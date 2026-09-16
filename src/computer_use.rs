//! Snapshot → model actions → injector loop. Tests stub the model.

use base64::Engine;
use crate::config::LlmConfig;
use crate::hub::Hub;
use crate::input::{parse_control_json, Action, Injector};
use crate::snapshot::encode_snapshot;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const MAX_STEPS: u32 = 20;

pub trait ActionModel: Send + Sync {
    fn kind(&self) -> &'static str {
        "stub"
    }
    fn plan(&self, task: &str, step: u32, jpeg: &[u8]) -> Vec<Action>;
    fn plan_async<'a>(
        &'a self,
        task: &'a str,
        step: u32,
        jpeg: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Vec<Action>> + Send + 'a>> {
        let v = self.plan(task, step, jpeg);
        Box::pin(async move { v })
    }
}

pub struct DoneModel;

impl ActionModel for DoneModel {
    fn kind(&self) -> &'static str {
        "done"
    }
    fn plan(&self, _task: &str, _step: u32, _jpeg: &[u8]) -> Vec<Action> {
        vec![Action::Done]
    }
}

/// Scripted stub: step 0 click, step 1 type, then done. Ignores pixels.
pub struct StubClickTypeModel {
    pub x: f64,
    pub y: f64,
    pub text: String,
}

impl Default for StubClickTypeModel {
    fn default() -> Self {
        Self {
            x: 0.5,
            y: 0.5,
            text: "hello".into(),
        }
    }
}

impl ActionModel for StubClickTypeModel {
    fn plan(&self, _task: &str, step: u32, _jpeg: &[u8]) -> Vec<Action> {
        match step {
            0 => vec![Action::Click {
                x: self.x,
                y: self.y,
            }],
            1 => vec![Action::Type {
                text: self.text.clone(),
            }],
            _ => vec![Action::Done],
        }
    }
}

pub struct LlmActionModel {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    client: reqwest::Client,
}

impl LlmActionModel {
    pub fn from_cfg(cfg: &LlmConfig) -> Self {
        Self {
            base_url: cfg.base_url.clone(),
            api_key: cfg.api_key.clone(),
            model: cfg.model.clone(),
            client: reqwest::Client::new(),
        }
    }

    pub fn action_prompt(task: &str, step: u32) -> String {
        format!(
            "You operate a computer from a screenshot. Task: {task}\nStep: {step}\n\
             Click/move/scroll coordinates are normalized 0–1 relative to the image.\n\
             Respond ONLY with JSON: {{\"actions\":[{{\"action\":\"click\",\"x\":0.5,\"y\":0.5}},\
{{\"action\":\"type\",\"text\":\"hi\"}},{{\"action\":\"key\",\"key\":\"Enter\"}},\
{{\"action\":\"scroll\",\"x\":0.5,\"y\":0.5,\"dy\":-1}},{{\"action\":\"wait\",\"ms\":200}},\
{{\"action\":\"done\"}}]}}."
        )
    }

    pub async fn plan_vision(&self, task: &str, step: u32, jpeg: &[u8]) -> Vec<Action> {
        if self.api_key.trim().is_empty() {
            return vec![Action::Done];
        }
        let b64 = base64::engine::general_purpose::STANDARD.encode(jpeg);
        let url = format!(
            "{}/chat/completions",
            self.base_url.trim_end_matches('/')
        );
        let body = serde_json::json!({
            "model": self.model,
            "temperature": 0.2,
            "max_tokens": 1024,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": Self::action_prompt(task, step)},
                    {"type": "image_url", "image_url": {"url": format!("data:image/jpeg;base64,{b64}")}}
                ]
            }]
        });
        let res = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await;
        let Ok(res) = res else {
            return vec![Action::Done];
        };
        let Ok(v) = res.json::<Value>().await else {
            return vec![Action::Done];
        };
        if let Some(usage) = v.pointer("/usage") {
            tracing::info!(step, usage = %usage, "llm computer-use usage");
        }
        let content = v
            .pointer("/choices/0/message/content")
            .and_then(|c| c.as_str())
            .unwrap_or("");
        let parsed = extract_json_object(content).unwrap_or(Value::Null);
        let mut acts = actions_from_model_json(&parsed);
        if acts.is_empty() {
            acts.push(Action::Done);
        }
        acts
    }
}

impl ActionModel for LlmActionModel {
    fn kind(&self) -> &'static str {
        "llm"
    }
    fn plan(&self, _task: &str, _step: u32, _jpeg: &[u8]) -> Vec<Action> {
        // Sync path is unused; App drives plan_async → plan_vision.
        vec![Action::Done]
    }
    fn plan_async<'a>(
        &'a self,
        task: &'a str,
        step: u32,
        jpeg: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Vec<Action>> + Send + 'a>> {
        let task = task.to_string();
        let jpeg = jpeg.to_vec();
        Box::pin(async move { self.plan_vision(&task, step, &jpeg).await })
    }
}

pub fn production_model(llm: &LlmConfig) -> Arc<dyn ActionModel> {
    Arc::new(LlmActionModel::from_cfg(llm))
}

pub fn extract_json_object(text: &str) -> Option<Value> {
    let mut s = text.trim();
    if s.starts_with("```") {
        if let Some(nl) = s.find('\n') {
            s = &s[nl + 1..];
        }
        if let Some(stripped) = s.strip_suffix("```") {
            s = stripped.trim();
        }
    }
    let start = s.find('{')?;
    let mut depth = 0i32;
    let mut in_str = false;
    let mut esc = false;
    let bytes = s.as_bytes();
    for i in start..bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            if esc {
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        if c == '"' {
            in_str = true;
        } else if c == '{' {
            depth += 1;
        } else if c == '}' {
            depth -= 1;
            if depth == 0 {
                return serde_json::from_str(&s[start..=i]).ok();
            }
        }
    }
    None
}

pub fn actions_from_model_json(v: &Value) -> Vec<Action> {
    let list = if let Some(arr) = v.get("actions").and_then(|a| a.as_array()) {
        arr.clone()
    } else if v.get("action").is_some() {
        vec![v.clone()]
    } else {
        return vec![];
    };
    list.iter().filter_map(parse_control_json).collect()
}

pub fn run_task(
    task: &str,
    model: &dyn ActionModel,
    injector: &dyn Injector,
    jpeg: &[u8],
    max_steps: u32,
    cancel: &AtomicBool,
) -> Vec<Action> {
    let mut applied = Vec::new();
    let task = task.trim();
    if task.is_empty() {
        return applied;
    }
    for step in 0..max_steps {
        if cancel.load(Ordering::SeqCst) {
            break;
        }
        let actions = model.plan(task, step, jpeg);
        for a in actions {
            if cancel.load(Ordering::SeqCst) {
                return applied;
            }
            match &a {
                Action::Done => {
                    applied.push(a);
                    return applied;
                }
                Action::Wait { .. } => applied.push(a),
                _ => {
                    injector.apply(&a);
                    applied.push(a);
                }
            }
        }
    }
    applied
}

pub async fn run_task_async<G, Fut, W, FutW>(
    task: &str,
    model: &dyn ActionModel,
    injector: &dyn Injector,
    mut grab: G,
    mut wait_fresh: W,
    max_steps: u32,
    cancel: &AtomicBool,
) -> Vec<Action>
where
    G: FnMut() -> Fut,
    Fut: Future<Output = Vec<u8>>,
    W: FnMut() -> FutW,
    FutW: Future<Output = bool>,
{
    let mut applied = Vec::new();
    let task = task.trim();
    if task.is_empty() {
        return applied;
    }
    for step in 0..max_steps {
        if cancel.load(Ordering::SeqCst) {
            break;
        }
        // Steps after the first wait for a FRESH snapshot: planning again on the
        // same frame burns a paid call and cannot observe the actions just
        // applied. `false` means no fresh frame arrived in time, so stop instead
        // of looping on a frozen screen.
        if step > 0 && !wait_fresh().await {
            break;
        }
        let jpeg = grab().await;
        if jpeg.is_empty() {
            break;
        }
        let actions = model.plan_async(task, step, &jpeg).await;
        for a in actions {
            if cancel.load(Ordering::SeqCst) {
                return applied;
            }
            match &a {
                Action::Done => {
                    applied.push(a);
                    return applied;
                }
                Action::Wait { ms } => {
                    let ms = *ms;
                    applied.push(a);
                    let start = Instant::now();
                    while start.elapsed().as_millis() < ms as u128 {
                        if cancel.load(Ordering::SeqCst) {
                            return applied;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
                _ => {
                    injector.apply(&a);
                    applied.push(a);
                }
            }
        }
    }
    applied
}

/// Prefer TYPE_SNAP JPEG; otherwise encode the latest fragment via ffmpeg.
pub async fn grab_jpeg(hub: &Hub) -> Vec<u8> {
    if let Some(snap) = hub.last_snap() {
        if let Some(j) = encode_snapshot(None, &snap).await {
            return j;
        }
    }
    if let Some(lat) = hub.latest() {
        let init = hub.init_segment();
        if let Some(j) = encode_snapshot(init.as_deref(), &lat).await {
            return j;
        }
    }
    Vec::new()
}

pub fn shared_cancel() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{FakeInjector, Injected};
    use crate::protocol::TYPE_SNAP;
    use bytes::Bytes;

    #[test]
    fn stub_click_then_type_then_done() {
        let inj = FakeInjector::new();
        let model = StubClickTypeModel::default();
        let cancel = AtomicBool::new(false);
        let applied = run_task("open notes", &model, &inj, b"", MAX_STEPS, &cancel);
        assert_eq!(
            applied,
            vec![
                Action::Click { x: 0.5, y: 0.5 },
                Action::Type {
                    text: "hello".into()
                },
                Action::Done
            ]
        );
        assert_eq!(
            inj.recorded(),
            vec![
                Injected::Click { x: 0.5, y: 0.5 },
                Injected::Type {
                    text: "hello".into()
                }
            ]
        );
    }

    #[test]
    fn cancel_stops_loop() {
        let inj = FakeInjector::new();
        let model = StubClickTypeModel::default();
        let cancel = AtomicBool::new(true);
        let applied = run_task("x", &model, &inj, b"", MAX_STEPS, &cancel);
        assert!(applied.is_empty());
        assert!(inj.recorded().is_empty());
    }

    #[test]
    fn production_model_is_llm_not_done() {
        let m = production_model(&LlmConfig::default());
        assert_eq!(m.kind(), "llm");
        assert_ne!(m.kind(), "done");
    }

    #[test]
    fn actions_from_model_json_click_type_done() {
        let v = serde_json::json!({
            "actions": [
                {"action":"click","x":0.2,"y":0.3},
                {"action":"type","text":"hi"},
                {"action":"done"}
            ]
        });
        assert_eq!(
            actions_from_model_json(&v),
            vec![
                Action::Click { x: 0.2, y: 0.3 },
                Action::Type { text: "hi".into() },
                Action::Done
            ]
        );
    }

    #[tokio::test]
    async fn async_loop_grabs_a_new_frame_each_step() {
        let inj = FakeInjector::new();
        let model = StubClickTypeModel::default();
        let cancel = AtomicBool::new(false);
        let grabs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let g = grabs.clone();
        let applied = run_task_async(
            "t",
            &model,
            &inj,
            || {
                let g = g.clone();
                async move {
                    g.fetch_add(1, Ordering::SeqCst);
                    b"\xff\xd8snap".to_vec()
                }
            },
            || async { true },
            MAX_STEPS,
            &cancel,
        )
        .await;
        assert_eq!(applied.last(), Some(&Action::Done));
        assert!(grabs.load(Ordering::SeqCst) >= 3, "need a snapshot per step");
    }

    #[tokio::test]
    async fn async_loop_stops_when_no_fresh_frame_arrives() {
        let inj = FakeInjector::new();
        let model = StubClickTypeModel::default();
        let cancel = AtomicBool::new(false);
        let grabs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let g = grabs.clone();
        let applied = run_task_async(
            "t",
            &model,
            &inj,
            || {
                let g = g.clone();
                async move {
                    g.fetch_add(1, Ordering::SeqCst);
                    b"\xff\xd8snap".to_vec()
                }
            },
            || async { false }, // capture stalled: no fresh frame ever
            MAX_STEPS,
            &cancel,
        )
        .await;
        // Step 0 runs without a wait; the stalled capture then stops the loop
        // before it spends further calls on a frozen screen.
        assert_eq!(grabs.load(Ordering::SeqCst), 1);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0], Action::Click { x: 0.5, y: 0.5 });
    }

    #[tokio::test]
    async fn grab_jpeg_prefers_type_snap_over_fragment() {
        let hub = Hub::new();
        hub.publish_unit(crate::protocol::TYPE_FRAG, Bytes::from_static(b"moof-not-jpeg"), 1920, 1080);
        hub.publish_unit(TYPE_SNAP, Bytes::from_static(b"\xff\xd8SNAP"), 0, 0);
        let j = grab_jpeg(&hub).await;
        assert_eq!(j, b"\xff\xd8SNAP");
    }

    #[tokio::test]
    async fn llm_model_posts_jpeg_and_parses_actions() {
        use axum::routing::post;
        use axum::{Json, Router};
        use std::io::Write;
        use std::sync::{Arc, Mutex};
        use tokio::net::TcpListener;

        // Capture tracing output so we can assert the usage log actually fires.
        #[derive(Clone)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl Write for Buf {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let writer = captured.clone();
        let _ = tracing_subscriber::fmt()
            .with_writer(move || Buf(writer.clone()))
            .try_init();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/chat/completions",
                post(|Json(v): Json<Value>| async move {
                    let content = &v["messages"][0]["content"];
                    let arr = content.as_array().expect("content array");
                    assert!(arr.iter().any(|c| c["type"] == "image_url"));
                    Json(serde_json::json!({
                        "choices": [{"message": {"content":
                            "{\"actions\":[{\"action\":\"click\",\"x\":0.2,\"y\":0.3},{\"action\":\"done\"}]}"
                        }}],
                        "usage": {"prompt_tokens": 1234, "completion_tokens": 56, "total_tokens": 1290}
                    }))
                }),
            );
            axum::serve(listener, app).await.ok();
        });
        let model = LlmActionModel::from_cfg(&LlmConfig {
            base_url: format!("http://{addr}"),
            api_key: "k".into(),
            model: "m".into(),
            ..LlmConfig::default()
        });
        let acts = model
            .plan_async("click the button", 0, b"\xff\xd8\xff\xd9")
            .await;
        assert_eq!(
            acts,
            vec![
                Action::Click { x: 0.2, y: 0.3 },
                Action::Done
            ]
        );
        let captured_bytes = captured.lock().unwrap().clone();
        let log = String::from_utf8_lossy(&captured_bytes);
        assert!(
            log.contains("llm computer-use usage"),
            "origin computer-use path must log model token usage; got: {log}"
        );
        assert!(
            log.contains("1290") && log.contains("1234"),
            "logged usage must carry the reported token counts; got: {log}"
        );
    }
}
