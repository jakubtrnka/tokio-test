use futures::{Sink, SinkExt, StreamExt};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

use tokio_util::codec;
use tracing::{debug, debug_span, info, warn, Instrument};
use tracing_subscriber::EnvFilter;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

pub struct ProxyServer {
    up_sender: mpsc::Sender<Box<[u8]>>,
    down_sender: mpsc::Sender<Box<[u8]>>,
}

impl ProxyServer {
    pub async fn forward_up(&self, payload: &[u8]) -> Result {
        let mut augmented_payload = b"forwarded up:".to_vec();
        augmented_payload.extend_from_slice(payload);
        self.up_sender
            .send(augmented_payload.into_boxed_slice())
            .await?;
        Ok(())
    }

    pub async fn forward_down(&self, payload: &[u8]) -> Result {
        let mut augmented_payload = b"forwarded down:".to_vec();
        augmented_payload.extend_from_slice(payload);
        self.down_sender
            .send(augmented_payload.into_boxed_slice())
            .await?;
        Ok(())
    }
}

async fn sending_task(
    mut data_receiver: mpsc::Receiver<Box<[u8]>>,
    mut sender: impl Sink<bytes::Bytes> + Unpin,
    tasks_started: Arc<AtomicU64>,
    tasks_terminated: Arc<AtomicU64>,
) {
    tasks_started.fetch_add(1, Relaxed);
    while let Some(x) = data_receiver.recv().await {
        if sender.send(x.into()).await.is_err() {
            break;
        }
    }
    tasks_terminated.fetch_add(1, Relaxed);
}

pub async fn run(
    stream: tokio::net::TcpStream,
    tasks_started: Arc<AtomicU64>,
    tasks_terminated: Arc<AtomicU64>,
) {
    tasks_started.fetch_add(1, Relaxed);
    debug!("proxy-connection-start");
    let downstream = codec::Framed::new(stream, codec::LengthDelimitedCodec::new());
    let upstream = codec::Framed::new(
        if let Ok(stream) = tokio::net::TcpStream::connect("127.0.0.1:13337").await {
            stream
        } else {
            tasks_terminated.fetch_add(1, Relaxed);
            return;
        },
        codec::LengthDelimitedCodec::new(),
    );
    let (down_sender, down_receiver) = mpsc::channel::<Box<[u8]>>(1000);
    let (up_sender, up_receiver) = mpsc::channel::<Box<[u8]>>(1000);

    let (down_write, mut down_read) = downstream.split();
    let (up_write, mut up_read) = upstream.split();

    tokio::spawn(sending_task(
        down_receiver,
        down_write,
        tasks_started.clone(),
        tasks_terminated.clone(),
    ));
    tokio::spawn(sending_task(
        up_receiver,
        up_write,
        tasks_started.clone(),
        tasks_terminated.clone(),
    ));

    let proxy_server = ProxyServer {
        up_sender,
        down_sender,
    };

    loop {
        tokio::select! {
            payload = down_read.next() => {
                if let Some(Ok(bytes)) = payload {
                    if proxy_server.forward_up(&bytes).await.is_err() {
                        break
                    }
                } else {
                    break
                }
            }
            payload = up_read.next() => {
                if let Some(Ok(bytes)) = payload {
                    if proxy_server.forward_down(&bytes).await.is_err() {
                        break
                    }
                } else {
                    break
                }
            }
        }
    }
    tasks_terminated.fetch_add(1, Relaxed);
    debug!("proxy-connection-closed");
}

#[tokio::main]
async fn main() -> Result {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    // console_subscriber::init();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let tasks_started = Arc::new(AtomicU64::new(0));
    let tasks_terminated = Arc::new(AtomicU64::new(0));
    let task_terminated_copy = tasks_terminated.clone();
    let task_started_copy = tasks_started.clone();
    tokio::spawn(
        async move {
            loop {
                let started = task_started_copy.load(Relaxed);
                let terminated = task_terminated_copy.load(Relaxed);
                info!(
                    "started {}, termianted {}, currently running {} tokio tasks",
                    started,
                    terminated,
                    started - terminated
                );
                sleep(Duration::from_secs(5)).await;
            }
        }
        .instrument(tracing::info_span!("task monitor")),
    );

    info!("started proxy");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:13336").await?;
    loop {
        match listener.accept().await {
            Ok((stream, socket)) => {
                tokio::spawn(
                    run(stream, tasks_started.clone(), tasks_terminated.clone())
                        .instrument(debug_span!("proxy connection", ?socket)),
                );
            }
            Err(e) => {
                warn!("{}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
