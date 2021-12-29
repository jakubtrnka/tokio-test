use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, sleep_until, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, debug_span, info, warn, Instrument};
use tracing_subscriber::EnvFilter;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

async fn process(stream: TcpStream, timeout: u64) {
    let framed = Framed::new(stream, LengthDelimitedCodec::new());
    let (mut write, mut read) = framed.split();

    tokio::spawn(async move {
        while let Some(payload) = read.next().await {
            debug!("Received {:?}", payload);
        }
    });
    let timeout_at = Instant::now() + Duration::from_secs(timeout);
    let mut counter: u32 = 0;
    loop {
        counter = counter.wrapping_add(1);
        let payload = format!("Some string {}_{}", timeout, counter);
        if write
            .send(payload.as_bytes().to_vec().into())
            .await
            .is_err()
        {
            break;
        }
        tokio::select! {
            _ = sleep(std::time::Duration::from_millis(1000)) => {}
            _ = sleep_until(timeout_at) => break
        }
    }
    write.close().await.unwrap();
}

#[tokio::main]
async fn main() -> Result {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info")))
        .init();
    let connection_count = std::env::args()
        .nth(1)
        .expect("Missing number of upstream connections")
        .parse::<usize>()
        .expect("Invalid number");

    let mut tasks = Vec::new();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(connection_count));
    let mut session_timeout = 10_u64;
    let mut idx = 0_u64;
    loop {
        let permit = semaphore.clone().acquire_owned().await?;
        let stream = if let Ok(stream) = TcpStream::connect("127.0.0.1:13336").await {
            stream
        } else {
            warn!("Target busy");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        };
        idx = idx.wrapping_add(1);
        // simulate random numbers
        session_timeout = (19 * session_timeout + 3) % 59;
        tasks.push(tokio::spawn(
            async move {
                info!("started idx {}", idx);
                // tokio::time::timeout(
                //     std::time::Duration::from_secs(session_timeout),
                //     process(stream, idx),
                // )
                //     .await
                //     .ok();

                process(stream, session_timeout).await;
                debug!("terminated");
                drop(permit)
            }
            .instrument(debug_span!("client", idx)),
        ));
        tokio::time::sleep(std::time::Duration::from_micros(200)).await;
    }
}
