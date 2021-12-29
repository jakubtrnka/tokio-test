use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, debug_span, info, Instrument};
use tracing_subscriber::EnvFilter;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

async fn process(stream: TcpStream) {
    debug!("starting");
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    while let Some(Ok(payload)) = framed.next().await {
        let mut response = b"my answer is:".to_vec();
        payload
            .as_ref()
            .iter()
            .take(100)
            .copied()
            .for_each(|b| response.push(b));
        if framed.send(response.into()).await.is_err() {
            break;
        }
    }
    debug!("terminating");
}

#[tokio::main]
async fn main() -> Result {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info")))
        .init();
    let server = TcpListener::bind("127.0.0.1:13337").await?;
    info!("Eater started");
    while let Ok((stream, socket)) = server.accept().await {
        tokio::spawn(process(stream).instrument(debug_span!("connection", ?socket)));
    }
    Ok(())
}
