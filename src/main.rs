use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

mod config;

use config::{Config, Logger};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_socks::tcp::Socks5Stream;

/// Reads ~/.config/shoehorn/shoehorn.conf.
#[tokio::main]
async fn main() {
    let config = match Config::load() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("failed to load config: {e}");
            std::process::exit(1);
        }
    };
    let logger = match Logger::new(config.log_path.as_deref()) {
        Ok(logger) => logger,
        Err(e) => {
            eprintln!("failed to initialize logger: {e}");
            std::process::exit(1);
        }
    };
    let listen_addr = config.listen_addr;
    let socks_addr: Arc<str> = Arc::from(config.socks_addr);

    let listener = TcpListener::bind(&listen_addr).await.unwrap();
    logger.info(format!(
        "listening on {listen_addr}, forwarding via SOCKS5 {socks_addr}"
    ));
    match &config.log_path {
        Some(path) => logger.info(format!("logging to {}", path.display())),
        None => logger.info("file logging disabled"),
    }

    loop {
        let Ok((client, peer)) = listener.accept().await else {
            continue;
        };
        let socks_addr = Arc::clone(&socks_addr);
        let logger = logger.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(client, &socks_addr, &logger, peer).await {
                logger.error(format!("[{peer}] {e}"));
            }
        });
    }
}

async fn handle(
    client: TcpStream,
    socks_addr: &str,
    logger: &Logger,
    peer: SocketAddr,
) -> io::Result<()> {
    let mut reader = BufReader::new(client);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;
    let mut parts = request_line.trim_end().splitn(3, ' ');
    let method = parts.next().unwrap_or("").to_ascii_uppercase();
    let target = parts.next().unwrap_or("").to_string();

    if !method.is_empty() {
        logger.info(format!("[{peer}] request method={method} target={target}"));
    }

    match method.as_str() {
        "CONNECT" => handle_connect(reader, socks_addr, &target).await,
        _ if !method.is_empty() => handle_http(reader, socks_addr, &method, &target).await,
        _ => Err(io::Error::other("empty request")),
    }
}

async fn handle_connect(
    mut reader: BufReader<TcpStream>,
    socks_addr: &str,
    target: &str,
) -> io::Result<()> {
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
    }

    let mut client = reader.into_inner();
    let upstream = match Socks5Stream::connect(socks_addr, target).await {
        Ok(s) => s,
        Err(e) => {
            client
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                .await?;
            return Err(io::Error::other(format!(
                "SOCKS5 connect to {target} failed: {e}"
            )));
        }
    };

    client
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;

    let (mut cr, mut cw) = client.split();
    let (mut ur, mut uw) = tokio::io::split(upstream.into_inner());
    tokio::try_join!(
        tokio::io::copy(&mut cr, &mut uw),
        tokio::io::copy(&mut ur, &mut cw),
    )?;

    Ok(())
}

async fn handle_http(
    mut reader: BufReader<TcpStream>,
    socks_addr: &str,
    method: &str,
    target: &str,
) -> io::Result<()> {
    // target is an absolute URI: http://example.com:8080/path
    let without_scheme = target.strip_prefix("http://").unwrap_or(target);
    let slash_pos = without_scheme.find('/').unwrap_or(without_scheme.len());
    let host_port = &without_scheme[..slash_pos];
    let path = without_scheme.get(slash_pos..).unwrap_or("/");

    let (host, port) = match host_port.rfind(':') {
        Some(i) => (&host_port[..i], host_port[i + 1..].parse().unwrap_or(80u16)),
        None => (host_port, 80u16),
    };

    let mut upstream = Socks5Stream::connect(socks_addr, (host, port))
        .await
        .map_err(io::Error::other)?
        .into_inner();

    // Rewrite to relative path: "GET http://example.com/path HTTP/1.1" → "GET /path HTTP/1.1"
    upstream
        .write_all(format!("{method} {path} HTTP/1.1\r\n").as_bytes())
        .await?;

    // Forward headers, stripping Proxy-* and enforcing Connection: close
    let mut saw_connection = false;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("proxy-") {
            continue;
        }
        if lower.starts_with("connection:") {
            saw_connection = true;
            upstream.write_all(b"Connection: close\r\n").await?;
            continue;
        }
        upstream.write_all(line.as_bytes()).await?;
    }
    if !saw_connection {
        upstream.write_all(b"Connection: close\r\n").await?;
    }
    upstream.write_all(b"\r\n").await?;

    // Splice: request body → upstream, response → client
    let mut client = reader.into_inner();
    let (mut cr, mut cw) = client.split();
    let (mut ur, mut uw) = upstream.split();
    tokio::try_join!(
        tokio::io::copy(&mut cr, &mut uw),
        tokio::io::copy(&mut ur, &mut cw),
    )?;

    Ok(())
}
