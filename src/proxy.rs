use std::io;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_socks::tcp::Socks5Stream;

struct RequestHead {
    method: String,
    target: String,
    version: String,
    headers: Vec<String>,
}

#[derive(Clone, Copy)]
struct BodyFraming {
    content_length: Option<u64>,
    chunked: bool,
}

#[derive(Clone, Copy, Default)]
pub struct TransferStats {
    pub client_to_upstream: u64,
    pub upstream_to_client: u64,
}

pub struct ProxyOutcome {
    pub target: Option<String>,
    pub transfer: TransferStats,
    pub error: Option<io::Error>,
}

impl TransferStats {
    fn add(&mut self, other: Self) {
        self.client_to_upstream += other.client_to_upstream;
        self.upstream_to_client += other.upstream_to_client;
    }
}

pub async fn handle<F>(client: TcpStream, socks_addr: &str, on_target: F) -> ProxyOutcome
where
    F: FnOnce(&str),
{
    let mut reader = BufReader::new(client);
    let mut transfers = TransferStats::default();
    let mut target = None;
    let mut on_target = Some(on_target);

    loop {
        let head = match read_request_head(&mut reader).await {
            Ok(Some(head)) => head,
            Ok(None) => {
                return ProxyOutcome {
                    target,
                    transfer: transfers,
                    error: None,
                };
            }
            Err(error) => {
                return ProxyOutcome {
                    target,
                    transfer: transfers,
                    error: Some(error),
                };
            }
        };

        if let Some(on_target) = on_target.take() {
            on_target(&head.target);
            target = Some(head.target.clone());
        }

        if head.method == "CONNECT" {
            let result = handle_connect(reader, socks_addr, &head.target).await;
            let transfer = result.as_ref().ok().copied();
            if let Some(transfer) = transfer {
                transfers.add(transfer);
            }
            return ProxyOutcome {
                target,
                transfer: transfers,
                error: result.err(),
            };
        }

        let should_continue = match handle_http(&mut reader, socks_addr, &head).await {
            Ok(should_continue) => should_continue,
            Err(error) => {
                return ProxyOutcome {
                    target,
                    transfer: transfers,
                    error: Some(error),
                };
            }
        };

        if !should_continue {
            return ProxyOutcome {
                target,
                transfer: transfers,
                error: None,
            };
        }
    }
}

async fn handle_connect(
    reader: BufReader<TcpStream>,
    socks_addr: &str,
    target: &str,
) -> io::Result<TransferStats> {
    let pending = reader.buffer().to_vec();
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

    let mut upstream = upstream.into_inner();
    if !pending.is_empty() {
        upstream.write_all(&pending).await?;
    }

    let (client_to_upstream, upstream_to_client) =
        tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;

    Ok(TransferStats {
        client_to_upstream: client_to_upstream + pending.len() as u64,
        upstream_to_client,
    })
}

async fn handle_http(
    reader: &mut BufReader<TcpStream>,
    socks_addr: &str,
    head: &RequestHead,
) -> io::Result<bool> {
    // target is an absolute URI: http://example.com:8080/path
    let without_scheme = head.target.strip_prefix("http://").unwrap_or(&head.target);
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

    // Rewrite to relative path: "GET http://example.com/path HTTP/1.1" -> "GET /path HTTP/1.1"
    upstream
        .write_all(format!("{} {path} {}\r\n", head.method, head.version).as_bytes())
        .await?;

    let request_framing = request_body_framing(head);
    let close_after_response = client_requested_close(head);
    for line in &head.headers {
        let lower = line.to_ascii_lowercase();
        if is_hop_by_hop_header(&lower) {
            continue;
        }
        upstream.write_all(line.as_bytes()).await?;
    }
    upstream.write_all(b"Connection: close\r\n").await?;
    upstream.write_all(b"\r\n").await?;

    copy_body(reader, &mut upstream, request_framing).await?;
    upstream.shutdown().await?;

    let response_allows_reuse = relay_response(&head.method, upstream, reader.get_mut()).await?;

    Ok(response_allows_reuse && !close_after_response)
}

async fn read_request_head(reader: &mut BufReader<TcpStream>) -> io::Result<Option<RequestHead>> {
    let mut request_line = String::new();
    let bytes = reader.read_line(&mut request_line).await?;
    if bytes == 0 {
        return Ok(None);
    }
    if request_line == "\r\n" || request_line == "\n" {
        return Ok(None);
    }

    let mut parts = request_line.trim_end().splitn(3, ' ');
    let method = parts.next().unwrap_or("").to_ascii_uppercase();
    let target = parts.next().unwrap_or("").to_string();
    let version = parts.next().unwrap_or("HTTP/1.1").to_string();

    if method.is_empty() || target.is_empty() {
        return Err(io::Error::other("malformed request line"));
    }

    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
        headers.push(line);
    }

    Ok(Some(RequestHead {
        method,
        target,
        version,
        headers,
    }))
}

fn request_body_framing(head: &RequestHead) -> BodyFraming {
    body_framing(&head.headers)
}

fn body_framing(headers: &[String]) -> BodyFraming {
    let mut content_length = None;
    let mut chunked = false;

    for line in headers {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().ok();
        } else if name.eq_ignore_ascii_case("transfer-encoding")
            && value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        {
            chunked = true;
        }
    }

    BodyFraming {
        content_length,
        chunked,
    }
}

fn client_requested_close(head: &RequestHead) -> bool {
    head.version.eq_ignore_ascii_case("HTTP/1.0")
        || head.headers.iter().any(|line| {
            let Some((name, value)) = line.split_once(':') else {
                return false;
            };
            (name.eq_ignore_ascii_case("connection")
                || name.eq_ignore_ascii_case("proxy-connection"))
                && value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("close"))
        })
}

fn is_hop_by_hop_header(lower: &str) -> bool {
    lower.starts_with("connection:")
        || lower.starts_with("proxy-connection:")
        || lower.starts_with("proxy-")
        || lower.starts_with("keep-alive:")
        || lower.starts_with("te:")
        || lower.starts_with("trailer:")
        || lower.starts_with("upgrade:")
}

async fn copy_body<R, W>(reader: &mut R, writer: &mut W, framing: BodyFraming) -> io::Result<()>
where
    R: tokio::io::AsyncBufRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    if framing.chunked {
        copy_chunked_body(reader, writer).await
    } else if let Some(content_length) = framing.content_length {
        let mut limited = reader.take(content_length);
        tokio::io::copy(&mut limited, writer).await?;
        Ok(())
    } else {
        Ok(())
    }
}

async fn copy_chunked_body<R, W>(reader: &mut R, writer: &mut W) -> io::Result<()>
where
    R: tokio::io::AsyncBufRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    loop {
        let mut size_line = String::new();
        reader.read_line(&mut size_line).await?;
        writer.write_all(size_line.as_bytes()).await?;

        let size_text = size_line
            .split_once(';')
            .map(|(size, _)| size)
            .unwrap_or(&size_line);
        let chunk_size = u64::from_str_radix(size_text.trim(), 16)
            .map_err(|_| io::Error::other("invalid chunk size"))?;

        if chunk_size == 0 {
            loop {
                let mut trailer = String::new();
                reader.read_line(&mut trailer).await?;
                writer.write_all(trailer.as_bytes()).await?;
                if trailer == "\r\n" || trailer == "\n" || trailer.is_empty() {
                    return Ok(());
                }
            }
        }

        let mut limited = reader.take(chunk_size + 2);
        tokio::io::copy(&mut limited, writer).await?;
    }
}

async fn relay_response(
    request_method: &str,
    upstream: TcpStream,
    client: &mut TcpStream,
) -> io::Result<bool> {
    let mut upstream = BufReader::new(upstream);
    let mut status_line = String::new();
    upstream.read_line(&mut status_line).await?;
    if status_line.is_empty() {
        return Err(io::Error::other("empty upstream response"));
    }
    client.write_all(status_line.as_bytes()).await?;

    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        upstream.read_line(&mut line).await?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
        headers.push(line);
    }

    let framing = body_framing(&headers);
    let can_reuse = response_has_self_defined_length(request_method, &status_line, framing);
    for line in &headers {
        if !is_hop_by_hop_header(&line.to_ascii_lowercase()) {
            client.write_all(line.as_bytes()).await?;
        }
    }
    client.write_all(b"\r\n").await?;

    if !response_has_body(request_method, &status_line) {
        return Ok(can_reuse);
    }

    if framing.chunked || framing.content_length.is_some() {
        copy_body(&mut upstream, client, framing).await?;
    } else {
        tokio::io::copy(&mut upstream, client).await?;
    }

    Ok(can_reuse)
}

fn response_has_body(request_method: &str, status_line: &str) -> bool {
    if request_method.eq_ignore_ascii_case("HEAD") {
        return false;
    }

    let status = status_code(status_line).unwrap_or(0);
    !(status / 100 == 1 || status == 204 || status == 304)
}

fn response_has_self_defined_length(
    request_method: &str,
    status_line: &str,
    framing: BodyFraming,
) -> bool {
    !response_has_body(request_method, status_line)
        || framing.chunked
        || framing.content_length.is_some()
}

fn status_code(status_line: &str) -> Option<u16> {
    status_line.split_whitespace().nth(1)?.parse().ok()
}
