# shoehorn

HTTP proxy that forwards traffic through a SOCKS5 server.

## Install

```sh
cargo install shoehorn
```

## Config

Create `~/.config/shoehorn/shoehorn.conf`:

```ini
listen_addr=127.0.0.1:8080
socks_addr=127.0.0.1:1080
log_path=/tmp/shoehorn.log
```

`log_path` is optional. Logs always go to stderr.

## Run

```sh
shoehorn
```

## Logging

Each accepted client connection gets a strictly increasing `task=N` identifier.
Task lifecycle lines include `target` and `active_tasks`. Completed tasks also
log `elapsed_ms`, `tx_bytes`, and `rx_bytes` on task end lines.

`CONNECT` requests are tunnels, so one spawned task owns one tunnel until it
closes. Requests for different HTTPS origins cannot share that task at the proxy
protocol layer.
