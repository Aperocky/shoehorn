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
