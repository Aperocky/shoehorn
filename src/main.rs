use std::sync::Arc;
use std::time::{Duration, Instant};

mod config;
mod proxy;
mod runtime;

use config::{Config, Logger};
use runtime::RuntimeStats;
use tokio::net::TcpListener;

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
    let stats = Arc::new(RuntimeStats::default());

    let listener = TcpListener::bind(&listen_addr).await.unwrap();
    logger.info(format!(
        "listening on {listen_addr}, forwarding via SOCKS5 {socks_addr}"
    ));
    match &config.log_path {
        Some(path) => logger.info(format!("logging to {}", path.display())),
        None => logger.info("file logging disabled"),
    }
    let mut traffic_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(300),
        Duration::from_secs(300),
    );

    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = traffic_interval.tick() => {
                let snapshot = stats.snapshot();
                if snapshot.active_tasks > 0 {
                    logger.info(format!(
                        "traffic active_tasks={} total_tx_bytes={} total_rx_bytes={} total_bytes={}",
                        snapshot.active_tasks,
                        snapshot.tx_bytes,
                        snapshot.rx_bytes,
                        snapshot.tx_bytes + snapshot.rx_bytes
                    ));
                }
                continue;
            }
        };
        let Ok((client, peer)) = accepted else {
            continue;
        };

        let socks_addr = Arc::clone(&socks_addr);
        let logger = logger.clone();
        let stats = Arc::clone(&stats);
        tokio::spawn(async move {
            let started_at = Instant::now();
            let task = stats.start_task();
            let start_logger = logger.clone();
            let task_id = task.id;
            let active_tasks = task.active;

            let outcome = proxy::handle(client, &socks_addr, stats.as_ref(), move |target| {
                start_logger.info(format!(
                    "[task={task_id}] [{peer}] task start target={target} active_tasks={active_tasks}"
                ));
            })
            .await;
            let elapsed_ms = started_at.elapsed().as_millis();
            let task = stats.finish_task(task.id);
            let target = outcome.target.as_deref().unwrap_or("-");

            match outcome.error {
                None => logger.info(format!(
                    "[task={}] [{peer}] task end target={} active_tasks={} elapsed_ms={elapsed_ms} tx_bytes={} rx_bytes={}",
                    task.id,
                    target,
                    task.active,
                    outcome.transfer.client_to_upstream,
                    outcome.transfer.upstream_to_client
                )),
                Some(e) => logger.error(format!(
                    "[task={}] [{peer}] task end target={} active_tasks={} elapsed_ms={elapsed_ms} tx_bytes={} rx_bytes={} error={e}",
                    task.id,
                    target,
                    task.active,
                    outcome.transfer.client_to_upstream,
                    outcome.transfer.upstream_to_client
                )),
            }
        });
    }
}
