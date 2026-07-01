use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Default)]
pub struct RuntimeStats {
    next_task_id: AtomicUsize,
    active_tasks: AtomicUsize,
    total_tx_bytes: AtomicU64,
    total_rx_bytes: AtomicU64,
}

impl RuntimeStats {
    pub fn start_task(&self) -> CounterSnapshot {
        let id = self.next_task_id.fetch_add(1, Ordering::Relaxed) + 1;
        let active = self.active_tasks.fetch_add(1, Ordering::Relaxed) + 1;
        CounterSnapshot { id, active }
    }

    pub fn finish_task(&self, id: usize) -> CounterSnapshot {
        let active = self.active_tasks.fetch_sub(1, Ordering::Relaxed) - 1;
        CounterSnapshot { id, active }
    }

    pub fn add_tx_bytes(&self, bytes: u64) {
        self.total_tx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_rx_bytes(&self, bytes: u64) {
        self.total_rx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RuntimeSnapshot {
        RuntimeSnapshot {
            active_tasks: self.active_tasks.load(Ordering::Relaxed),
            tx_bytes: self.total_tx_bytes.load(Ordering::Relaxed),
            rx_bytes: self.total_rx_bytes.load(Ordering::Relaxed),
        }
    }
}

pub struct CounterSnapshot {
    pub id: usize,
    pub active: usize,
}

pub struct RuntimeSnapshot {
    pub active_tasks: usize,
    pub tx_bytes: u64,
    pub rx_bytes: u64,
}
