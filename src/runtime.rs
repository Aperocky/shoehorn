use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub struct RuntimeStats {
    next_task_id: AtomicUsize,
    active_tasks: AtomicUsize,
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
}

pub struct CounterSnapshot {
    pub id: usize,
    pub active: usize,
}
