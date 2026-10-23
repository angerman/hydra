use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
#[allow(clippy::struct_field_names)]
pub struct Metrics {
    pub substituting_path_count: AtomicU64,

    pub uploading_path_count: AtomicU64,
    pub downloading_path_count: AtomicU64,
}

impl Metrics {
    /// Count `v` paths as substituting until the returned guard is dropped.
    pub fn substituting_paths(&self, v: u64) -> InFlight<'_> {
        InFlight::new(&self.substituting_path_count, v)
    }

    #[must_use]
    pub fn get_substituting_path_count(&self) -> u64 {
        self.substituting_path_count.load(Ordering::Relaxed)
    }

    /// Count `v` paths as uploading until the returned guard is dropped.
    pub fn uploading_paths(&self, v: u64) -> InFlight<'_> {
        InFlight::new(&self.uploading_path_count, v)
    }

    #[must_use]
    pub fn get_uploading_path_count(&self) -> u64 {
        self.uploading_path_count.load(Ordering::Relaxed)
    }

    /// Count `v` paths as downloading until the returned guard is dropped.
    pub fn downloading_paths(&self, v: u64) -> InFlight<'_> {
        InFlight::new(&self.downloading_path_count, v)
    }

    #[must_use]
    pub fn get_downloading_path_count(&self) -> u64 {
        self.downloading_path_count.load(Ordering::Relaxed)
    }
}

/// Paths counted as in flight until dropped.  A guard rather than paired
/// add/sub calls, so a transfer that fails or is aborted (as when the
/// queue-runner tunnel drops) does not leave the gauge raised for the life of
/// the builder.
#[must_use]
#[derive(Debug)]
pub struct InFlight<'a> {
    counter: &'a AtomicU64,
    n: u64,
}

impl<'a> InFlight<'a> {
    fn new(counter: &'a AtomicU64, n: u64) -> Self {
        counter.fetch_add(n, Ordering::Relaxed);
        Self { counter, n }
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.n, Ordering::Relaxed);
    }
}
