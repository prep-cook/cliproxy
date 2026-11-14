use std::task::Poll;

use ck_stream::{Config, Read, Store, Writer};
use ck_translate::codec::{Result, source::Source};

/// Real mapped inputs for every protocol's semantic tests. Payload copies belong
/// only in assertions; production codecs receive the same lease-backed Source.
pub struct Mapped {
    #[allow(dead_code, reason = "boundary_codec uses Source; chat_codec feeds ingress directly")]
    pub source: Source,
    pub writer: Writer,
    pub store: Store,
    directory: std::path::PathBuf,
}

impl Mapped {
    pub fn new() -> Self {
        Self::with_clock(std::sync::Arc::new(ck_stream::ManualClock::default()))
    }

    pub fn with_clock(clock: std::sync::Arc<ck_stream::ManualClock>) -> Self {
        Self::configured(clock, 64)
    }

    pub fn configured(clock: std::sync::Arc<ck_stream::ManualClock>, frames: usize) -> Self {
        let directory = std::env::temp_dir().join(format!("ck-codec-{}", uuid::Uuid::new_v4()));
        let mut config = Config::new(directory.clone());
        config.pool_bytes = Some(frames * ck_stream::FRAME_BYTES);
        config.max_event_bytes = 16 * ck_stream::FRAME_BYTES;
        config.max_metadata_bytes = 4 * 1024 * 1024;
        config.header_slots = 8;
        config.workers = 1;
        let store = Store::with_clock(config, clock).unwrap();
        let mut writer = store.create().unwrap();
        Self {
            source: Source::new(&mut writer).unwrap(),
            writer,
            store,
            directory,
        }
    }

    #[allow(dead_code, reason = "boundary_codec uses Source; chat_codec feeds ingress directly")]
    pub fn push(&mut self, bytes: &[u8]) -> Result<()> {
        self.source.push_bytes(&mut self.writer, bytes)?;
        Ok(())
    }

    pub fn output(&self, writer: &Writer) -> Vec<u8> {
        let mut reader = self.store.reader(writer.id(), 0).unwrap();
        let mut bytes = Vec::new();
        while let Poll::Ready(Some(Read::Record(lease))) = reader.try_next() {
            lease.visit(|chunk| bytes.extend_from_slice(chunk)).unwrap();
        }
        bytes
    }
}

impl Drop for Mapped {
    fn drop(&mut self) {
        self.store.shutdown().unwrap();
        std::fs::remove_dir(&self.directory).unwrap();
    }
}
