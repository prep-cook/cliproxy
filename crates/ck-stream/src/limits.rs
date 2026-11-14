use std::{ffi::CString, io, os::unix::ffi::OsStrExt, path::PathBuf, time::Duration};

use crate::{
    FRAME_BYTES,
    publication::{Frame, Index},
};

pub const MAX_POOL_BYTES: usize = 4 * 1024 * 1024 * 1024;
pub const MAX_STREAM_FRAMES: u32 = 5_368;
const HEAP_HEADROOM: usize = 64 * 1024 * 1024;

/// Deploy-only settings. `None` selects the capped default, not an unlimited pool.
#[derive(Clone, Debug)]
pub struct Config {
    pub directory: PathBuf,
    pub pool_bytes: Option<usize>,
    /// Persistent warm backing, rounded up to frames. None or zero disables the reserve.
    pub startup_reserve_bytes: Option<usize>,
    pub max_frames: Option<u32>,
    pub max_event_bytes: usize,
    pub max_metadata_bytes: usize,
    pub cache_ttl: Duration,
    /// Mapped header slots. Not a cache limit: creating a stream with none free evicts the
    /// least recently read stream.
    pub header_slots: u32,
    pub workers: usize,
}

impl Config {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            pool_bytes: None,
            startup_reserve_bytes: None,
            max_frames: None,
            max_event_bytes: 1024 * 1024,
            max_metadata_bytes: 1024 * 1024,
            cache_ttl: Duration::from_secs(3600),
            header_slots: 4096,
            workers: std::thread::available_parallelism().map_or(1, usize::from),
        }
    }

    /// Resolve the default directory without falling back to a public temporary path.
    pub fn from_environment() -> io::Result<Self> {
        let directory = std::env::var_os("XDG_RUNTIME_DIR")
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "set XDG_RUNTIME_DIR or an explicit stream.pool_directory",
                )
            })?;
        Ok(Self::new(PathBuf::from(directory).join("cooker")))
    }

    pub(crate) fn resolve(&self) -> io::Result<Limits> {
        let mut path = self.directory.as_path();
        while !path.exists() {
            path = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .ok_or_else(|| invalid("pool directory must have an existing parent"))?;
        }
        let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| invalid("directory contains NUL"))?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: path is NUL terminated and stat points to writable storage of the ABI type.
        if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful statvfs initialized every field.
        let stat = unsafe { stat.assume_init() };
        let fs_cap = (stat.f_blocks as u128 * stat.f_frsize as u128 / 2).min(usize::MAX as u128) as usize;
        self.resolve_caps(fs_cap, cgroup_cap()?)
    }

    fn resolve_caps(&self, fs_cap: usize, cgroup: Option<usize>) -> io::Result<Limits> {
        if self
            .pool_bytes
            .is_some_and(|n| n == 0 || n % FRAME_BYTES != 0 || n > MAX_POOL_BYTES)
        {
            return Err(invalid("pool_bytes must be a nonzero frame multiple at most 4 GiB"));
        }
        let bytes = self
            .pool_bytes
            .unwrap_or(MAX_POOL_BYTES)
            .min(fs_cap)
            .min(cgroup.unwrap_or(usize::MAX));
        let pool_frames = u32::try_from(bytes / FRAME_BYTES).map_err(|_| invalid("pool capacity overflow"))?;
        let capacity_bytes = pool_frames as usize * FRAME_BYTES;
        let reserve_bytes = self.startup_reserve_bytes.unwrap_or_default();
        if reserve_bytes > capacity_bytes {
            return Err(invalid("startup_reserve_bytes exceeds the resolved pool capacity"));
        }
        let startup_reserve_frames = reserve_bytes.div_ceil(FRAME_BYTES) as u32;
        let max_frames = self.max_frames.unwrap_or(MAX_STREAM_FRAMES.min(pool_frames));
        if !(4..=MAX_STREAM_FRAMES).contains(&max_frames) || max_frames > pool_frames {
            return Err(invalid("max_frames must be between 4 and min(5368, pool_frames)"));
        }
        if self.max_event_bytes == 0
            || self.max_event_bytes > max_frames as usize * FRAME_BYTES
            || self.cache_ttl.is_zero()
            || self.header_slots == 0
            || self.workers == 0
        {
            return Err(invalid("invalid event size, cache ttl, header slots or worker count"));
        }
        let minimum = (max_frames as usize)
            .checked_mul(std::mem::size_of::<Frame>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Index>()))
            .and_then(|n| n.checked_add(crate::publication::READER_BYTES))
            .ok_or_else(|| invalid("metadata capacity overflow"))?;
        if self.max_metadata_bytes < minimum {
            return Err(invalid("metadata limit cannot hold the frame table, event and reader"));
        }
        Ok(Limits {
            pool_frames,
            startup_reserve_frames,
            max_frames,
            max_event_bytes: self.max_event_bytes,
            max_metadata_bytes: self.max_metadata_bytes,
            cache_ttl: self.cache_ttl,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub pool_frames: u32,
    pub startup_reserve_frames: u32,
    pub max_frames: u32,
    pub max_event_bytes: usize,
    pub max_metadata_bytes: usize,
    pub cache_ttl: Duration,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn cgroup_cap() -> io::Result<Option<usize>> {
    let membership = std::fs::read_to_string("/proc/self/cgroup")?;
    let Some(relative) = membership.lines().find_map(|line| line.strip_prefix("0::")) else {
        return Ok(None);
    };
    let root = std::path::Path::new("/sys/fs/cgroup");
    let mut path = root.join(relative.trim_start_matches('/'));
    let mut cap = None;
    loop {
        match std::fs::read_to_string(path.join("memory.max")) {
            Ok(value) if value.trim() != "max" => {
                let value: usize = value.trim().parse().map_err(|_| invalid("invalid cgroup memory.max"))?;
                let available = value.saturating_sub(HEAP_HEADROOM);
                cap = Some(cap.map_or(available, |old: usize| old.min(available)));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if path == root || !path.pop() {
            break;
        }
    }
    Ok(cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_pool_derives_quota_and_rejects_explicit_overcommit() {
        let mut config = Config::new(PathBuf::from("/unused"));
        config.max_event_bytes = 100_000;
        let limits = config.resolve_caps(500_000, None).unwrap();
        assert_eq!(limits.pool_frames, 10);
        assert_eq!(limits.max_frames, 10);
        config.max_frames = Some(11);
        assert!(config.resolve_caps(500_000, None).is_err());
        config.max_frames = None;
        assert_eq!(config.resolve_caps(500_000, Some(300_000)).unwrap().max_frames, 6);
        assert!(config.resolve_caps(150_000, None).is_err());
        config.startup_reserve_bytes = Some(500_000);
        assert_eq!(config.resolve_caps(500_000, None).unwrap().startup_reserve_frames, 10);
        config.startup_reserve_bytes = Some(500_001);
        assert!(config.resolve_caps(500_000, None).is_err());
        config.startup_reserve_bytes = Some(50_001);
        assert_eq!(config.resolve_caps(500_000, None).unwrap().startup_reserve_frames, 2);
        config.startup_reserve_bytes = Some(0);
        assert_eq!(config.resolve_caps(500_000, None).unwrap().startup_reserve_frames, 0);
    }
}
