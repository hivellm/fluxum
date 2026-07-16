//! One page file per table per shard (TIER-023/TIER-024).
//!
//! `storage.page_dir/shard-<shard_id>/table-<table_id>.pages`: a 32-byte
//! superblock (in a reserved 256-byte slot at offset 0) followed by
//! variable-length physical page records (header + payload), allocated at
//! 256-byte granularity from a per-file first-fit free-extent list.
//!
//! Copy-on-write (TIER-025): a live page is never overwritten in place —
//! every write allocates a fresh extent, and the caller repoints its page
//! directory afterwards, returning the superseded extent to the free list.
//! Torn-page protection follows: a crash mid-write can only tear an extent
//! no directory references yet; the torn copy fails its CRC on any read and
//! is garbage.
//!
//! The in-memory free list is rebuilt from checkpoint manifests (T2.3); this
//! module treats a reopened file as allocate-from-the-end until the manifest
//! layer hands it the persisted free state. Durability is never this file's
//! job (recovery = checkpoint root + `CommitLog` replay, TIER-061).

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::error::{FluxumError, Result};

use super::format::{self, EXTENT_ALIGN, SUPERBLOCK_LEN};

/// A physical record location inside a page file: `len` is the exact record
/// length (header + stored payload); the occupied slot is `len` rounded up
/// to [`EXTENT_ALIGN`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    /// Byte offset of the record, always [`EXTENT_ALIGN`]-aligned.
    pub offset: u64,
    /// Exact record length in bytes.
    pub len: u64,
}

/// Round a record length up to its extent-slot size (TIER-024).
fn slot_len(len: u64) -> u64 {
    len.div_ceil(EXTENT_ALIGN) * EXTENT_ALIGN
}

/// One open page file with its free-extent list.
#[derive(Debug)]
pub struct PageFile {
    file: File,
    /// Allocation frontier: every byte at `offset >= end` is unallocated.
    end: u64,
    /// Free slots: offset → slot length. Adjacent slots are coalesced on
    /// free, so first-fit fragmentation stays bounded.
    free: BTreeMap<u64, u64>,
}

impl PageFile {
    /// Create a fresh page file with its superblock (fails if it exists).
    pub fn create(path: &Path, page_size: u32, shard_id: u32, table_id: u32) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let block = format::encode_superblock(page_size, shard_id, table_id);
        write_at(&file, 0, &block)?;
        Ok(Self {
            file,
            end: EXTENT_ALIGN, // superblock owns slot 0
            free: BTreeMap::new(),
        })
    }

    /// Open an existing page file, verifying its superblock against the
    /// expected coordinates. Returns the file and its recorded `page_size`.
    /// The free list starts empty (see the module docs).
    pub fn open(path: &Path, shard_id: u32, table_id: u32) -> Result<(Self, u32)> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut block = vec![0u8; SUPERBLOCK_LEN];
        read_at(&file, 0, &mut block)?;
        let page_size = format::decode_superblock(&block, shard_id, table_id)?;
        let len = file.metadata()?.len().max(EXTENT_ALIGN);
        Ok((
            Self {
                file,
                end: slot_len(len),
                free: BTreeMap::new(),
            },
            page_size,
        ))
    }

    /// Write one encoded page image to a freshly allocated extent (CoW,
    /// TIER-025) and return its location.
    pub fn write_page(&mut self, image: &[u8]) -> Result<Extent> {
        let len = image.len() as u64;
        let offset = self.allocate(slot_len(len));
        write_at(&self.file, offset, image)?;
        Ok(Extent { offset, len })
    }

    /// Read the exact record at `extent` with a single positional read
    /// (TIER-032 step 2). CRC verification is the caller's job — this layer
    /// returns raw bytes.
    pub fn read_page(&self, extent: Extent) -> Result<Vec<u8>> {
        let len = usize::try_from(extent.len).map_err(|_| {
            FluxumError::Storage(format!("extent length {} overflows usize", extent.len))
        })?;
        let mut image = vec![0u8; len];
        read_at(&self.file, extent.offset, &mut image)?;
        Ok(image)
    }

    /// Return a superseded extent's slot to the free list, coalescing with
    /// adjacent free slots.
    pub fn free_extent(&mut self, extent: Extent) {
        let mut offset = extent.offset;
        let mut len = slot_len(extent.len);
        // Merge the predecessor if it ends exactly where this slot starts.
        if let Some((&prev_off, &prev_len)) = self.free.range(..offset).next_back()
            && prev_off + prev_len == offset
        {
            self.free.remove(&prev_off);
            offset = prev_off;
            len += prev_len;
        }
        // Merge the successor if it starts exactly where this slot ends.
        if let Some(&next_len) = self.free.get(&(offset + len)) {
            self.free.remove(&(offset + len));
            len += next_len;
        }
        self.free.insert(offset, len);
    }

    /// First-fit allocation (TIER-024): the lowest-offset free slot that
    /// fits, splitting off any remainder; extend the file when none fits.
    fn allocate(&mut self, slot: u64) -> u64 {
        let found = self
            .free
            .iter()
            .find(|&(_, &len)| len >= slot)
            .map(|(&off, &len)| (off, len));
        match found {
            Some((offset, len)) => {
                self.free.remove(&offset);
                if len > slot {
                    self.free.insert(offset + slot, len - slot);
                }
                offset
            }
            None => {
                let offset = self.end;
                self.end += slot;
                offset
            }
        }
    }

    /// Flush file contents to stable storage (checkpoint fsync ordering is
    /// T2.3's; exposed so tests and the flush path can force durability).
    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Total allocated bytes (allocation frontier), for the
    /// `fluxum_coldtier_bytes` gauge.
    pub fn allocated_bytes(&self) -> u64 {
        self.end
    }
}

/// Positional write without moving shared state: loops until `buf` is fully
/// written at `offset`.
fn write_at(file: &File, offset: u64, buf: &[u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt as _;
        file.write_all_at(buf, offset)?;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt as _;
        let mut written = 0usize;
        while written < buf.len() {
            let n = file.seek_write(&buf[written..], offset + written as u64)?;
            if n == 0 {
                return Err(FluxumError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "page file write returned zero bytes",
                )));
            }
            written += n;
        }
    }
    Ok(())
}

/// Positional read: fills `buf` exactly from `offset` or fails.
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt as _;
        file.read_exact_at(buf, offset)?;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt as _;
        let mut read = 0usize;
        while read < buf.len() {
            let n = file.seek_read(&mut buf[read..], offset + read as u64)?;
            if n == 0 {
                return Err(FluxumError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "page file read past end",
                )));
            }
            read += n;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::format::{PageHeader, encode_page};
    use super::*;

    fn temp_file() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let path = dir.path().join("table-1.pages");
        (dir, path)
    }

    fn image(page_id: u64, payload: &[u8]) -> Vec<u8> {
        encode_page(&PageHeader::new(page_id, 1, 0, 0), payload).unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn create_open_round_trips_the_superblock() {
        let (_dir, path) = temp_file();
        drop(PageFile::create(&path, 8192, 7, 1).unwrap_or_else(|e| panic!("{e}")));
        let (_file, page_size) = PageFile::open(&path, 7, 1).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(page_size, 8192);
        // Wrong coordinates are rejected.
        assert!(PageFile::open(&path, 8, 1).is_err());
    }

    #[test]
    fn write_read_round_trips_pages() {
        let (_dir, path) = temp_file();
        let mut file = PageFile::create(&path, 4096, 0, 1).unwrap_or_else(|e| panic!("{e}"));
        let a = image(1, &[0xAA; 100]);
        let b = image(2, &[0xBB; 700]);
        let ea = file.write_page(&a).unwrap_or_else(|e| panic!("{e}"));
        let eb = file.write_page(&b).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(ea.offset % EXTENT_ALIGN, 0);
        assert_eq!(eb.offset % EXTENT_ALIGN, 0);
        assert_ne!(ea.offset, eb.offset);
        assert_eq!(file.read_page(ea).unwrap_or_else(|e| panic!("{e}")), a);
        assert_eq!(file.read_page(eb).unwrap_or_else(|e| panic!("{e}")), b);
    }

    #[test]
    fn free_list_is_first_fit_with_coalescing() {
        let (_dir, path) = temp_file();
        let mut file = PageFile::create(&path, 4096, 0, 1).unwrap_or_else(|e| panic!("{e}"));
        // Three 1-slot records back to back.
        let e1 = file
            .write_page(&image(1, &[1; 50]))
            .unwrap_or_else(|e| panic!("{e}"));
        let e2 = file
            .write_page(&image(2, &[2; 50]))
            .unwrap_or_else(|e| panic!("{e}"));
        let e3 = file
            .write_page(&image(3, &[3; 50]))
            .unwrap_or_else(|e| panic!("{e}"));
        let frontier = file.allocated_bytes();

        // Free the middle, then the first: they must coalesce into one
        // 2-slot extent that a 2-slot record can reuse (first-fit).
        file.free_extent(e2);
        file.free_extent(e1);
        let big = image(4, &[4; 300]); // needs 2 slots (332 bytes)
        let e4 = file.write_page(&big).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e4.offset, e1.offset, "coalesced slot reused first-fit");
        assert_eq!(file.allocated_bytes(), frontier, "no growth on reuse");
        assert_eq!(file.read_page(e4).unwrap_or_else(|e| panic!("{e}")), big);
        // e3 is untouched.
        assert_eq!(
            file.read_page(e3).unwrap_or_else(|e| panic!("{e}")),
            image(3, &[3; 50])
        );
    }

    #[test]
    fn reading_past_the_end_of_the_file_is_an_io_error() {
        let (_dir, path) = temp_file();
        let file = PageFile::create(&path, 4096, 0, 1).unwrap_or_else(|e| panic!("{e}"));
        // An extent no writer ever produced: positional read must fail
        // instead of returning zeroed bytes.
        let err = match file.read_page(Extent {
            offset: 1 << 20,
            len: 64,
        }) {
            Ok(_) => panic!("read past EOF returned bytes"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("read past end"), "{err}");
    }

    #[test]
    fn cow_never_reuses_a_live_offset() {
        let (_dir, path) = temp_file();
        let mut file = PageFile::create(&path, 4096, 0, 1).unwrap_or_else(|e| panic!("{e}"));
        let v1 = image(1, &[1; 64]);
        let e1 = file.write_page(&v1).unwrap_or_else(|e| panic!("{e}"));
        // A rewrite of the same page goes to a fresh extent while the old
        // one is still readable (the caller frees it only after repointing).
        let v2 = image(1, &[9; 64]);
        let e2 = file.write_page(&v2).unwrap_or_else(|e| panic!("{e}"));
        assert_ne!(e1.offset, e2.offset);
        assert_eq!(file.read_page(e1).unwrap_or_else(|e| panic!("{e}")), v1);
        assert_eq!(file.read_page(e2).unwrap_or_else(|e| panic!("{e}")), v2);
    }
}
