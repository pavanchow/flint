//! Flint's storage engine: a persistent append-only log with an in-memory index.
//! Writes append a record to the active segment and update an in-memory keydir
//! mapping each key to the byte offset of its latest value. Reads are one seek.
//! Compaction rewrites only the live keys into a fresh segment and drops the rest.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const HEADER: usize = 12; // crc(4) + klen(4) + vlen(4)
const TOMBSTONE: u32 = u32::MAX;
const SEG_EXT: &str = "flog";
/// Upper bound on a single key or value. Rejects absurd lengths on write and
/// caps replay allocations from a corrupt/hostile segment. Well below TOMBSTONE
/// so a real value length can never collide with the tombstone sentinel.
pub const MAX_KV: usize = 256 * 1024 * 1024;

/// Where a key's current value lives on disk.
#[derive(Clone, Copy)]
struct Loc {
    file_id: u64,
    val_pos: u64,
    val_len: u32,
}

struct Inner {
    dir: PathBuf,
    active_id: u64,
    active: File,
    active_len: u64,
    readers: HashMap<u64, File>,
    keydir: HashMap<Vec<u8>, Loc>,
    seg_cap: u64,
    /// When true, every write is fsync'd before returning (power-loss durable).
    sync_writes: bool,
}

/// fsync a directory so a rename/unlink of its entries is itself durable.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// An embedded key-value store backed by one directory of append-only segments.
pub struct Store {
    inner: Mutex<Inner>,
}

fn seg_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.{SEG_EXT}"))
}

fn encode(key: &[u8], val: Option<&[u8]>) -> Vec<u8> {
    let vlen = match val {
        Some(v) => v.len() as u32,
        None => TOMBSTONE,
    };
    let mut body = Vec::with_capacity(8 + key.len() + val.map_or(0, |v| v.len()));
    body.extend_from_slice(&(key.len() as u32).to_le_bytes());
    body.extend_from_slice(&vlen.to_le_bytes());
    body.extend_from_slice(key);
    if let Some(v) = val {
        body.extend_from_slice(v);
    }
    let crc = crc32fast::hash(&body);
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

impl Store {
    /// Open (creating if absent) a store rooted at `dir`. Replays every segment
    /// in order to rebuild the in-memory index. `seg_cap` is the byte size at
    /// which the active segment rolls over to a new one.
    pub fn open(dir: impl AsRef<Path>, seg_cap: u64) -> io::Result<Store> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut ids: Vec<u64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) == Some(SEG_EXT) {
                if let Some(id) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();

        let mut keydir: HashMap<Vec<u8>, Loc> = HashMap::new();
        let mut readers: HashMap<u64, File> = HashMap::new();
        for &id in &ids {
            let path = seg_path(&dir, id);
            replay(&path, id, &mut keydir)?;
            readers.insert(id, File::open(&path)?);
        }

        let active_id = ids.last().copied().unwrap_or(1);
        let active_path = seg_path(&dir, active_id);
        let active = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&active_path)?;
        let active_len = active.metadata()?.len();
        readers
            .entry(active_id)
            .or_insert(File::open(&active_path)?);

        Ok(Store {
            inner: Mutex::new(Inner {
                dir,
                active_id,
                active,
                active_len,
                readers,
                keydir,
                seg_cap,
                sync_writes: false,
            }),
        })
    }

    /// Enable per-write fsync. Off by default (crash-safe against process death,
    /// fast). On means every `set`/`delete` is durable against power loss.
    pub fn set_sync_writes(&self, on: bool) {
        self.inner.lock().unwrap().sync_writes = on;
    }

    pub fn set(&self, key: &[u8], val: &[u8]) -> io::Result<()> {
        if key.len() > MAX_KV || val.len() > MAX_KV {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "key or value exceeds MAX_KV"));
        }
        let mut g = self.inner.lock().unwrap();
        g.roll_if_needed()?;
        let rec = encode(key, Some(val));
        let rec_start = g.active_len;
        g.active.write_all(&rec)?;
        g.active_len += rec.len() as u64;
        let val_pos = rec_start + HEADER as u64 + key.len() as u64;
        let loc = Loc { file_id: g.active_id, val_pos, val_len: val.len() as u32 };
        g.keydir.insert(key.to_vec(), loc);
        if g.sync_writes {
            g.active.sync_all()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let mut g = self.inner.lock().unwrap();
        let loc = match g.keydir.get(key).copied() {
            Some(l) => l,
            None => return Ok(None),
        };
        Ok(Some(g.read_value(loc)?))
    }

    /// Append a tombstone and drop the key from the index. Returns whether the
    /// key was present.
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
        let mut g = self.inner.lock().unwrap();
        if !g.keydir.contains_key(key) {
            return Ok(false);
        }
        g.roll_if_needed()?;
        let rec = encode(key, None);
        g.active.write_all(&rec)?;
        g.active_len += rec.len() as u64;
        g.keydir.remove(key);
        if g.sync_writes {
            g.active.sync_all()?;
        }
        Ok(true)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().keydir.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Durably persist all buffered writes: fsync the active segment. This is a
    /// real `fsync`, not a userspace buffer flush, so data survives power loss.
    pub fn flush(&self) -> io::Result<()> {
        let g = self.inner.lock().unwrap();
        g.active.sync_all()
    }

    /// Number of on-disk segment files. Compaction drives this back toward 2.
    pub fn segment_count(&self) -> usize {
        self.inner.lock().unwrap().readers.len()
    }

    /// Merge every live key into one fresh segment and delete the old ones, then
    /// start a new empty active segment. Reclaims the space held by stale
    /// versions and tombstones.
    pub fn compact(&self) -> io::Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.compact()
    }
}

impl Inner {
    fn roll_if_needed(&mut self) -> io::Result<()> {
        if self.active_len < self.seg_cap {
            return Ok(());
        }
        self.active.sync_all()?; // the segment we are leaving must be durable
        let new_id = self.active_id + 1;
        let path = seg_path(&self.dir, new_id);
        self.active = OpenOptions::new().create(true).read(true).append(true).open(&path)?;
        self.active_len = 0;
        self.active_id = new_id;
        self.readers.insert(new_id, File::open(&path)?);
        Ok(())
    }

    fn read_value(&mut self, loc: Loc) -> io::Result<Vec<u8>> {
        let f = match self.readers.get_mut(&loc.file_id) {
            Some(f) => f,
            None => {
                let f = File::open(seg_path(&self.dir, loc.file_id))?;
                self.readers.entry(loc.file_id).or_insert(f)
            }
        };
        if loc.val_len as usize > MAX_KV {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "value length exceeds MAX_KV"));
        }
        f.seek(SeekFrom::Start(loc.val_pos))?;
        let mut buf = vec![0u8; loc.val_len as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn compact(&mut self) -> io::Result<()> {
        let merged_id = self.active_id + 1;
        let merged_path = seg_path(&self.dir, merged_id);
        let mut merged = OpenOptions::new().create(true).read(true).append(true).open(&merged_path)?;

        // Copy every live value into the merged segment, tracking new offsets.
        let keys: Vec<Vec<u8>> = self.keydir.keys().cloned().collect();
        let mut new_keydir: HashMap<Vec<u8>, Loc> = HashMap::with_capacity(keys.len());
        let mut pos: u64 = 0;
        for key in keys {
            let loc = self.keydir[&key];
            let val = self.read_value(loc)?;
            let rec = encode(&key, Some(&val));
            merged.write_all(&rec)?;
            let val_pos = pos + HEADER as u64 + key.len() as u64;
            new_keydir.insert(key, Loc { file_id: merged_id, val_pos, val_len: val.len() as u32 });
            pos += rec.len() as u64;
        }
        // The merged segment must be fully durable BEFORE any old segment is
        // unlinked, so a power loss mid-compaction can never destroy the only
        // copy of the data. On reopen the higher-id merged segment wins.
        merged.sync_all()?;
        sync_dir(&self.dir)?;

        let old_ids: Vec<u64> = self.readers.keys().copied().collect();

        // A fresh empty active segment follows the merged one.
        let active_id = merged_id + 1;
        let active_path = seg_path(&self.dir, active_id);
        let active = OpenOptions::new().create(true).read(true).append(true).open(&active_path)?;

        for id in old_ids {
            let _ = fs::remove_file(seg_path(&self.dir, id));
        }
        sync_dir(&self.dir)?; // make the unlinks durable too

        self.readers.clear();
        self.readers.insert(merged_id, File::open(&merged_path)?);
        self.readers.insert(active_id, File::open(&active_path)?);
        self.keydir = new_keydir;
        self.active = active;
        self.active_id = active_id;
        self.active_len = 0;
        Ok(())
    }
}

/// Scan one segment sequentially, folding its records into the keydir. Stops at
/// the first torn/corrupt record (a partial trailing write from a crash).
fn replay(path: &Path, file_id: u64, keydir: &mut HashMap<Vec<u8>, Loc>) -> io::Result<()> {
    let mut r = BufReader::new(File::open(path)?);
    let mut pos: u64 = 0;
    loop {
        let mut header = [0u8; HEADER];
        match r.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let klen = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let vlen = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let tomb = vlen == TOMBSTONE;
        let actual_vlen = if tomb { 0 } else { vlen as usize };

        // Cap lengths before allocating, so a corrupt/hostile segment cannot
        // request a multi-gigabyte buffer. Treat an over-cap length as the end
        // of valid data in this segment.
        if klen as usize > MAX_KV || actual_vlen > MAX_KV {
            break;
        }

        let mut key = vec![0u8; klen as usize];
        let mut val = vec![0u8; actual_vlen];
        if r.read_exact(&mut key).is_err() || r.read_exact(&mut val).is_err() {
            break; // torn write
        }

        let mut body = Vec::with_capacity(8 + key.len() + val.len());
        body.extend_from_slice(&klen.to_le_bytes());
        body.extend_from_slice(&vlen.to_le_bytes());
        body.extend_from_slice(&key);
        body.extend_from_slice(&val);
        if crc32fast::hash(&body) != crc {
            break; // corruption, stop replaying this segment
        }

        let rec_len = HEADER as u64 + klen as u64 + actual_vlen as u64;
        if tomb {
            keydir.remove(&key);
        } else {
            let val_pos = pos + HEADER as u64 + klen as u64;
            keydir.insert(key, Loc { file_id, val_pos, val_len: actual_vlen as u32 });
        }
        pos += rec_len;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        let mut p = std::env::temp_dir();
        // unique-ish per test without Instant/rand: use the test-provided suffix
        p.push(format!("flint-test-{}", std::process::id()));
        p
    }

    fn fresh(sub: &str) -> PathBuf {
        let mut d = tmp();
        d.push(sub);
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn set_get_delete() {
        let dir = fresh("basic");
        let s = Store::open(&dir, 1 << 20).unwrap();
        s.set(b"a", b"1").unwrap();
        s.set(b"b", b"two").unwrap();
        assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(s.get(b"b").unwrap().as_deref(), Some(&b"two"[..]));
        assert_eq!(s.get(b"missing").unwrap(), None);
        assert!(s.delete(b"a").unwrap());
        assert_eq!(s.get(b"a").unwrap(), None);
        assert!(!s.delete(b"a").unwrap());
    }

    #[test]
    fn overwrite_keeps_latest() {
        let dir = fresh("overwrite");
        let s = Store::open(&dir, 1 << 20).unwrap();
        s.set(b"k", b"old").unwrap();
        s.set(b"k", b"new").unwrap();
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = fresh("reopen");
        {
            let s = Store::open(&dir, 1 << 20).unwrap();
            s.set(b"k", b"v").unwrap();
            s.set(b"gone", b"x").unwrap();
            s.delete(b"gone").unwrap();
            s.flush().unwrap();
        }
        let s = Store::open(&dir, 1 << 20).unwrap();
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        assert_eq!(s.get(b"gone").unwrap(), None);
    }

    #[test]
    fn rolls_segments_then_reads() {
        let dir = fresh("roll");
        let s = Store::open(&dir, 64).unwrap(); // tiny cap forces rollovers
        for i in 0..50u32 {
            s.set(format!("key{i}").as_bytes(), format!("val{i}").as_bytes()).unwrap();
        }
        assert!(s.segment_count() > 1);
        for i in 0..50u32 {
            assert_eq!(
                s.get(format!("key{i}").as_bytes()).unwrap().as_deref(),
                Some(format!("val{i}").as_bytes())
            );
        }
    }

    #[test]
    fn compaction_reclaims_and_preserves() {
        let dir = fresh("compact");
        let s = Store::open(&dir, 128).unwrap();
        for i in 0..200u32 {
            s.set(b"hot", format!("v{i}").as_bytes()).unwrap(); // same key, many stale versions
        }
        s.set(b"cold", b"stays").unwrap();
        s.delete(b"cold").unwrap();
        s.set(b"live", b"yes").unwrap();
        let before = s.segment_count();
        s.compact().unwrap();
        assert!(s.segment_count() <= before);
        assert_eq!(s.get(b"hot").unwrap().as_deref(), Some(&b"v199"[..]));
        assert_eq!(s.get(b"cold").unwrap(), None);
        assert_eq!(s.get(b"live").unwrap().as_deref(), Some(&b"yes"[..]));
        // survives reopen after compaction
        drop(s);
        let s2 = Store::open(&dir, 128).unwrap();
        assert_eq!(s2.get(b"hot").unwrap().as_deref(), Some(&b"v199"[..]));
        assert_eq!(s2.get(b"live").unwrap().as_deref(), Some(&b"yes"[..]));
    }

    #[test]
    fn rejects_oversize_key_or_value() {
        let dir = fresh("oversize");
        let s = Store::open(&dir, 1 << 20).unwrap();
        let big = vec![0u8; MAX_KV + 1];
        assert!(s.set(b"k", &big).is_err());
        assert!(s.set(&big, b"v").is_err());
        assert!(s.set(b"k", b"ok").is_ok());
    }

    #[test]
    fn corrupt_huge_length_does_not_allocate_or_panic() {
        let dir = fresh("corruptlen");
        let _ = Store::open(&dir, 1 << 20).unwrap();
        // Hand-write a segment claiming a 4GB key. Replay must not allocate it.
        let seg = dir.join("000001.flog");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u32.to_le_bytes());          // crc (ignored, we break first)
        bytes.extend_from_slice(&0xFFFF_FFFEu32.to_le_bytes()); // klen ~4GB
        bytes.extend_from_slice(&0u32.to_le_bytes());           // vlen
        fs::write(&seg, &bytes).unwrap();
        let s = Store::open(&dir, 1 << 20).unwrap(); // must return, not OOM/panic
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn sync_writes_mode_persists() {
        let dir = fresh("fsync");
        {
            let s = Store::open(&dir, 1 << 20).unwrap();
            s.set_sync_writes(true);
            s.set(b"durable", b"yes").unwrap();
        }
        let s = Store::open(&dir, 1 << 20).unwrap();
        assert_eq!(s.get(b"durable").unwrap().as_deref(), Some(&b"yes"[..]));
    }

    #[test]
    fn values_with_spaces_and_binary() {
        let dir = fresh("binary");
        let s = Store::open(&dir, 1 << 20).unwrap();
        s.set(b"phrase", b"hello there world").unwrap();
        s.set(b"bin", &[0u8, 255, 10, 13, 32]).unwrap();
        assert_eq!(s.get(b"phrase").unwrap().as_deref(), Some(&b"hello there world"[..]));
        assert_eq!(s.get(b"bin").unwrap().as_deref(), Some(&[0u8, 255, 10, 13, 32][..]));
    }
}
