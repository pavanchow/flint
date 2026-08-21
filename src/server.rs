//! A small protocol over TCP so any client (even netcat) can talk to Flint,
//! plus a background thread that compacts segments as they pile up.
//!
//! Text commands (keys are whitespace-delimited, values are the rest of the line):
//!   SET <key> <value...>   -> OK
//!   GET <key>              -> $<len> then the raw value bytes, or `nil`
//!   DEL <key>              -> 1 or 0
//!   LEN                    -> number of live keys
//!   COMPACT / PING / QUIT
//!
//! Binary commands (length-prefixed, so keys/values may contain spaces or NUL):
//!   BSET <klen> <vlen>\n<key bytes><value bytes>   -> OK
//!   BGET <klen>\n<key bytes>                        -> $<len> then value bytes, or `nil`
//!   BDEL <klen>\n<key bytes>                        -> 1 or 0

use crate::store::MAX_KV;
use crate::Store;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const MAX_LINE: u64 = 16 * 1024 * 1024; // backstop for the text protocol

pub struct Config {
    pub port: u16,
    /// Run auto-compaction once the store holds more than this many segments.
    pub compact_at_segments: usize,
    /// How often the background thread checks.
    pub compact_interval: Duration,
    /// Reject new TCP connections past this many concurrent ones.
    pub max_connections: usize,
}

/// Decrements the live-connection counter when a connection's thread ends.
struct ConnGuard(Arc<AtomicUsize>);
impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub fn serve(store: Arc<Store>, cfg: Config) -> io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", cfg.port))?;
    eprintln!("flint listening on 127.0.0.1:{}", cfg.port);

    {
        let store = Arc::clone(&store);
        let threshold = cfg.compact_at_segments;
        let interval = cfg.compact_interval;
        thread::spawn(move || loop {
            thread::sleep(interval);
            if store.segment_count() > threshold {
                if let Err(e) = store.compact() {
                    eprintln!("flint: compaction failed: {e}");
                }
            }
        });
    }

    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming() {
        let mut stream = stream?;
        // Bound concurrency: reject rather than spawn an unbounded thread that
        // would exhaust file descriptors under a connection flood.
        if active.fetch_add(1, Ordering::SeqCst) >= cfg.max_connections {
            active.fetch_sub(1, Ordering::SeqCst);
            let _ = stream.write_all(b"ERR too many connections\n");
            continue;
        }
        let guard = ConnGuard(Arc::clone(&active));
        let store = Arc::clone(&store);
        thread::spawn(move || {
            let _guard = guard; // held for the life of the connection
            if let Err(e) = handle(store, stream) {
                if e.kind() != io::ErrorKind::UnexpectedEof {
                    eprintln!("flint: connection error: {e}");
                }
            }
        });
    }
    Ok(())
}

fn read_n(r: &mut impl BufRead, n: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn handle(store: Arc<Store>, stream: TcpStream) -> io::Result<()> {
    let mut w = stream.try_clone()?;
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        let read = (&mut r).take(MAX_LINE).read_line(&mut line)?;
        if read == 0 {
            return Ok(());
        }
        if read as u64 == MAX_LINE && !line.ends_with('\n') {
            w.write_all(b"ERR line too long\n")?;
            return Ok(());
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        let (cmd, rest) = match trimmed.split_once(' ') {
            Some((c, r)) => (c, r),
            None => (trimmed, ""),
        };
        match cmd.to_ascii_uppercase().as_str() {
            "SET" => {
                let (key, val) = match rest.split_once(' ') {
                    Some((k, v)) => (k, v),
                    None => {
                        w.write_all(b"ERR usage: SET <key> <value>\n")?;
                        continue;
                    }
                };
                store.set(key.as_bytes(), val.as_bytes())?;
                w.write_all(b"OK\n")?;
            }
            "GET" => reply_get(&mut w, &store.get(rest.as_bytes())?)?,
            "DEL" => {
                let hit = store.delete(rest.as_bytes())?;
                w.write_all(if hit { b"1\n" } else { b"0\n" })?;
            }
            // Length-prefixed binary variants: keys and values may hold spaces/NUL.
            "BSET" => {
                let (klen, vlen) = match parse_two_lens(rest) {
                    Some(pair) => pair,
                    None => {
                        w.write_all(b"ERR usage: BSET <klen> <vlen>\n")?;
                        continue;
                    }
                };
                if klen > MAX_KV || vlen > MAX_KV {
                    w.write_all(b"ERR length exceeds MAX_KV\n")?;
                    return Ok(()); // stream framing is now ambiguous, close it
                }
                let key = read_n(&mut r, klen)?;
                let val = read_n(&mut r, vlen)?;
                store.set(&key, &val)?;
                w.write_all(b"OK\n")?;
            }
            "BGET" => {
                let klen = match rest.trim().parse::<usize>() {
                    Ok(n) if n <= MAX_KV => n,
                    _ => {
                        w.write_all(b"ERR usage: BGET <klen>\n")?;
                        return Ok(());
                    }
                };
                let key = read_n(&mut r, klen)?;
                reply_get(&mut w, &store.get(&key)?)?;
            }
            "BDEL" => {
                let klen = match rest.trim().parse::<usize>() {
                    Ok(n) if n <= MAX_KV => n,
                    _ => {
                        w.write_all(b"ERR usage: BDEL <klen>\n")?;
                        return Ok(());
                    }
                };
                let key = read_n(&mut r, klen)?;
                let hit = store.delete(&key)?;
                w.write_all(if hit { b"1\n" } else { b"0\n" })?;
            }
            "LEN" => w.write_all(format!("{}\n", store.len()).as_bytes())?,
            "COMPACT" => {
                store.compact()?;
                w.write_all(b"OK\n")?;
            }
            "PING" => w.write_all(b"PONG\n")?,
            "QUIT" => return Ok(()),
            "" => {}
            other => {
                w.write_all(format!("ERR unknown command '{other}'\n").as_bytes())?;
            }
        }
        w.flush()?;
    }
}

fn reply_get(w: &mut impl Write, v: &Option<Vec<u8>>) -> io::Result<()> {
    match v {
        Some(v) => {
            w.write_all(format!("${}\n", v.len()).as_bytes())?;
            w.write_all(v)?;
            w.write_all(b"\n")?;
        }
        None => w.write_all(b"nil\n")?,
    }
    Ok(())
}

fn parse_two_lens(rest: &str) -> Option<(usize, usize)> {
    let (a, b) = rest.trim().split_once(' ')?;
    Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
}
