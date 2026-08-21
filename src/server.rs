//! A small line protocol over TCP so any client (even netcat) can talk to Flint,
//! plus a background thread that compacts segments as they pile up.
//!
//!   SET <key> <value...>   -> OK
//!   GET <key>              -> $<len> then the raw value bytes, or `nil`
//!   DEL <key>              -> 1 or 0
//!   LEN                    -> number of live keys
//!   COMPACT                -> OK
//!   PING                   -> PONG
//!
//! Keys are whitespace-delimited, so keys themselves cannot contain spaces over
//! this protocol; a value is the rest of the line and may contain spaces.

use crate::Store;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub struct Config {
    pub port: u16,
    /// Run auto-compaction once the store holds more than this many segments.
    pub compact_at_segments: usize,
    /// How often the background thread checks.
    pub compact_interval: Duration,
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

    for stream in listener.incoming() {
        let stream = stream?;
        let store = Arc::clone(&store);
        thread::spawn(move || {
            if let Err(e) = handle(store, stream) {
                if e.kind() != io::ErrorKind::UnexpectedEof {
                    eprintln!("flint: connection error: {e}");
                }
            }
        });
    }
    Ok(())
}

fn handle(store: Arc<Store>, stream: TcpStream) -> io::Result<()> {
    let mut w = stream.try_clone()?;
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        if r.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let line = line.trim_end_matches(['\r', '\n']);
        let (cmd, rest) = match line.split_once(' ') {
            Some((c, r)) => (c, r),
            None => (line, ""),
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
            "GET" => match store.get(rest.as_bytes())? {
                Some(v) => {
                    w.write_all(format!("${}\n", v.len()).as_bytes())?;
                    w.write_all(&v)?;
                    w.write_all(b"\n")?;
                }
                None => w.write_all(b"nil\n")?,
            },
            "DEL" => {
                let hit = store.delete(rest.as_bytes())?;
                w.write_all(if hit { b"1\n" } else { b"0\n" })?;
            }
            "LEN" => {
                w.write_all(format!("{}\n", store.len()).as_bytes())?;
            }
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
