//! An MCP server over stdio so an agent can use Flint as durable scratch memory:
//! set, get, delete, and count keys that survive across sessions.

use crate::Store;
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::sync::Arc;

pub fn serve_mcp(store: Arc<Store>) -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");

        let resp = match method {
            "initialize" => json!({
                "jsonrpc": "2.0", "id": id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "flint", "version": env!("CARGO_PKG_VERSION") }
                }
            }),
            "tools/list" => json!({
                "jsonrpc": "2.0", "id": id,
                "result": { "tools": tool_specs() }
            }),
            "tools/call" => match call_tool(&store, &req) {
                Ok(text) => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": { "content": [{ "type": "text", "text": text }] }
                }),
                Err(msg) => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": { "content": [{ "type": "text", "text": msg }], "isError": true }
                }),
            },
            "notifications/initialized" | "" => continue,
            _ => json!({
                "jsonrpc": "2.0", "id": id,
                "error": { "code": -32601, "message": "method not found" }
            }),
        };
        stdout.write_all(serde_json::to_string(&resp).unwrap().as_bytes())?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}

fn call_tool(store: &Store, req: &Value) -> Result<String, String> {
    let params = req.get("params").ok_or("missing params")?;
    let name = params.get("name").and_then(|n| n.as_str()).ok_or("missing tool name")?;
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    let key = || args.get("key").and_then(|k| k.as_str()).map(|s| s.to_string());

    match name {
        "flint_set" => {
            let k = key().ok_or("flint_set needs a key")?;
            let v = args.get("value").and_then(|v| v.as_str()).ok_or("flint_set needs a value")?;
            store.set(k.as_bytes(), v.as_bytes()).map_err(|e| e.to_string())?;
            store.flush().map_err(|e| e.to_string())?;
            Ok(format!("set {k}"))
        }
        "flint_get" => {
            let k = key().ok_or("flint_get needs a key")?;
            match store.get(k.as_bytes()).map_err(|e| e.to_string())? {
                Some(v) => Ok(String::from_utf8_lossy(&v).into_owned()),
                None => Ok("(nil)".into()),
            }
        }
        "flint_delete" => {
            let k = key().ok_or("flint_delete needs a key")?;
            let hit = store.delete(k.as_bytes()).map_err(|e| e.to_string())?;
            store.flush().map_err(|e| e.to_string())?;
            Ok(if hit { format!("deleted {k}") } else { format!("{k} not found") })
        }
        "flint_len" => Ok(store.len().to_string()),
        other => Err(format!("unknown tool '{other}'")),
    }
}

fn tool_specs() -> Value {
    json!([
        { "name": "flint_set", "description": "Store a value under a key in Flint (persists across sessions).",
          "inputSchema": { "type": "object", "required": ["key", "value"],
            "properties": { "key": { "type": "string" }, "value": { "type": "string" } } } },
        { "name": "flint_get", "description": "Read the value stored under a key. Returns (nil) if absent.",
          "inputSchema": { "type": "object", "required": ["key"],
            "properties": { "key": { "type": "string" } } } },
        { "name": "flint_delete", "description": "Delete a key from Flint.",
          "inputSchema": { "type": "object", "required": ["key"],
            "properties": { "key": { "type": "string" } } } },
        { "name": "flint_len", "description": "Return the number of live keys stored in Flint.",
          "inputSchema": { "type": "object", "properties": {} } }
    ])
}
