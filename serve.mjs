#!/usr/bin/env node
// Local preview: serves site/ with data/ mounted at /data, exactly as GitHub Pages lays it out.
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const site = path.join(root, "site");
const data = process.env.DATA_DIR || path.join(root, "data");
const port = Number(process.env.PORT || 8123);
const types = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".json": "application/json", ".css": "text/css", ".txt": "text/plain" };

http.createServer((req, res) => {
  const url = decodeURIComponent(req.url.split("?")[0]);
  let file = url.startsWith("/data/") ? path.join(data, url.slice(6)) : path.join(site, url === "/" ? "index.html" : url);
  if (!file.startsWith(site) && !file.startsWith(data)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404); return res.end("not found"); }
    res.writeHead(200, { "content-type": types[path.extname(file)] || "application/octet-stream", "cache-control": "no-cache" });
    res.end(buf);
  });
}).listen(port, "127.0.0.1", () => console.log(`http://127.0.0.1:${port}/  (data from ${data})`));
