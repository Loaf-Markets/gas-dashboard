#!/usr/bin/env node
// Local preview: serves site/ with data/ mounted at /data, exactly as GitHub Pages lays it out.
//
// Security notes:
//   - Every request path is percent-decoded inside a try/catch, because a malformed
//     escape (e.g. "/%") makes decodeURIComponent throw URIError; an uncaught one
//     would take the whole preview server down (denial of service).
//   - Resolved paths are checked against their root with a SEPARATOR-aware prefix
//     test. A plain `startsWith(root)` is not enough: any sibling directory whose
//     name merely begins with "data" or "site" (e.g. "database/") would pass.
//   - The root is chosen by the URL prefix and the join is then confined to that
//     root, so a request cannot switch from serving data/ to serving site/.
//   - NUL bytes are rejected outright: node:fs throws ERR_INVALID_ARG_VALUE on them.
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const site = path.resolve(root, "site");
const data = path.resolve(process.env.DATA_DIR || path.join(root, "data"));
const port = Number(process.env.PORT || 8123);
const types = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".json": "application/json", ".css": "text/css", ".txt": "text/plain" };

/** True when `target` is `base` itself or sits underneath it on a path-separator boundary. */
const isInside = (base, target) => target === base || target.startsWith(base + path.sep);

/**
 * Map a request URL onto a file path, or null when the request must be refused.
 * Returns null for: undecodable paths, NUL bytes, ".." segments, and anything that
 * resolves outside the root its URL prefix selected.
 *
 * The root is chosen from the RAW path, deliberately BEFORE any normalisation.
 * `new URL("/data/../site/app.js", ...).pathname` is "/site/app.js", which would
 * silently re-root a data request into the site root; the prefix has to be decided
 * from the bytes the client actually sent.
 */
function resolveRequest(rawUrl) {
  // Strip the query/fragment without normalising the path.
  const rawPath = String(rawUrl ?? "/").split("?")[0].split("#")[0];
  let pathname;
  try {
    pathname = decodeURIComponent(rawPath);
  } catch {
    return null; // "/%" -> URIError; must not escape into the server
  }
  if (pathname.includes("\0")) return null; // node:fs throws ERR_INVALID_ARG_VALUE on NUL

  if (pathname === "/data" || pathname === "/data/") return null; // no file named after the mount itself
  if (pathname.startsWith("/data/")) {
    const rel = pathname.slice("/data/".length);
    if (!rel || hasDotDot(rel)) return null;
    const file = path.resolve(data, rel);
    return isInside(data, file) ? file : null;
  }

  const rel = pathname === "/" ? "index.html" : pathname.replace(/^\/+/, "");
  if (!rel || hasDotDot(rel)) return null;
  const file = path.resolve(site, rel);
  return isInside(site, file) ? file : null;
}

/** True when any segment of a relative path is exactly "..". */
const hasDotDot = (rel) => rel.split(/[/\\]/).includes("..");

// Exported for the test suite; the resolver is pure and needs no listening socket.
export { resolveRequest, isInside, site, data };

// Only bind a port when executed directly, so importing this module in tests
// (or another tool) has no side effect.
export const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  http.createServer((req, res) => {
    const file = resolveRequest(req.url || "/");
    if (!file) { res.writeHead(403); return res.end("forbidden"); }
    fs.readFile(file, (err, buf) => {
      if (err) {
        // Only a genuinely missing file is a 404; anything else (permissions, a
        // directory, an EISDIR) is a server-side fault, not a client mistake.
        const code = err.code === "ENOENT" || err.code === "ENOTDIR" ? 404 : 500;
        res.writeHead(code); return res.end(code === 404 ? "not found" : "error");
      }
      res.writeHead(200, { "content-type": types[path.extname(file)] || "application/octet-stream", "cache-control": "no-cache" });
      res.end(buf);
    });
  }).listen(port, "127.0.0.1", () => console.log(`http://127.0.0.1:${port}/  (data from ${data})`));
}
