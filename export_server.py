"""Local helper for exporting dashboard charts as PNG: serves docs/ on :8766 and saves POST /save into exhibits/<date>/.
Used with the in-page export snippet (see exhibits/README.md). Not part of the dashboard itself.
"""
"""Serve the dashboard and accept POST /save {name, data(base64 PNG)} -> exhibits folder."""
import base64, json, os, sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
DOCS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exhibits", __import__("datetime").date.today().strftime("%Y%m%d"))
class H(SimpleHTTPRequestHandler):
    def __init__(self, *a, **k): super().__init__(*a, directory=DOCS, **k)
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0)); body = json.loads(self.rfile.read(n))
        name = os.path.basename(body["name"]); data = body["data"].split(",", 1)[1]
        os.makedirs(OUT, exist_ok=True)
        with open(os.path.join(OUT, name), "wb") as f: f.write(base64.b64decode(data))
        self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
        self.wfile.write(json.dumps({"saved": name, "bytes": len(data) * 3 // 4}).encode())
    def log_message(self, *a): pass
ThreadingHTTPServer(("127.0.0.1", 8766), H).serve_forever()
