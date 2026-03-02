"""Minimal HTTP server for the NG911 dashboard."""
import http.server
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
http.server.HTTPServer(("", 8080), http.server.SimpleHTTPRequestHandler).serve_forever()
