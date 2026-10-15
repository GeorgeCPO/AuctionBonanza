import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The backend sets no CORS headers, so the browser talks to one origin (this
// dev server) and the server forwards each prefix to the service behind it.
// Proxying avoids touching three servers just to let a browser in.
function proxyTo(target: string, prefix: string) {
  return {
    target,
    changeOrigin: true,
    rewrite: (path: string) => path.slice(prefix.length),
  };
}

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/identity": proxyTo("http://localhost:3001", "/identity"),
      "/auction": proxyTo("http://localhost:3002", "/auction"),
      "/gateway": proxyTo("http://localhost:3003", "/gateway"),
    },
  },
});
