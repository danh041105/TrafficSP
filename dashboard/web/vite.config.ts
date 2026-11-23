import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: `npm run dev` proxy /api sang backend FastAPI (container traffic-dashboard, cổng 8095).
export default defineConfig({
  plugins: [react()],
  server: { port: 5175, proxy: { "/api": "http://localhost:8095" } },
  build: { outDir: "dist", chunkSizeWarningLimit: 1600 },
});
