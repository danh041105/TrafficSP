import { defineConfig } from "@playwright/test";

// Kiểm thử giao diện thật (http://localhost:8095, dữ liệu thật). Dùng Chrome cài sẵn trên máy (channel "chrome"), không tải trình duyệt.
export default defineConfig({
  testDir: "e2e",
  timeout: 120_000,
  expect: { timeout: 60_000 },
  retries: 0,
  reporter: [["list"], ["json", { outputFile: "e2e-results.json" }]],
  use: {
    baseURL: process.env.DASHBOARD_URL ?? "http://localhost:8095",
    channel: "chrome",
    viewport: { width: 1440, height: 900 },
    screenshot: "only-on-failure",
    launchOptions: { args: ["--use-angle=swiftshader", "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist"] },
  },
});
