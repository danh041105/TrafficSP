import { expect, test, type Page } from "@playwright/test";

// Lỗi console thật của trang (bỏ qua cảnh báo style của nền bản đồ OpenFreeMap).
function watchConsole(page: Page) {
  const errors: string[] = [];
  page.on("console", (m) => { if (m.type() === "error" && !/tiles\.openfreemap|Failed to load resource.*(png|pbf|json)$/.test(m.text())) errors.push(m.text()); });
  page.on("pageerror", (e) => errors.push(String(e)));
  return errors;
}

test.describe("Trung tâm vận hành", () => {
  test("làn chính: KPI, timeline, sơ đồ, số đo, handoff hiển thị từ dữ liệu thật", async ({ page }) => {
    const errors = watchConsole(page);
    await page.goto("/#/ops?lane=main");
    await expect(page.getByRole("heading", { name: "Trung tâm vận hành" })).toBeVisible();
    const kpis = page.locator(".ops-kpis [role=listitem]");
    await expect(kpis).toHaveCount(6);
    await expect(kpis.first()).not.toContainText("—", { timeout: 60_000 });          // batch_state có giá trị thật
    await expect(page.getByRole("img", { name: /Cửa sổ 5 phút theo giờ sự kiện/ })).toBeVisible();
    await expect(page.getByRole("img", { name: /batch_state đã công bố và watermark/ })).toBeVisible();
    const nodes = page.locator("g.flow-node");
    await expect(nodes).toHaveCount(11);
    await nodes.filter({ hasText: "Lambda handoff" }).click();                        // bấm node -> bảng chi tiết
    await expect(page.getByRole("region", { name: /Chi tiết Lambda handoff/ })).toBeVisible();
    await expect(page.locator(".metric-tile")).toHaveCount(9);
    await expect(page.locator(".handoff-layout table.data tbody tr").first()).toBeVisible();
    await page.getByRole("button", { name: "Xem dạng bảng" }).click();                 // giá trị đọc được không qua tooltip
    await expect(page.locator("table.data").filter({ hasText: "Batch đã công bố" }).first()).toBeVisible();
    expect(errors, errors.join("\n")).toEqual([]);
  });

  test("làn phát lại: phiên được ghi rõ là dữ liệu lịch sử, không phải trực tiếp", async ({ page }) => {
    const errors = watchConsole(page);
    await page.goto("/#/ops?lane=demo");
    await expect(page.getByText("Đây không phải giao thông New York trực tiếp", { exact: false })).toBeVisible();
    await expect(page.locator(".replay-state .mono-id")).toContainText("demo");
    await expect(page.getByRole("progressbar")).toBeVisible();
    expect(errors, errors.join("\n")).toEqual([]);
  });
});

test.describe("Grafana nhúng", () => {
  test("dashboard Kafka và Spark của Grafana hiện trong trang Vận hành, Kafka lọc theo topic của làn", async ({ page }) => {
    await page.goto("/#/ops?lane=main");
    const sec = page.locator("section", { has: page.getByRole("heading", { name: "Grafana: Kafka và Spark" }) });
    await sec.scrollIntoViewIfNeeded();
    const frame = page.frameLocator("iframe.grafana-frame");
    await expect(page.locator("iframe.grafana-frame")).toHaveAttribute("src", /var-topic=traffic_raw(&|$)/);
    await expect(frame.locator("body")).toContainText("Brokers", { timeout: 60_000 });
    await sec.getByRole("button", { name: "Spark - Traffic" }).click();
    await page.locator("iframe.grafana-frame").scrollIntoViewIfNeeded();   // Grafana chỉ vẽ panel khi khung nằm trong vùng nhìn
    await expect(frame.locator("body")).toContainText("Worker sống", { timeout: 60_000 });
  });
});

test.describe("Các trang nghiệp vụ", () => {
  for (const [path, heading] of [["overview", "Tổng quan mạng lưới"], ["map", "Bản đồ đoạn đường"], ["forecast", "Dự báo tốc độ sau 15 phút"],
    ["model", "Đánh giá mô hình dự báo"]] as const) {
    test(`trang ${path} tải không lỗi`, async ({ page }) => {
      const errors = watchConsole(page);
      await page.goto(`/#/${path}?lane=main`);
      await expect(page.getByRole("heading", { name: heading })).toBeVisible();
      await expect(page.locator(".error")).toHaveCount(0, { timeout: 60_000 });
      expect(errors, errors.join("\n")).toEqual([]);
    });
  }
});

test.describe("Hợp đồng API", () => {
  test("timeline: mỗi cửa sổ có trạng thái lấy từ dữ liệu, không tự điền 0", async ({ request }) => {
    const r = await request.get("/api/ops/timeline?lane=main");
    expect(r.ok()).toBeTruthy();
    const d = await r.json();
    expect(d.windows.length).toBeGreaterThan(10);
    for (const w of d.windows) {
      for (const k of ["batch", "speed", "serving_batch", "serving_speed"]) expect(w[k] === null || w[k] > 0).toBeTruthy();
    }
  });
  test("số đo không có được trả null kèm lý do", async ({ request }) => {
    const d = await (await request.get("/api/ops/metrics?lane=main")).json();
    expect(d.not_available.kafka_consumer_lag).toContain("consumer group");
    const flow = await (await request.get("/api/ops/flow?lane=main")).json();
    expect(Object.keys(flow.nodes)).toHaveLength(11);
    for (const n of Object.values<any>(flow.nodes)) expect(["ok", "degraded", "error", "unknown"]).toContain(n.status);
  });
  test("lỗi rõ ràng khi chọn làn không hợp lệ", async ({ request }) => {
    const r = await request.get("/api/overview?lane=khong-co");
    expect(r.status()).toBe(400);
  });
});
