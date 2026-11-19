# file này sẽ có các hàm hỗ trợ đọc các dữ liệu từ 2 nguồn weather và traffic 
# và xác định giai đoạn(1 - 9 hoặc 10 - 12)
import json
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from common import config
from common.schemas import TRAFFIC_FIELDS, WEATHER_FIELDS, field_names

PHASES = {
    "test": {"months": [1], "traffic_day": "2025-01-01", "realtime": False},
    "gd1": {"months": list(config.HISTORY_MONTHS), "traffic_day": None, "realtime": False},
    "gd2": {"months": list(config.REPLAY_MONTHS), "traffic_day": None, "realtime": True},
}
# Các trường không nằm trong mảng "hourly" của file JSON; producer tự điền.
WEATHER_METADATA = ("cell_id", "latitude", "longitude", "time", "run_id")
WEATHER_VARS = [f for f in field_names(WEATHER_FIELDS) if f not in WEATHER_METADATA]

# traffic 
TRAFFIC_COLUMNS = [f for f in field_names(TRAFFIC_FIELDS) if f != "run_id"]
BATCH_ROWS = 50_000 # chia ra từng batch để tránh tràn RAM

def weather_in_phase(phase: str, t: str) -> bool:
    # t là chuỗi giờ UTC dạng "2025-01-01T00:00" 
    if phase == "test":
        return t.startswith("2025-01-01")
    if phase == "gd1":
        return t <= config.WEATHER_PHASE_BOUNDARY_UTC 
    return t > config.WEATHER_PHASE_BOUNDARY_UTC   # gd2

def read_weather(phase_name: str, run_id: str) -> list[dict]:
    #Đọc 16 file ifs_<lat>_<lon>.json, làm phẳng mảng hourly: 1 message = 1 ô × 1 giờ.
    files = sorted(config.RAW_WEATHER_DIR.glob("ifs_*.json"))
    if not files:
        raise FileNotFoundError(f"Không thấy file weather nào trong {config.RAW_WEATHER_DIR}")

    messages = []
    for path in files:
        cell_id = path.stem.removeprefix("ifs_")      # "ifs_40.5272_-74.2373" -> "40.5272_-74.2373"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        hourly = data["hourly"]

        # Các mảng trong hourly song song nhau: phần tử thứ i của mọi mảng cùng thuộc giờ hourly["time"][i]
        for i, t in enumerate(hourly["time"]):
            if not weather_in_phase(phase_name, t):
                continue
            msg = {
                "cell_id": cell_id,
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "time": t,
            }
            for var in WEATHER_VARS:
                msg[var] = hourly[var][i]
            msg["run_id"] = run_id
            messages.append(msg)

    # Sắp theo giờ rồi theo ô: GĐ2 cần phát đúng thứ tự thời gian
    messages.sort(key=lambda m: (m["time"], m["cell_id"]))
    return messages

def iter_traffic(phase_name: str, run_id: str):
    # Đọc từng file 2025-MM.parquet theo lô (không nạp cả tháng vào RAM), giữ thứ tự data_as_of của file.
    phase = PHASES[phase_name]
    for month in phase["months"]:
        path = config.RAW_TRAFFIC_DIR / f"2025-{month:02d}.parquet"
        for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS, columns=TRAFFIC_COLUMNS):
            table = pa.Table.from_batches([batch])
            # data_as_of: timestamp[ns] giờ NY -> "2025-01-01T00:03:03".
            # Không dùng pc.strftime vì trên Windows nó đòi tzdata dù cột không có múi giờ.
            as_of = pc.replace_substring(
                pc.cast(pc.cast(table["data_as_of"], pa.timestamp("s")), pa.string()), " ", "T")
            table = table.set_column(table.schema.get_field_index("data_as_of"), "data_as_of", as_of)
            if phase["traffic_day"]:   # phase test: chỉ lấy 1 ngày
                table = table.filter(pc.starts_with(table["data_as_of"], phase["traffic_day"]))
            table = table.append_column("run_id", pa.array([run_id] * table.num_rows, pa.string()))
            yield from table.to_pylist()