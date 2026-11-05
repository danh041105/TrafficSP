"""Bang ma thoi tiet WMO (weather_code) theo tai lieu Open-Meteo, kem nhom va muc do (quy tac phan tich)."""
# (code, description_en, description_vi, category, precip_type, severity_rank)
WMO = [
    (0, "Clear sky", "Trời quang", "Clear", "none", 0),
    (1, "Mainly clear", "Gần như quang", "Clear", "none", 0),
    (2, "Partly cloudy", "Mây rải rác", "Cloudy", "none", 1),
    (3, "Overcast", "Nhiều mây", "Cloudy", "none", 1),
    (45, "Fog", "Sương mù", "Fog", "none", 3),
    (48, "Depositing rime fog", "Sương mù đóng băng", "Fog", "none", 4),
    (51, "Drizzle: light", "Mưa phùn nhẹ", "Drizzle", "liquid", 2),
    (53, "Drizzle: moderate", "Mưa phùn", "Drizzle", "liquid", 2),
    (55, "Drizzle: dense", "Mưa phùn dày", "Drizzle", "liquid", 3),
    (56, "Freezing drizzle: light", "Mưa phùn đóng băng nhẹ", "Drizzle", "frozen", 5),
    (57, "Freezing drizzle: dense", "Mưa phùn đóng băng dày", "Drizzle", "frozen", 6),
    (61, "Rain: slight", "Mưa nhẹ", "Rain", "liquid", 3),
    (63, "Rain: moderate", "Mưa vừa", "Rain", "liquid", 4),
    (65, "Rain: heavy", "Mưa to", "Rain", "liquid", 5),
    (66, "Freezing rain: light", "Mưa đóng băng nhẹ", "Rain", "frozen", 6),
    (67, "Freezing rain: heavy", "Mưa đóng băng to", "Rain", "frozen", 7),
    (71, "Snow fall: slight", "Tuyết nhẹ", "Snow", "frozen", 5),
    (73, "Snow fall: moderate", "Tuyết vừa", "Snow", "frozen", 6),
    (75, "Snow fall: heavy", "Tuyết dày", "Snow", "frozen", 7),
    (77, "Snow grains", "Hạt tuyết", "Snow", "frozen", 5),
    (80, "Rain showers: slight", "Mưa rào nhẹ", "Showers", "liquid", 3),
    (81, "Rain showers: moderate", "Mưa rào", "Showers", "liquid", 4),
    (82, "Rain showers: violent", "Mưa rào dữ dội", "Showers", "liquid", 6),
    (85, "Snow showers: slight", "Tuyết rào nhẹ", "Snow", "frozen", 6),
    (86, "Snow showers: heavy", "Tuyết rào dày", "Snow", "frozen", 7),
    (95, "Thunderstorm", "Dông", "Thunderstorm", "liquid", 8),
    (96, "Thunderstorm with slight hail", "Dông kèm mưa đá nhỏ", "Thunderstorm", "frozen", 9),
    (99, "Thunderstorm with heavy hail", "Dông kèm mưa đá lớn", "Thunderstorm", "frozen", 9),
]
CATEGORY_VI = {"Clear": "Quang", "Cloudy": "Mây", "Fog": "Sương mù", "Drizzle": "Mưa phùn", "Rain": "Mưa",
               "Showers": "Mưa rào", "Snow": "Tuyết", "Thunderstorm": "Dông"}
MONTH_VI = ["Tháng 1", "Tháng 2", "Tháng 3", "Tháng 4", "Tháng 5", "Tháng 6", "Tháng 7", "Tháng 8", "Tháng 9", "Tháng 10", "Tháng 11", "Tháng 12"]
DAY_VI = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ nhật"]
