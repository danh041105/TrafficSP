from pyspark.sql import DataFrame, Column, Window, functions as F

# Định dạng của tọa độ lat, long hợp lệ:
# ví dụ: 40.75719,-73.99724
POINT_PATTERNS = r"^-?\d+\.\d+,-?\d+\.\d+$"
LINK_POINT_MAX_LEN = 255

# bán kính của trái đất: sử dụng để tính khoảng cách haversine cho lat và lon
EARTH_RADIUS_KM = 6371.0

def clean_link_points(df: DataFrame) -> DataFrame:
    # Chuẩn hóa chuỗi
    link_points = F.trim(F.col("link_points"))
    # Tách chuỗi thành danh sách các tọa độ
    points = F.split(link_points, r"\s+")
    # nếu độ dài của link_points >= 255 thì sẽ cắt cặp tọa độ cuối cùng 
    points = F.when(F.length("link_points") >= LINK_POINT_MAX_LEN,
                    F.slice(points, 1, F.size(points) - 1)
    ).otherwise(points)
    # Sau đó chỉ giữ các tọa độ đúng định dạng
    points = F.filter(points, lambda p: p.rlike(POINT_PATTERNS))
    new_df = df.withColumn("link_points",
                            F.when(F.size(points) > 0, F.array_join(points, " ")))
    return new_df

# tính điểm đại diện cho link_points: lấy giá trị trung vị
def add_median_point(df: DataFrame) -> DataFrame:
    link_points = F.trim(F.col("link_points"))
    # Tách chuỗi thành danh sách các tọa độ
    points = F.split(link_points, r"\s+")
    # lấy vĩ độ trong points
    lats = F.array_sort(F.transform(points, lambda p: F.split(p, ",")[0].cast("double")))
    lons = F.array_sort(F.transform(points, lambda p: F.split(p, ",")[1].cast("double")))
    # index giữa danh sách các point
    mid = (F.floor(F.size(points) / 2) + 1).cast("int")
    new_df = (df
            .withColumn("latitude", F.element_at(lats, mid))        
            .withColumn("longitude", F.element_at(lons, mid))
    )
    return new_df

# Khoảng cách haversine
# công thức: 
# a = sin^2((lat2 - lat1)/2) + cos(lat1) * cos(lat2) * sin^2((lon2 - lon1)/2)
# c = 2 * arcsin(căn a)
# d = R * c (R: bán kính trái đất)
# Trong đó lat và lon phải đổi từ độ sang radian
def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    # kết quả trả về sẽ khoảng khoảng cách giữa 2 điểm có đơn vị là km
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = F.sin(dlat/2) ** 2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon/2) ** 2
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))

def assign_nearest_cell(links: DataFrame, cells: DataFrame) -> DataFrame:
    # links: latitude, longitude
    # cells: cell_id, latitude, longitude
    cells = cells.select("cell_id",
            F.col("latitude").alias("cell_lat"),
            F.col("longitude").alias("cell_lon")
        )
    
    df = links.crossJoin(F.broadcast(cells)) # ghép link với 16 ô thời tiết
    # Tính khoảng cách từ link đến từng ô thời tiết
    df = df.withColumn(
        "cell_distance_km",
        haversine_km(
            F.col("latitude"),
            F.col("longitude"),
            F.col("cell_lat"),
            F.col("cell_lon")
        )
    )
    # tìm ra ô thời tiết gần nhất với link
    window = Window.partitionBy("link_id").orderBy("cell_distance_km")
    new_df = (df
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "cell_lat", "cell_lon")
    )
    return new_df