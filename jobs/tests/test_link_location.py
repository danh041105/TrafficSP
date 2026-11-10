# Chạy: docker exec -it traffic-airflow-scheduler spark-submit /opt/airflow/jobs/tests/test_link_location.py
from pyspark.sql import functions as F
from common.spark_session import build_spark
from batch.transformation.link_location import (clean_link_points, add_median_point,
                                                haversine_km, assign_nearest_cell)

spark = build_spark("test_link_location")

# 1) Haversine: 1 độ vĩ = 6371 * pi/180 ≈ 111.195 km
d = spark.range(1).select(haversine_km(F.lit(40.0), F.lit(-74.0), F.lit(41.0), F.lit(-74.0)).alias("d")).first()["d"]
assert abs(d - 111.195) < 0.01, d
print("HAVERSINE: OK", round(d, 3))

# 2) Trung vị: vĩ độ [40.1, 40.2, 40.9] -> 40.2 ; kinh độ [-73.5, -73.3, -73.1] -> -73.3
links = add_median_point(spark.createDataFrame(
    [(1, "40.1,-73.1 40.9,-73.5 40.2,-73.3")], ["link_id", "link_points"]))
r = links.first()
assert (r["latitude"], r["longitude"]) == (40.2, -73.3), (r["latitude"], r["longitude"])
print("MEDIAN: OK")

# 3) Ô gần nhất: link ở (40.75, -73.92) gần ô B hơn ô A
cells = spark.createDataFrame([("A", 40.60, -74.00), ("B", 40.80, -73.90)],
                              ["cell_id", "latitude", "longitude"])
links = spark.createDataFrame([(1, 40.75, -73.92)], ["link_id", "latitude", "longitude"])
r = assign_nearest_cell(links, cells).first()
assert r["cell_id"] == "B", r
print("NEAREST CELL: OK", r["cell_id"], round(r["cell_distance_km"], 2), "km")

spark.stop()