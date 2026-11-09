from pyspark.sql import SparkSession

def build_spark(app_name, extra_conf=None):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
    )
    # cấu hình key value cho session
    for key, value in (extra_conf or {}).items():
        builder=builder.config(key, value)

    # Khởi tạo SparkSession
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   # tắt bớt log INFO của JVM
    return spark