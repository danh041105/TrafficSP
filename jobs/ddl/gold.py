"""Initialize the empty Gold Iceberg tables using Spark SQL.
The schema lives in gold.sql, beside this entrypoint. This job creates missing
objects only. It does not migrate existing schemas, seed dimensions or load
Silver data. SCD behavior and logical PK/FK checks belong to future load jobs.
"""
from pathlib import Path
from common.spark_session import build_spark

def main() -> None:
    # The checked-in DDL has full-line comments and no semicolons in literals.
    ddl_path = Path(__file__).with_suffix(".sql")
    sql = "\n".join(
        line for line in ddl_path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )
    
    spark = build_spark("ddl_gold")
    try:
        for statement in sql.split(";"):
            if statement.strip():
                spark.sql(statement.strip())
        spark.sql("SHOW TABLES IN lakehouse.gold").show(truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
