"""
ETL Framework — Dynamic Config-Driven 
"""

from sqlalchemy import create_engine, text
import pandas as pd
import os
import json
import logging
from datetime import datetime

# =========================
# 🔹 LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market",
)

# =========================
# 🔹 METADATA
# =========================
def get_last_run_timestamp(engine):
    query = """
        SELECT MAX(end_time) AS last_run
        FROM etl_metadata
        WHERE status = 'success'
    """
    result = pd.read_sql(query, engine)
    return result["last_run"].iloc[0] if result["last_run"].iloc[0] else None


def log_etl_run(engine, start_time, end_time, rows, status):
    query = text("""
        INSERT INTO etl_metadata (start_time, end_time, rows_processed, status)
        VALUES (:start_time, :end_time, :rows, :status)
    """)

    with engine.begin() as conn:
        conn.execute(query, {
            "start_time": start_time,
            "end_time": end_time,
            "rows": rows,
            "status": status,
        })


# =========================
# 🔹 EXTRACT
# =========================
def extract(engine, config, last_run=None):
    data = {}

    for name, table in config["source_tables"].items():
        if name == "orders" and last_run:
            query = f"""
                SELECT * FROM {table}
                WHERE order_date > '{last_run}'
            """
            logging.info(f"Incremental load since {last_run}")
        else:
            query = f"SELECT * FROM {table}"

        df = pd.read_sql(query, engine)
        logging.info(f"{name}: {len(df)} rows")
        data[name] = df

    return data


# =========================
# 🔹 DYNAMIC TRANSFORM 
# =========================
def transform(data, config):
    logging.info("Transforming data (dynamic)...")

    # start with first table
    base_table = config["joins"][0][0]
    df = data[base_table]

    # joins
    for left, right, key in config["joins"]:
        df = df.merge(data[right], on=key)

    # calculations
    for calc in config.get("calculations", []):
        df[calc["new_column"]] = df.eval(calc["formula"])

    # filters
    for f in config.get("filters", []):
        if f["op"] == "!=":
            df = df[df[f["column"]] != f["value"]]
        elif f["op"] == "<=":
            df = df[df[f["column"]] <= f["value"]]
        elif f["op"] == ">":
            df = df[df[f["column"]] > f["value"]]

    # aggregation
    agg_dict = {
        new_col: (col, func)
        for new_col, (col, func) in config["aggregations"].items()
    }

    result = df.groupby(config["groupby"], as_index=False).agg(**agg_dict)

    # optional calculations after aggregation
    if "post_calculations" in config:
        for calc in config["post_calculations"]:
            result[calc["new_column"]] = result.eval(calc["formula"])

    # optional top category logic
    if config.get("top_category"):
        cat_col = config["top_category"]["category_col"]
        val_col = config["top_category"]["value_col"]

        temp = (
            df.groupby(config["groupby"] + [cat_col])[val_col]
            .sum()
            .reset_index()
            .sort_values(val_col, ascending=False)
        )

        top = temp.drop_duplicates(subset=config["groupby"])
        top = top[config["groupby"] + [cat_col]]

        result = result.merge(top, on=config["groupby"], how="left")

    # outliers (optional)
    if config.get("outliers"):
        col = config["outliers"]["column"]
        mean = result[col].mean()
        std = result[col].std()

        result["is_outlier"] = result[col] > (mean + 3 * std)

    logging.info(f"Rows after transform: {len(result)}")

    return result


# =========================
# 🔹 VALIDATE
# =========================
def validate(df):
    checks = {
        "no_nulls": not df.isnull().any().any(),
        "no_duplicates": not df.duplicated().any(),
    }

    for k, v in checks.items():
        logging.info(f"{k}: {'PASS' if v else 'FAIL'}")

    if not all(checks.values()):
        raise ValueError("Validation failed")

    return checks


# =========================
# 🔹 REPORT
# =========================
def generate_quality_report(df, checks, path):
    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "rows": len(df),
        "checks": checks
    }

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as f:
        json.dump(report, f, indent=4)

    logging.info(f"Report saved to {path}")


# =========================
# 🔹 LOAD
# =========================
def load(df, engine, config):
    df.to_sql(config["output_table"], engine, if_exists="replace", index=False)

    os.makedirs(os.path.dirname(config["output_csv"]), exist_ok=True)
    df.to_csv(config["output_csv"], index=False)

    logging.info(f"Loaded {len(df)} rows")


# =========================
# 🔹 RUN
# =========================
def run_pipeline(config_path):
    logging.info(f"Running pipeline: {config_path}")

    with open(config_path) as f:
        config = json.load(f)

    engine = create_engine(DATABASE_URL)
    start = datetime.utcnow()

    try:
        last_run = get_last_run_timestamp(engine)

        data = extract(engine, config, last_run)
        df = transform(data, config)
        checks = validate(df)

        generate_quality_report(df, checks, config["quality_report"])
        load(df, engine, config)

        log_etl_run(engine, start, datetime.utcnow(), len(df), "success")

        logging.info("Pipeline SUCCESS")

    except Exception as e:
        log_etl_run(engine, start, datetime.utcnow(), 0, "failed")
        logging.error(str(e))
        raise


if __name__ == "__main__":
    run_pipeline("config/customer_pipeline.json")