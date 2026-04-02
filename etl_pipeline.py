"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os
import json
from datetime import datetime


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    # 1. JOIN tables
    df = data_dict["order_items"] \
        .merge(data_dict["orders"], on="order_id") \
        .merge(data_dict["products"], on="product_id") \
        .merge(data_dict["customers"], on="customer_id")

    # 2. Compute line_total
    df["line_total"] = df["quantity"] * df["unit_price"]

    # 3. Filter cancelled orders
    df = df[df["status"].str.lower() != "cancelled"]

    # 4. Filter suspicious quantities
    df = df[df["quantity"] <= 100]

    # 5. Aggregate per customer
    agg = (
        df.groupby(["customer_id", "customer_name", "city"], as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("line_total", "sum"),
        )
    )

    # Correct avg_order_value
    agg["avg_order_value"] = agg["total_revenue"] / agg["total_orders"]

    # Top category per customer
    category_revenue = (
        df.groupby(["customer_id", "category"], as_index=False)["line_total"]
        .sum()
        .sort_values(["customer_id", "line_total"], ascending=[True, False])
    )

    top_category = category_revenue.drop_duplicates(subset=["customer_id"])
    top_category = top_category.rename(columns={"category": "top_category"})[
        ["customer_id", "top_category"]
    ]

    # Final result
    result = agg.merge(top_category, on="customer_id", how="left")

    # --- NEW: Outlier detection ---
    mean_rev = result["total_revenue"].mean()
    std_rev = result["total_revenue"].std()
    result["is_outlier"] = result["total_revenue"] > (mean_rev + 3 * std_rev)

    return result


def validate(df):
    """Run data quality checks on the transformed DataFrame."""
    checks = {
        "no_null_customer_id": df["customer_id"].notna().all(),
        "no_null_customer_name": df["customer_name"].notna().all(),
        "total_revenue_positive": (df["total_revenue"] > 0).all(),
        "no_duplicate_customer_id": not df["customer_id"].duplicated().any(),
        "total_orders_positive": (df["total_orders"] > 0).all(),
    }

    for name, result in checks.items():
        print(f"{name}: {'PASS' if result else 'FAIL'}")

    # --- NEW: print outliers ---
    outliers_count = df["is_outlier"].sum()
    print(f"Outliers detected: {outliers_count}")

    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise ValueError(f"Validation failed: {', '.join(failed)}")

    return checks


def generate_quality_report(df, checks, path="output/quality_report.json"):
    """Generate data quality report JSON."""

    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_records": len(df),
        "checks": {k: bool(v) for k, v in checks.items()},
        "failed_checks": [k for k, v in checks.items() if not v],
        "outliers": df[df["is_outlier"]][
        ["customer_id", "total_revenue"]
        ].to_dict(orient="records"),
    }

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as f:
        json.dump(report, f, indent=4)

    print(f"Quality report saved to {path}")


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file."""
    df.to_sql("customer_summary", engine, if_exists="replace", index=False)

    csv_dir = os.path.dirname(csv_path) or "."
    os.makedirs(csv_dir, exist_ok=True)
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows.")


def main():
    """Orchestrate the ETL pipeline."""
    print("Starting ETL pipeline...")

    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/amman_market",
    )
    engine = create_engine(database_url)

    print("Extracting data...")
    data = extract(engine)

    print("Transforming data...")
    transformed = transform(data)
    print(f"Rows after transform: {len(transformed)}")

    print("Validating data...")
    checks = validate(transformed)

    print("Generating quality report...")
    generate_quality_report(transformed, checks)

    print("Loading data...")
    load(transformed, engine, "output/customer_analytics.csv")

    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()
