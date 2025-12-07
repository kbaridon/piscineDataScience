import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import sys
import os


def connect_db():
    """Create the engine connected to postgreSQL"""
    load_dotenv("../ex01/srcs/.env")
    host = "localhost:5432"  # Hard-coded (For docker)
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB")

    if not all([user, pwd, dbname]):
        raise Exception(".env is wrong!") from None

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}/{dbname}"

    return create_engine(url, pool_pre_ping=True)


def fusion(engine, table1, table2):
    """Add all infos of items table into customers table."""
    print(f"\n📂 Processing {table1}, {table2}")

    query = f"""
    ALTER TABLE {table1} 
    ADD COLUMN IF NOT EXISTS category_id bigint,
    ADD COLUMN IF NOT EXISTS category_code text,
    ADD COLUMN IF NOT EXISTS brand text;
    
    UPDATE {table1} c
    SET 
        category_id = i.category_id,
        category_code = i.category_code,
        brand = i.brand
    FROM {table2} i
    WHERE c.product_id = i.product_id;
    """

    with engine.begin() as conn:
        conn.execute(text(query))


def main():
    """Take a path the a folder full of csv.
    Add the CSV in a customer table in postgreSQL"""

    if len(sys.argv) != 1:
        print("Usage: python3 fusion.py")
        return

    table1 = "customers"
    table2 = "items"
    try:
        engine = connect_db()
    except (KeyboardInterrupt, Exception) as e:
        print("❌ Error:", e)
        return

    try:
        fusion(engine, table1, table2)
        print(f"✅ Fusion has been completed in {table1} and {table2} !")
    except (KeyboardInterrupt, Exception) as e:
        print("❌ Error while fusionning the tables:", e)


if __name__ == "__main__":
    main()
