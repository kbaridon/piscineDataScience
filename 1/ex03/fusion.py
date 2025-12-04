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


def remove_duplicates(engine, table):
    """Take a table in postgreSQL and remove all the duplicates"""
    print(f"\n📂 Processing {table}")

    query = f"""
    """
    with engine.begin() as conn:
        conn.execute(text(query))


def main():
    """Take a path the a folder full of csv.
    Add the CSV in a customer table in postgreSQL"""

    if len(sys.argv) != 2:
        print("Usage: python3 fusion.py table")
        return

    table = sys.argv[1]
    try:
        engine = connect_db()
    except (KeyboardInterrupt, Exception) as e:
        print("❌ Error:", e)
        return

    try:
        fusion(engine, table)
        print(f"✅ Fusion has been completed in {table} !")
    except (KeyboardInterrupt, Exception) as e:
        print("❌ Error while fusionning the tables:", e)


if __name__ == "__main__":
    main()
