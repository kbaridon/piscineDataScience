import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm
import glob
import sys
import os
import io


def connect_db():
    """Create the engine connected to postgreSQL"""
    load_dotenv("./srcs/.env")
    host = "localhost:5432"  # Hard-coded (For docker)
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB")

    if not all([user, pwd, dbname]):
        raise Exception(".env is wrong!") from None

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}/{dbname}"

    return create_engine(url, pool_pre_ping=True)


def create_table(engine):
    """Take the connection to postgreSQL.
    Create an SQL Query and execute it"""
    query = """
    CREATE TABLE IF NOT EXISTS customers (
        event_time timestamptz,
        event_type text,
        product_id integer,
        price decimal(10,2),
        user_id bigint,
        user_session uuid
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))
        conn.execute(text("TRUNCATE customers;"))  # Reset table


def copy_chunk_to_db(conn, df):
    """Take a connection to postgreSQL and a DataFrame.
    Copy all the DataFrame to """
    output = io.StringIO()
    df.to_csv(output, index=False, header=False)
    output.seek(0)

    conn.connection.cursor().copy_expert(
        "COPY customers FROM STDIN WITH CSV", output)


def process_csv(engine, file_path):
    """Take each csv file, and send it to copy_chunk with pandas."""
    print(f"\n📂 Processing {os.path.basename(file_path)}")

    chunksize = 25000
    total_rows = sum(1 for _ in open(file_path)) - 1  # (header)

    with engine.begin() as conn:
        with tqdm(total=total_rows, unit="rows", dynamic_ncols=True) as pbar:
            for chunk in pd.read_csv(file_path, chunksize=chunksize):
                chunk['user_session'] = chunk['user_session'].where(
                    pd.notnull(chunk['user_session']), None)
                copy_chunk_to_db(conn, chunk)
                pbar.update(len(chunk))


def main():
    """Take a path the a folder full of csv.
    Add the CSV in a customer table in postgreSQL"""

    if len(sys.argv) != 2:
        print("Usage: python3 customers_table.py folder/")
        return

    folder = sys.argv[1]
    try:
        engine = connect_db()
        create_table(engine)
    except (KeyboardInterrupt, Exception) as e:
        print("❌ Error:", e)
        return

    files = sorted(glob.glob(os.path.join(folder, "data_202*_*.csv")))
    if not files:
        print("❌ No CSV files found")
        return

    print(f"\n--> Found {len(files)} CSV files")

    for f in files:
        try:
            process_csv(engine, f)
            print(f"✅ Success for {os.path.basename(f)} !")
        except (KeyboardInterrupt, Exception):
            print("❌ Error while processing CSV files.")

    print("\n🎉 Success !")


if __name__ == "__main__":
    main()
