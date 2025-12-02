import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm
import glob
import sys
import os

def main():
    load_dotenv("./srcs/.env")
    user = os.getenv('POSTGRES_USER')
    pwd = os.getenv('POSTGRES_PASSWORD')
    dbname = os.getenv('POSTGRES_DB')

    if not all([user, pwd, dbname]):
        print("Error: .env is wrong")
        return

    if len(sys.argv) != 2:
        print("Usage: python3 automatic_table.py folder/")
        return

    folderPath = sys.argv[1]
    
    try:
        db_connection_str = f"postgresql://{user}:{pwd}@localhost:5432/{dbname}" # Host/Port are hard-coded because it's docker.
        db_connection = create_engine(db_connection_str)
    except Exception as e:
        print(f"SQL Error: {e}")
        return

    search_path = os.path.join(folderPath, "data_*.csv")
    files = glob.glob(search_path)
    files.sort()

    if not files:
        print(f"No files found.")
        return

    for file_path in files:
        basename = os.path.basename(file_path)
        table_name = basename.rsplit('.', 1)[0]
        
        print(f"\n📂 Working on {basename} -> Table: {table_name}")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_time timestamptz,
            event_type text,
            product_id integer,
            price decimal(10,2),
            user_id bigint,
            user_session uuid
        );
        """
        
        try:
            with db_connection.connect() as connection:
                connection.execute(text(create_table_query))
                connection.commit()
            print(f"   ↳ Table '{table_name}' exists (has been created or was already here).")
        except Exception as e:
            print(f"❌ Error while creating the table : {e}")
            continue

        try:
            print("   ↳ Reading the CSV...")
            df = pd.read_csv(file_path, na_values=[''])
            
            df['user_session'] = df['user_session'].where(pd.notnull(df['user_session']), None)
            
            chunksize = 10000
            total_rows = len(df)
            
            print(f"   ↳ Adding {total_rows} rows :")
            with tqdm(total=total_rows, unit='rows', ncols=100) as pbar:
                for i in range(0, total_rows, chunksize):
                    chunk = df.iloc[i : i + chunksize]
                    chunk.to_sql(table_name, db_connection, if_exists='append', index=False, method='multi')
                    pbar.update(len(chunk))
            
            print(f"✅ Success for {basename}")
            
        except Exception as e:
            print(f"❌ Error on {basename}: {e}")
            
    print("\nDone !")

if __name__ == "__main__":
    main()