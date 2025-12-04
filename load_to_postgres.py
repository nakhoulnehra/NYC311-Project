import psycopg2
import os
import glob
import pandas as pd
import numpy as np

DB_NAME = "nyc311"
DB_USER = "postgres"
DB_PASSWORD = "admin"   
DB_HOST = "localhost"
DB_PORT = "5432"

DATA_FOLDER = r"C:\Users\Admin\Desktop\LAU\Fall 2025\Data Science\Project\clean_311_csv"


def clean_dataframe(df):
    # === Fix boolean columns ===
    bool_cols = ["has_geo", "is_weekend"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({"true": True, "false": False})
            )

    # === Fix numeric columns ===
    numeric_cols = [
        "latitude", "longitude",
        "x_coordinate_state_plane", "y_coordinate_state_plane",
        "response_hours"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # === Drop invalid lat/long rows ===
    df = df.dropna(subset=["latitude", "longitude"])

    # === Remove leftover weird values ===
    df.replace(["True", "False", ""], np.nan, inplace=True)

    return df


def load_csv(path, cursor, conn):
    print(f"\n🔵 Loading: {path}")

    df = pd.read_csv(path)
    df = clean_dataframe(df)

    temp_path = "temp_clean.csv"
    df.to_csv(temp_path, index=False)

    # === COPY into PostgreSQL EXACT column order ===
    with open(temp_path, "r", encoding="utf-8") as f:
        cursor.copy_expert("""
            COPY service_requests (
                unique_key,
                created_date,
                closed_date,
                response_hours,
                status,
                resolution_action_updated_date,
                due_date,
                agency,
                agency_name,
                complaint_type,
                descriptor,
                resolution_description,
                borough,
                city,
                incident_zip,
                latitude,
                longitude,
                x_coordinate_state_plane,
                y_coordinate_state_plane,
                has_geo,
                created_year,
                created_month,
                created_dow,
                created_hour,
                is_weekend
            )
            FROM STDIN
            WITH CSV HEADER
        """, f)

    # === Build the PostGIS geometry ===
    cursor.execute("""
        UPDATE service_requests
        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        WHERE geom IS NULL
          AND longitude IS NOT NULL
          AND latitude IS NOT NULL;
    """)

    conn.commit()
    print(f" Finished: {path}")


def main():
    print("Scanning CSV files...")
    csv_files = glob.glob(os.path.join(DATA_FOLDER, "*.csv"))
    print(f"Found {len(csv_files)} CSV files.")

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    for file in csv_files:
        load_csv(file, cursor, conn)

    cursor.close()
    conn.close()
    print("\n ALL FILES LOADED SUCCESSFULLY ")


if __name__ == "__main__":
    main()
