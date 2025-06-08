import psycopg2
from datetime import datetime

# Config
conn = psycopg2.connect(
    dbname="neondb",
    user="neondb_owner",
    password="npg_4pwVqHslB0Dx",
    host="ep-calm-sound-a8hjlif2-pooler.eastus2.azure.neon.tech",
    port="5432"
)
cursor = conn.cursor()
LAST_LOAD_FILE = "last_load.txt"

# Load last timestamp
def read_last_load_time():
    try:
        with open(LAST_LOAD_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        # Default to a very old date
        return "2000-01-01T00:00:00"

# Save current timestamp to file
def save_current_load_time():
    with open(LAST_LOAD_FILE, "w") as f:
        f.write(datetime.utcnow().isoformat())

# Run procedure
def run_incremental_load():
    last_loaded_at = read_last_load_time()

    try:
        print(f"Calling procedure with last_loaded_at: {last_loaded_at}")
        cursor.execute("CALL star.populate_all_dims_and_fact_incremental(%s);", (last_loaded_at,))
        conn.commit()
        save_current_load_time()
        print("Procedure executed and timestamp updated.")
    except Exception as e:
        conn.rollback()
        print("Error running procedure:", e)
    finally:
        cursor.close()
        conn.close()

# Run it
run_incremental_load()
