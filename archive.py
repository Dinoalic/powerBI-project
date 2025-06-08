import psycopg2

conn = psycopg2.connect(
    dbname="neondb",
    user="neondb_owner",
    password="npg_4pwVqHslB0Dx",
    host="ep-calm-sound-a8hjlif2-pooler.eastus2.azure.neon.tech",   # or your DB host
    port="5432"         # default PostgreSQL port
)

cursor = conn.cursor()

def call_archive_procedure():
    try:
        cursor.execute("CALL archive_all_data();")
        conn.commit()
        print("Archive procedure executed successfully.")

    except Exception as e:
        print("Error calling archive procedure:", e)

call_archive_procedure()