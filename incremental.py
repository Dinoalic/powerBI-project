import pandas as pd
import psycopg2

# DB connection
conn = psycopg2.connect(
    dbname="neondb",
    user="neondb_owner",
    password="npg_4pwVqHslB0Dx",
    host="ep-calm-sound-a8hjlif2-pooler.eastus2.azure.neon.tech",
    port="5432"
)
cursor = conn.cursor()

# Load the incremental file
df = pd.read_excel("enriched_incremental_data.xlsx")

# Helper to parse comma-separated strings
def parse_list(val):
    if pd.isna(val):
        return []
    return [item.strip() for item in str(val).split(",") if item.strip()]

# Collect all SQL statements
all_statements = ""

for index, row in df.iterrows():
    print(f"Processing row {index}")
    show_id = row['show_id']
    state = str(row.get('state')).strip()

    # DELETE if state == 1
    if state == '1':
        print("Deleting")
        for table in [
            "show_date_added", "show_director", "show_cast",
            "show_country", "show_genre", "show"
        ]:
            stmt = cursor.mogrify(
                f"DELETE FROM {table} WHERE show_id = %s;",
                (show_id,)
            ).decode("utf-8")
            all_statements += stmt
        continue

    # Common duration extraction
    time_duration = str(row['duration']).split()[0] if pd.notna(row['duration']) else None

    # UPDATE if state == 2
    if state == '2':
        print("Updating")
        stmt = cursor.mogrify("""
            UPDATE show SET
                type = %s,
                title = %s,
                release_year = %s,
                rating = %s,
                duration = %s,
                description = %s,
                popularity = %s,
                vote_average = %s,
                vote_count = %s
            WHERE show_id = %s;
        """, (
            row['type'],
            row['title'],
            row['release_year'],
            row['rating'],
            time_duration,
            row['description'],
            row.get('popularity') if pd.notna(row['popularity']) else 0,
            row.get('vote_average') if pd.notna(row['vote_average']) else 0,
            row.get('vote_count') if pd.notna(row['vote_count']) else 0,
            show_id
        )).decode("utf-8")
        all_statements += stmt

    # INSERT if state == 0
    if state == '0':
        print("Inserting")
        stmt = cursor.mogrify("""
            INSERT INTO show (
                show_id, type, title, release_year, rating, duration,
                description, popularity, vote_average, vote_count
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (show_id) DO NOTHING;
        """, (
            show_id,
            row['type'],
            row['title'],
            row['release_year'],
            row['rating'],
            time_duration,
            row['description'],
            row.get('popularity') if pd.notna(row['popularity']) else 0,
            row.get('vote_average') if pd.notna(row['vote_average']) else 0,
            row.get('vote_count') if pd.notna(row['vote_count']) else 0
        )).decode("utf-8")
        all_statements += stmt

    # Related tables (for both INSERT and UPDATE)
    if not pd.isna(row['date_added']):
        stmt = cursor.mogrify(
            "INSERT INTO show_date_added (show_id, date_added) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (show_id, pd.to_datetime(row['date_added']).date())
        ).decode("utf-8")
        all_statements += stmt

    for director in parse_list(row.get('director')):
        stmt = cursor.mogrify(
            "INSERT INTO show_director (show_id, director) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (show_id, director)
        ).decode("utf-8")
        all_statements += stmt

    for actor in parse_list(row.get('cast')):
        stmt = cursor.mogrify(
            "INSERT INTO show_cast (show_id, actor) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (show_id, actor)
        ).decode("utf-8")
        all_statements += stmt

    for country in parse_list(row.get('country')):
        stmt = cursor.mogrify(
            "INSERT INTO show_country (show_id, country) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (show_id, country)
        ).decode("utf-8")
        all_statements += stmt

    for genre in parse_list(row.get('genres')):
        stmt = cursor.mogrify(
            "INSERT INTO show_genre (show_id, genre) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (show_id, genre)
        ).decode("utf-8")
        all_statements += stmt

# Execute all collected SQL statements
cursor.execute(all_statements)
conn.commit()
cursor.close()
conn.close()

print("✅ Incremental load completed.")
