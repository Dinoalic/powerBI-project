import pandas as pd
import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    dbname="neondb",
    user="neondb_owner",
    password="npg_4pwVqHslB0Dx",
    host="ep-calm-sound-a8hjlif2-pooler.eastus2.azure.neon.tech",   # or your DB host
    port="5432"         # default PostgreSQL port
)

cursor = conn.cursor()
df = pd.read_excel("enriched_data.xlsx")
LAST_LOAD_FILE = "last_load.txt"

def save_current_load_time():
    with open(LAST_LOAD_FILE, "w") as f:
        f.write(datetime.utcnow().isoformat())

save_current_load_time()

# Helper function to safely parse strings to lists
def parse_list(val):
    if pd.isna(val):
        return []
    return [item.strip() for item in str(val).split(",")]


all_inserts = ""
# Iterate through rows
for index, row in df.iterrows():
    show_id = row['show_id']

    time_duration = row['duration'].split()[0]
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
    all_inserts += stmt

    if not pd.isna(row['date_added']):
        stmt = cursor.mogrify(
            "INSERT INTO show_date_added (show_id,date_added) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            (show_id, pd.to_datetime(row['date_added']).date())
        ).decode("utf-8")
        all_inserts += stmt

    for directors in parse_list(row.get('director')):
        all_directors = directors.split(',')
        for director in all_directors:
            stmt = cursor.mogrify(
                "INSERT INTO show_director (show_id,director) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
                (show_id, director)
            ).decode("utf-8")
            all_inserts += stmt

    for actors in parse_list(row.get('cast')):
        all_actors = actors.split(',')
        for actor in all_actors:
            stmt = cursor.mogrify(
                "INSERT INTO show_cast (show_id,actor) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
                (show_id, actor)
            ).decode("utf-8")
        all_inserts += stmt

    for country in parse_list(row.get('country')):
        stmt = cursor.mogrify(
            "INSERT INTO show_country (show_id,country) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            (show_id, country)
        ).decode("utf-8")
        all_inserts += stmt

    for genres in parse_list(row.get('genres')):
        all_genres = genres.split(',')
        for single_genre in all_genres:
            stmt = cursor.mogrify(
                "INSERT INTO show_genre (show_id,genre) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
                (show_id, single_genre)
            ).decode("utf-8")
            all_inserts += stmt

cursor.execute(all_inserts)
conn.commit()
cursor.close()
conn.close()