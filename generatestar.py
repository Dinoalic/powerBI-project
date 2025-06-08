import psycopg2

conn = psycopg2.connect(
    dbname="neondb",
    user="neondb_owner",
    password="npg_4pwVqHslB0Dx",
    host="ep-calm-sound-a8hjlif2-pooler.eastus2.azure.neon.tech",   # or your DB host
    port="5432"         # default PostgreSQL port
)

cursor = conn.cursor()

def populate_dim_show():
    cursor.execute("""
        INSERT INTO star.dim_show (show_id, title, type, rating, description)
        SELECT s.show_id, s.title, s.type, s.rating, s.description
        FROM public.show s
        WHERE NOT EXISTS (
            SELECT 1 FROM star.dim_show ds WHERE ds.show_id = s.show_id
        )
        ON CONFLICT (show_id) DO NOTHING;
    """)
    conn.commit()

def populate_dim_date():
    cursor.execute("""
        INSERT INTO star.dim_date (full_date, year, month, day, weekday)
        SELECT DISTINCT date_added,
               EXTRACT(YEAR FROM date_added)::INT,
               EXTRACT(MONTH FROM date_added)::INT,
               EXTRACT(DAY FROM date_added)::INT,
               EXTRACT(DOW FROM date_added)::INT
        FROM public.show_date_added sda
        WHERE date_added IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM star.dim_date dd WHERE dd.full_date = sda.date_added
          )
        ON CONFLICT (full_date) DO NOTHING;
    """)
    conn.commit()

def populate_dim_director():
    cursor.execute("""
        INSERT INTO star.dim_director (director_name)
        SELECT DISTINCT director
        FROM public.show_director sd
        WHERE NOT EXISTS (
            SELECT 1 FROM star.dim_director dd WHERE dd.director_name = sd.director
        )
        ON CONFLICT (director_name) DO NOTHING;
    """)
    conn.commit()

def populate_dim_cast():
    cursor.execute("""
        INSERT INTO star.dim_cast (actor_name)
        SELECT DISTINCT actor
        FROM public.show_cast sc
        WHERE NOT EXISTS (
            SELECT 1 FROM star.dim_cast dc WHERE dc.actor_name = sc.actor
        )
        ON CONFLICT (actor_name) DO NOTHING;
    """)
    conn.commit()

def populate_dim_country():
    cursor.execute("""
        INSERT INTO star.dim_country (country_name)
        SELECT DISTINCT country
        FROM public.show_country sco
        WHERE NOT EXISTS (
            SELECT 1 FROM star.dim_country dco WHERE dco.country_name = sco.country
        )
        ON CONFLICT (country_name) DO NOTHING;
    """)
    conn.commit()

def populate_dim_genre():
    cursor.execute("""
        INSERT INTO star.dim_genre (genre_name)
        SELECT DISTINCT genre
        FROM public.show_genre sg
        WHERE NOT EXISTS (
            SELECT 1 FROM star.dim_genre dg WHERE dg.genre_name = sg.genre
        )
        ON CONFLICT (genre_name) DO NOTHING;
    """)
    conn.commit()

def populate_fact_show_and_bridges():
    cursor.execute("""
        INSERT INTO star.fact_show (show_id, dim_show_key, dim_date_key, popularity, vote_average, vote_count, duration_minutes, release_year)
        SELECT s.show_id,
               ds.dim_show_key,
               dd.dim_date_key,
               s.popularity,
               s.vote_average,
               s.vote_count,
               CASE WHEN s.duration ~ '^[0-9]+$' THEN s.duration::INT ELSE NULL END,
               s.release_year
        FROM public.show s
        LEFT JOIN star.dim_show ds ON s.show_id = ds.show_id
        LEFT JOIN public.show_date_added sda ON s.show_id = sda.show_id
        LEFT JOIN star.dim_date dd ON sda.date_added = dd.full_date
        WHERE NOT EXISTS (
            SELECT 1 FROM star.fact_show fs WHERE fs.show_id = s.show_id
        )
        ON CONFLICT (show_id) DO NOTHING;
    """)

    # Insert new fact_show_director bridge rows
    cursor.execute("""
        INSERT INTO star.fact_show_director (fact_show_id, dim_director_key)
        SELECT fs.fact_show_id, dd.dim_director_key
        FROM public.show_director sd
        JOIN star.dim_director dd ON sd.director = dd.director_name
        JOIN star.fact_show fs ON sd.show_id = fs.show_id
        WHERE NOT EXISTS (
            SELECT 1 FROM star.fact_show_director fsd
            WHERE fsd.fact_show_id = fs.fact_show_id AND fsd.dim_director_key = dd.dim_director_key
        )
        ON CONFLICT (fact_show_id, dim_director_key) DO NOTHING;
    """)

    # Insert new fact_show_cast bridge rows
    cursor.execute("""
        INSERT INTO star.fact_show_cast (fact_show_id, dim_cast_key)
        SELECT fs.fact_show_id, dc.dim_cast_key
        FROM public.show_cast sc
        JOIN star.dim_cast dc ON sc.actor = dc.actor_name
        JOIN star.fact_show fs ON sc.show_id = fs.show_id
        WHERE NOT EXISTS (
            SELECT 1 FROM star.fact_show_cast fsc
            WHERE fsc.fact_show_id = fs.fact_show_id AND fsc.dim_cast_key = dc.dim_cast_key
        )
        ON CONFLICT (fact_show_id, dim_cast_key) DO NOTHING;
    """)

    # Insert new fact_show_country bridge rows
    cursor.execute("""
        INSERT INTO star.fact_show_country (fact_show_id, dim_country_key)
        SELECT fs.fact_show_id, dco.dim_country_key
        FROM public.show_country sco
        JOIN star.dim_country dco ON sco.country = dco.country_name
        JOIN star.fact_show fs ON sco.show_id = fs.show_id
        WHERE NOT EXISTS (
            SELECT 1 FROM star.fact_show_country fsc
            WHERE fsc.fact_show_id = fs.fact_show_id AND fsc.dim_country_key = dco.dim_country_key
        )
        ON CONFLICT (fact_show_id, dim_country_key) DO NOTHING;
    """)

    # Insert new fact_show_genre bridge rows
    cursor.execute("""
        INSERT INTO star.fact_show_genre (fact_show_id, dim_genre_key)
        SELECT fs.fact_show_id, dg.dim_genre_key
        FROM public.show_genre sg
        JOIN star.dim_genre dg ON sg.genre = dg.genre_name
        JOIN star.fact_show fs ON sg.show_id = fs.show_id
        WHERE NOT EXISTS (
            SELECT 1 FROM star.fact_show_genre fsg
            WHERE fsg.fact_show_id = fs.fact_show_id AND fsg.dim_genre_key = dg.dim_genre_key
        )
        ON CONFLICT (fact_show_id, dim_genre_key) DO NOTHING;
    """)

    conn.commit()

def main():
    try:
        populate_dim_show()
        populate_dim_date()
        populate_dim_director()
        populate_dim_cast()
        populate_dim_country()
        populate_dim_genre()
        populate_fact_show_and_bridges()
        print("Incremental star schema population completed successfully.")
    except Exception as e:
        print("Error during incremental load:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()