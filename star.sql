CREATE SCHEMA IF NOT EXISTS star;

-- Dimension tables
CREATE TABLE star.dim_show (
    dim_show_key SERIAL PRIMARY KEY,
    show_id TEXT UNIQUE,
    title TEXT,
    type TEXT,
    rating TEXT,
    description TEXT
);

CREATE TABLE star.dim_date (
    dim_date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    weekday INTEGER
);

CREATE TABLE star.dim_director (
    dim_director_key SERIAL PRIMARY KEY,
    director_name TEXT UNIQUE
);

CREATE TABLE star.dim_cast (
    dim_cast_key SERIAL PRIMARY KEY,
    actor_name TEXT UNIQUE
);

CREATE TABLE star.dim_country (
    dim_country_key SERIAL PRIMARY KEY,
    country_name TEXT UNIQUE
);

CREATE TABLE star.dim_genre (
    dim_genre_key SERIAL PRIMARY KEY,
    genre_name TEXT UNIQUE
);

-- Fact table
CREATE TABLE star.fact_show (
    fact_show_id SERIAL PRIMARY KEY,
    show_id TEXT UNIQUE,
    dim_show_key INT REFERENCES star.dim_show(dim_show_key),
    dim_date_key INT REFERENCES star.dim_date(dim_date_key),
    popularity INTEGER,
    vote_average NUMERIC(3,1),
    vote_count INTEGER,
    duration_minutes INTEGER,
    release_year INTEGER
);

-- Bridge tables for many-to-many relationships
CREATE TABLE star.fact_show_director (
    fact_show_id INT REFERENCES star.fact_show(fact_show_id),
    dim_director_key INT REFERENCES star.dim_director(dim_director_key),
    PRIMARY KEY (fact_show_id, dim_director_key)
);

CREATE TABLE star.fact_show_cast (
    fact_show_id INT REFERENCES star.fact_show(fact_show_id),
    dim_cast_key INT REFERENCES star.dim_cast(dim_cast_key),
    PRIMARY KEY (fact_show_id, dim_cast_key)
);

CREATE TABLE star.fact_show_country (
    fact_show_id INT REFERENCES star.fact_show(fact_show_id),
    dim_country_key INT REFERENCES star.dim_country(dim_country_key),
    PRIMARY KEY (fact_show_id, dim_country_key)
);

CREATE TABLE star.fact_show_genre (
    fact_show_id INT REFERENCES star.fact_show(fact_show_id),
    dim_genre_key INT REFERENCES star.dim_genre(dim_genre_key),
    PRIMARY KEY (fact_show_id, dim_genre_key)
);




CREATE OR REPLACE PROCEDURE star.populate_dim_show()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_show (show_id, title, type, rating, description)
    SELECT DISTINCT show_id, title, type, rating, description
    FROM public.show
    ON CONFLICT (show_id) DO NOTHING;
END;
$$;

-- Populate dim_date (using date_added from show_date_added)
CREATE OR REPLACE PROCEDURE star.populate_dim_date()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_date (full_date, year, month, day, weekday)
    SELECT DISTINCT date_added,
           EXTRACT(YEAR FROM date_added)::INT,
           EXTRACT(MONTH FROM date_added)::INT,
           EXTRACT(DAY FROM date_added)::INT,
           EXTRACT(DOW FROM date_added)::INT
    FROM public.show_date_added
    WHERE date_added IS NOT NULL
    ON CONFLICT (full_date) DO NOTHING;
END;
$$;

-- Populate dim_director
CREATE OR REPLACE PROCEDURE star.populate_dim_director()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_director (director_name)
    SELECT DISTINCT director FROM public.show_director
    ON CONFLICT (director_name) DO NOTHING;
END;
$$;

-- Populate dim_cast
CREATE OR REPLACE PROCEDURE star.populate_dim_cast()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_cast (actor_name)
    SELECT DISTINCT actor FROM public.show_cast
    ON CONFLICT (actor_name) DO NOTHING;
END;
$$;

-- Populate dim_country
CREATE OR REPLACE PROCEDURE star.populate_dim_country()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_country (country_name)
    SELECT DISTINCT country FROM public.show_country
    ON CONFLICT (country_name) DO NOTHING;
END;
$$;

-- Populate dim_genre
CREATE OR REPLACE PROCEDURE star.populate_dim_genre()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_genre (genre_name)
    SELECT DISTINCT genre FROM public.show_genre
    ON CONFLICT (genre_name) DO NOTHING;
END;
$$;

Incremental Procedures For Star
-- DIM SHOW
CREATE OR REPLACE PROCEDURE star.populate_dim_show_incremental(last_loaded_at TIMESTAMP)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Insert new shows
    INSERT INTO star.dim_show (show_id, title, type, rating, description)
    SELECT DISTINCT show_id, title, type, rating, description
    FROM public.show
    WHERE updated_at > last_loaded_at
    ON CONFLICT (show_id) DO NOTHING;

    -- Delete related bridge data first
    DELETE FROM star.fact_show_director
    WHERE fact_show_id IN (
        SELECT fs.fact_show_id
        FROM star.fact_show fs
        WHERE fs.show_id NOT IN (SELECT show_id FROM public.show)
    );
    DELETE FROM star.fact_show_cast
    WHERE fact_show_id IN (
        SELECT fs.fact_show_id
        FROM star.fact_show fs
        WHERE fs.show_id NOT IN (SELECT show_id FROM public.show)
    );
    DELETE FROM star.fact_show_country
    WHERE fact_show_id IN (
        SELECT fs.fact_show_id
        FROM star.fact_show fs
        WHERE fs.show_id NOT IN (SELECT show_id FROM public.show)
    );
    DELETE FROM star.fact_show_genre
    WHERE fact_show_id IN (
        SELECT fs.fact_show_id
        FROM star.fact_show fs
        WHERE fs.show_id NOT IN (SELECT show_id FROM public.show)
    );

    -- Delete from fact_show
    DELETE FROM star.fact_show
    WHERE show_id NOT IN (SELECT show_id FROM public.show);

    -- Delete from dim_show
    DELETE FROM star.dim_show
    WHERE show_id NOT IN (SELECT show_id FROM public.show);
END;
$$;

-- DIM DATE
CREATE OR REPLACE PROCEDURE star.populate_dim_date_incremental(last_loaded_at TIMESTAMP)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_date (full_date, year, month, day, weekday)
    SELECT DISTINCT sda.date_added,
           EXTRACT(YEAR FROM sda.date_added)::INT,
           EXTRACT(MONTH FROM sda.date_added)::INT,
           EXTRACT(DAY FROM sda.date_added)::INT,
           EXTRACT(DOW FROM sda.date_added)::INT
    FROM public.show_date_added sda
    JOIN public.show s ON s.show_id = sda.show_id
    WHERE s.updated_at > last_loaded_at
      AND sda.date_added IS NOT NULL
    ON CONFLICT (full_date) DO NOTHING;

    DELETE FROM star.dim_date
    WHERE full_date NOT IN (
        SELECT DISTINCT date_added
        FROM public.show_date_added
        WHERE date_added IS NOT NULL
    );
END;
$$;

-- DIM DIRECTOR
CREATE OR REPLACE PROCEDURE star.populate_dim_director_incremental(last_loaded_at TIMESTAMP)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_director (director_name)
    SELECT DISTINCT sd.director
    FROM public.show_director sd
    JOIN public.show s ON s.show_id = sd.show_id
    WHERE s.updated_at > last_loaded_at
    ON CONFLICT (director_name) DO NOTHING;

    DELETE FROM star.fact_show_director
    WHERE dim_director_key IN (
        SELECT dim_director_key
        FROM star.dim_director d
        WHERE d.director_name NOT IN (SELECT DISTINCT director FROM public.show_director)
    );

    DELETE FROM star.dim_director
    WHERE director_name NOT IN (SELECT DISTINCT director FROM public.show_director);
END;
$$;

-- DIM CAST
CREATE OR REPLACE PROCEDURE star.populate_dim_cast_incremental(last_loaded_at TIMESTAMP)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_cast (actor_name)
    SELECT DISTINCT sc.actor
    FROM public.show_cast sc
    JOIN public.show s ON s.show_id = sc.show_id
    WHERE s.updated_at > last_loaded_at
    ON CONFLICT (actor_name) DO NOTHING;

    DELETE FROM star.fact_show_cast
    WHERE dim_cast_key IN (
        SELECT dim_cast_key
        FROM star.dim_cast d
        WHERE d.actor_name NOT IN (SELECT DISTINCT actor FROM public.show_cast)
    );

    DELETE FROM star.dim_cast
    WHERE actor_name NOT IN (SELECT DISTINCT actor FROM public.show_cast);
END;
$$;

-- DIM COUNTRY
CREATE OR REPLACE PROCEDURE star.populate_dim_country_incremental(last_loaded_at TIMESTAMP)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_country (country_name)
    SELECT DISTINCT sc.country
    FROM public.show_country sc
    JOIN public.show s ON s.show_id = sc.show_id
    WHERE s.updated_at > last_loaded_at
    ON CONFLICT (country_name) DO NOTHING;

    DELETE FROM star.fact_show_country
    WHERE dim_country_key IN (
        SELECT dim_country_key
        FROM star.dim_country d
        WHERE d.country_name NOT IN (SELECT DISTINCT country FROM public.show_country)
    );

    DELETE FROM star.dim_country
    WHERE country_name NOT IN (SELECT DISTINCT country FROM public.show_country);
END;
$$;

-- DIM GENRE
CREATE OR REPLACE PROCEDURE star.populate_dim_genre_incremental(last_loaded_at TIMESTAMP)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.dim_genre (genre_name)
    SELECT DISTINCT sg.genre
    FROM public.show_genre sg
    JOIN public.show s ON s.show_id = sg.show_id
    WHERE s.updated_at > last_loaded_at
    ON CONFLICT (genre_name) DO NOTHING;

    DELETE FROM star.fact_show_genre
    WHERE dim_genre_key IN (
        SELECT dim_genre_key
        FROM star.dim_genre d
        WHERE d.genre_name NOT IN (SELECT DISTINCT genre FROM public.show_genre)
    );

    DELETE FROM star.dim_genre
    WHERE genre_name NOT IN (SELECT DISTINCT genre FROM public.show_genre);
END;
$$;

-- FACT SHOW
CREATE OR REPLACE PROCEDURE star.populate_fact_show_incremental(last_loaded_at TIMESTAMP)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO star.fact_show (
        show_id,
        dim_show_key,
        dim_date_key,
        release_year,
        duration_minutes,
        popularity,
        vote_average,
        vote_count
    )
    SELECT
        s.show_id,
        ds.dim_show_key,
        dd.dim_date_key,
        s.release_year,
        s.duration::INT,
        s.popularity,
        s.vote_average,
        s.vote_count
    FROM public.show s
    LEFT JOIN star.dim_show ds ON s.show_id = ds.show_id
    LEFT JOIN public.show_date_added sda ON s.show_id = sda.show_id
    LEFT JOIN star.dim_date dd ON sda.date_added = dd.full_date
    WHERE s.updated_at > last_loaded_at
    ON CONFLICT (show_id) DO NOTHING;
END;
$$;

-- MASTER PROCEDURE
CREATE OR REPLACE PROCEDURE star.populate_all_dims_and_fact_incremental(last_loaded_at TIMESTAMP)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL star.populate_dim_show_incremental(last_loaded_at);
    CALL star.populate_dim_date_incremental(last_loaded_at);
    CALL star.populate_dim_director_incremental(last_loaded_at);
    CALL star.populate_dim_cast_incremental(last_loaded_at);
    CALL star.populate_dim_country_incremental(last_loaded_at);
    CALL star.populate_dim_genre_incremental(last_loaded_at);
    CALL star.populate_fact_show_incremental(last_loaded_at);
END;
$$;