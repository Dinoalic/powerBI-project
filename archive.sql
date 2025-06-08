CREATE SCHEMA IF NOT EXISTS archive;

-- Create archive tables
CREATE TABLE IF NOT EXISTS archive.archive_show (LIKE public.show INCLUDING ALL);
CREATE TABLE IF NOT EXISTS archive.archive_show_date_added (LIKE public.show_date_added INCLUDING ALL);
CREATE TABLE IF NOT EXISTS archive.archive_show_director (LIKE public.show_director INCLUDING ALL);
CREATE TABLE IF NOT EXISTS archive.archive_show_cast (LIKE public.show_cast INCLUDING ALL);
CREATE TABLE IF NOT EXISTS archive.archive_show_country (LIKE public.show_country INCLUDING ALL);
CREATE TABLE IF NOT EXISTS archive.archive_show_genre (LIKE public.show_genre INCLUDING ALL);

-- Create archive procedures

CREATE OR REPLACE PROCEDURE archive_show_data()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO archive.archive_show
    SELECT * FROM public.show
    ON CONFLICT (show_id) DO NOTHING;
END;
$$;

CREATE OR REPLACE PROCEDURE archive_show_date_added_data()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO archive.archive_show_date_added
    SELECT * FROM public.show_date_added
    ON CONFLICT (show_id) DO NOTHING;
END;
$$;

CREATE OR REPLACE PROCEDURE archive_show_director_data()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO archive.archive_show_director
    SELECT * FROM public.show_director
    ON CONFLICT (show_id, director) DO NOTHING;
END;
$$;

CREATE OR REPLACE PROCEDURE archive_show_cast_data()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO archive.archive_show_cast
    SELECT * FROM public.show_cast
    ON CONFLICT (show_id, actor) DO NOTHING;
END;
$$;

CREATE OR REPLACE PROCEDURE archive_show_country_data()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO archive.archive_show_country
    SELECT * FROM public.show_country
    ON CONFLICT (show_id, country) DO NOTHING;
END;
$$;

CREATE OR REPLACE PROCEDURE archive_show_genre_data()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO archive.archive_show_genre
    SELECT * FROM public.show_genre
    ON CONFLICT (show_id, genre) DO NOTHING;
END;
$$;

CREATE OR REPLACE PROCEDURE archive_all_data()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL archive_show_data();
    CALL archive_show_date_added_data();
    CALL archive_show_director_data();
    CALL archive_show_cast_data();
    CALL archive_show_country_data();
    CALL archive_show_genre_data();
END;
$$;