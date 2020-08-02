class SqlQueries:
    # CREATE TABLES
    staging_events_table_create = '''
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        TEXT,
        auth          TEXT,
        firstName     TEXT,
        gender        TEXT,
        iteminsession INTEGER,
        lastname      TEXT,
        length        FLOAT,
        level         TEXT,
        location      TEXT,
        method        TEXT,
        page          TEXT,
        registration  FLOAT,
        sessionid     INTEGER,
        song          TEXT,
        status        INTEGER,
        ts            TIMESTAMP,
        useragent     TEXT,
        userid        INTEGER
    );
    '''

    staging_songs_table_create = '''
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id          TEXT PRIMARY KEY,
        title            TEXT,
        duration         FLOAT,
        year             FLOAT,
        num_songs        FLOAT,
        artist_id        TEXT,
        artist_name      TEXT,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        artist_location  TEXT
    );
    '''

    songplays_table_create = '''
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id TEXT PRIMARY KEY,
            start_time  TIMESTAMP NOT NULL,
            user_id     INTEGER NOT NULL,
            level       TEXT NOT NULL,
            song_id     TEXT,
            artist_id   TEXT,
            session_id  INTEGER NOT NULL,
            location    TEXT NOT NULL,
            user_agent  TEXT NOT NULL
        );
    '''

    users_table_create = '''
        CREATE TABLE IF NOT EXISTS users (
            user_id    INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name  TEXT NOT NULL,
            gender     TEXT NOT NULL,
            level      TEXT NOT NULL
        );
    '''

    songs_table_create = '''
        CREATE TABLE IF NOT EXISTS songs (
            song_id   TEXT PRIMARY KEY,
            title     TEXT NOT NULL,
            artist_id TEXT NOT NULL,
            year      INTEGER,
            duration  FLOAT
        );
    '''

    artists_table_create = '''
        CREATE TABLE IF NOT EXISTS artists (
            artist_id TEXT PRIMARY KEY,
            name      TEXT NOT NULL,
            location  TEXT,
            lattitude FLOAT,
            longitude FLOAT
        );
    '''

    time_table_create = '''
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY,
            hour       INTEGER,
            day        INTEGER,
            week       INTEGER,
            month      INTEGER,
            year       INTEGER,
            weekday    INTEGER
        ) ;
    '''

    # INSERT TABLES
    staging_events_table_insert = '''
        CREATE TABLE IF NOT EXISTS staging_events AS
        SELECT
            data::jsonb->>'artist' AS artist,
            data::jsonb->>'auth' AS auth,
            data::jsonb->>'firstName' AS firstName,
            data::jsonb->>'gender' AS gender,
            CAST(data::jsonb->>'itemInSession' AS INTEGER) AS itemInSession,
            data::jsonb->>'lastName' AS lastName,
            CAST(data::jsonb->>'length' AS FLOAT) AS length,
            data::jsonb->>'level' AS level,
            data::jsonb->>'location' AS location,
            data::jsonb->>'method' AS method,
            data::jsonb->>'page' AS page,
            CAST(data::jsonb->>'registration' AS FLOAT) AS registration,
            CAST(data::jsonb->>'sessionId' AS INTEGER) AS sessionId,
            data::jsonb->>'song' AS song,
            CAST(data::jsonb->>'status' AS INTEGER) AS status,
            TO_TIMESTAMP(CAST(data::jsonb->>'ts' AS BIGINT) / 1000) AS ts,
            data::jsonb->>'userAgent' AS userAgent,
            CAST(NULLIF(data::jsonb->>'userId', '') AS INTEGER) AS userId
        FROM staging_events_json;
    '''

    staging_songs_table_insert = '''
        CREATE TABLE IF NOT EXISTS staging_songs AS
        SELECT
            NULLIF(data::jsonb->>'song_id', '') AS song_id,
            data::jsonb->>'title' AS title,
            CAST(data::jsonb->>'duration' AS FLOAT) AS duration,
            CAST(NULLIF(data::jsonb->>'year', '0') AS INTEGER) AS year,
            CAST(data::jsonb->>'num_songs' AS INTEGER) AS num_songs,
            data::jsonb->>'artist_id' AS artist_id,
            data::jsonb->>'artist_name' AS artist_name,
            CAST(data::jsonb->>'artist_latitude' AS FLOAT) AS artist_latitude,
            CAST(data::jsonb->>'artist_longitude' AS FLOAT) AS artist_longitude,
            NULLIF(data::jsonb->>'artist_location', '') AS artist_location
        FROM staging_songs_json;
    '''

    # SELECT TABLES
    songplays_table_select = '''
        SELECT
            md5(CAST(events.sessionid | events.start_time AS TEXT)) AS songplay_id,
            events.ts AS start_time,
            events.userid AS user_id,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionid AS session_id,
            events.location,
            events.useragent AS user_agent
        FROM (
            SELECT
                CAST(EXTRACT(EPOCH FROM ts) AS INTEGER) AS start_time,
                *
            FROM staging_events
        ) AS events
        LEFT JOIN staging_songs AS songs
            ON events.song    = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
        WHERE
            page        = 'NextSong'
            AND userid is NOT NULL
        ORDER BY start_time, user_id
    '''

    users_table_select = '''
        SELECT
            distinct userid,
            firstname,
            lastname,
            gender,
            level
        FROM staging_events
        WHERE
            page        = 'NextSong'
            AND userId is NOT NULL
        ORDER BY userId
    '''

    songs_table_select = '''
        SELECT
            distinct song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
        ORDER BY song_id
    '''

    artists_table_select = '''
        SELECT DISTINCT
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
        ORDER BY artist_id
    '''

    time_table_select = '''
        SELECT
            start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time),
            extract(month from start_time),
            extract(year from start_time),
            extract(dow from start_time)
        FROM songplays
        WHERE start_time IS NOT NULL
        ORDER BY start_time
    '''
