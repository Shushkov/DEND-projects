class SqlQueries:
    staging_events_table_create= ("""
        create table if not exists staging_events(
            artist text,
            auth varchar(10),
            firstName text,
            gender varchar(1),
            itemInSession int,
            lastName text,
            length real,
            level varchar(4),
            location text,
            method varchar(4),
            page text,
            registration FLOAT8,
            sessionId int,
            song text,
            status int,
            ts bigint,
            userAgent text,
            userId bigint DISTKEY
        ) DISTSTYLE KEY SORTKEY(song, artist);""")
    
    staging_songs_table_create = ("""
        create table if not exists staging_songs(
            num_song int,
            artist_id varchar(19),
            artist_latitude real,
            artist_longitude real,
            artist_location text,
            artist_name text,
            song_id varchar(19),
            title text,
            duration real,
            year int
        ) DISTSTYLE AUTO SORTKEY (title, artist_name);""")
    
    songplay_table_insert = ("""
        CREATE TABLE {}
        DISTSTYLE KEY distkey(user_id) sortkey(start_time)
        as
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        CREATE TABLE {}
        DISTSTYLE KEY distkey(id) sortkey(id)
        as
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        CREATE TABLE {}
        DISTSTYLE ALL sortkey(id,title)
        as
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        CREATE TABLE {}
        DISTSTYLE ALL sortkey(id,name)
        as
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        Create table {} 
        DISTSTYLE  ALL sortkey (start_time)
        as
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)