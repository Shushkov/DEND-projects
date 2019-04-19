# DROP TABLES

songplay_table_drop = "drop table if EXISTS songplays;"
user_table_drop = "drop table if EXISTS users;"
song_table_drop = "drop table if EXISTS songs;"
artist_table_drop = "drop table if EXISTS artists;"
time_table_drop = "drop table if EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""create table if not exists songplays(
    id bigserial primary key,
    start_time bigint, 
    user_id bigint, 
    level varchar(4), 
    song_id varchar, 
    artist_id varchar, 
    session_id bigint, 
    location varchar, 
    user_agent varchar
);""")

user_table_create = ("""create table if not exists users(
    id bigint primary key,
    first_name varchar, 
    last_name varchar, 
    gender varchar(1), 
    level varchar(4)
);""")

song_table_create = ("""create table if not exists songs(
    id varchar primary key,
    artist_id varchar,
    title varchar,
    duration numeric, 
    year int
);""")

artist_table_create = ("""create table if not exists artists(
    id varchar primary key,
    latitude numeric,
    longitude numeric,
    location varchar,
    name varchar
);""")

time_table_create = ("""create table if not exists time(
    start_time bigint primary key, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
);""")

# INSERT RECORDS

songplay_table_insert = ("""insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values(%s,%s,%s,%s,%s,%s,%s,%s);""")

user_table_insert = ("""insert into users(id, first_name, last_name, gender, level)
values(%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING;""")

song_table_insert = ("""insert into songs (id, title, artist_id, year, duration)
values(%s,%s,%s,%s,ROUND((%s)::numeric,2)) ON CONFLICT (id) DO NOTHING;""")

artist_table_insert = ("""insert into artists (id, name, location, latitude, longitude)
values(%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING;""")


time_table_insert = ("""insert into time (start_time ,hour,day,week,month,year,weekday)
values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (start_time) DO NOTHING;""")

# FIND SONGS

song_select = ("""select sng.id as song_id, art.id as artist_id from artists art
join songs sng  on sng.artist_id=art.id where sng.title=(%s) and art.name = (%s) and sng.duration=ROUND((%s)::numeric,2) 
;""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]