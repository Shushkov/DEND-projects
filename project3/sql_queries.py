import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")
# DROP TABLES

staging_songs_table_drop = "drop table if exists staging_songs;"
staging_events_table_drop = "drop table if exists staging_events;"
#used for tests
#staging_songs_table_drop = "select 1;"
#staging_events_table_drop = "select 1;"

songplay_table_drop = "drop table if EXISTS songplays;"
user_table_drop = "drop table if EXISTS userss;"
song_table_drop = "drop table if EXISTS songs;"
artist_table_drop = "drop table if EXISTS artists;"
time_table_drop = "drop table if EXISTS time;"

# CREATE TABLES

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
#used for tests
#staging_songs_table_create = ('select 1;')
#staging_events_table_create= ('select 1;')

songplay_table_create = ("""
create table if not exists songplays(
    id bigint IDENTITY(0, 1)  primary key,
    start_time bigint sortkey, 
    user_id bigint DISTKEY, 
    level varchar(4), 
    song_id varchar, 
    artist_id varchar, 
    session_id bigint, 
    location varchar, 
    user_agent varchar
) DISTSTYLE KEY;""")

user_table_create = ("""
create table if not exists users(
    id bigint primary key sortkey DISTKEY,
    first_name varchar, 
    last_name varchar, 
    gender varchar(1), 
    level varchar(4)
) DISTSTYLE KEY;""")

song_table_create = ("""
create table if not exists songs(
    id varchar primary key,
    artist_id varchar,
    title varchar,
    duration numeric, 
    year int    
) DISTSTYLE  ALL sortkey(id, title);""")

artist_table_create = ("""
create table if not exists artists(
    id varchar primary key,
    latitude numeric,
    longitude numeric,
    location varchar,
    name varchar
) DISTSTYLE  ALL sortkey(id,name);""")

time_table_create = ("""
create table if not exists time(
    start_time bigint primary key sortkey, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
) DISTSTYLE  ALL;""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from {}
credentials 'aws_iam_role={}'
FORMAT AS JSON {} region 'us-west-2';""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_songs_copy = ("""
copy staging_songs 
from {}
credentials 'aws_iam_role={}'
FORMAT AS JSON 'auto' region 'us-west-2' TRUNCATECOLUMNS ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF MAXERROR 2;""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select sevnt.ts, sevnt.userId, sevnt.level, sngs.song_id, sngs.artist_id, sevnt.sessionId, sevnt.location, sevnt.userAgent
from staging_events sevnt
join staging_songs sngs on sevnt.song = sngs.title and sevnt.artist=sngs.artist_name
where sevnt.page = 'NextSong'
;""")

user_table_insert = ("""
insert into users(id, first_name, last_name, gender, level)
select userId, firstName, lastName, gender, level
from staging_events where userId is not NULL;""")

song_table_insert = ("""
insert into songs(id, artist_id, title, duration, year)
select song_id, artist_id, title, duration, year
from staging_songs where song_id is not NULL and artist_id is not null;
""")

artist_table_insert = ("""
insert into artists(id, latitude, longitude, location, name)
select artist_id, artist_latitude, artist_longitude, artist_location, artist_name
from staging_songs  where artist_id is not null;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select ts, 
DATE_PART('hour',TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
DATE_PART('day',TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
DATE_PART('week',TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
DATE_PART('month',TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
DATE_PART('year',TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
DATE_PART('dayofweek',TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
from staging_events where page = 'NextSong';""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
