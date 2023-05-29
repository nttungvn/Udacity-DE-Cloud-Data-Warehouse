import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events" (
    "artist" varchar(1000),
    "auth" varchar(1000),
    "firstname" varchar(1000),
    "gender" varchar(1000),
    "itemInSession" int,
    "lastName"  varchar(1000),
    "length" float,
    "level" varchar(1000),
    "location" varchar(1000),
    "method" varchar(1000),
    "page" varchar(1000),
    "registration" float,
    "sessionId" int,
    "song" varchar(1000),
    "status" int,
    "ts" bigint,
    "userAgent" varchar(1000),
    "userId" int
);
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    "num_songs" int,
    "artist_id" varchar(1000),
    "artist_latitube" float,
    "artist_longitube" float,
    "artist_location" varchar(1000),
    "artist_name" varchar(1000),
    "song_id" varchar(1000),
    "title" varchar(1000),
    "duration" float,
    "year" int
);
""")

songplay_table_create = ("""
CREATE TABLE "songplay" (
    "songplay_id" int primary key,
    "start_time" int,
    "user_id" int,
    "level" varchar(1000),
    "song_id" varchar(1000),
    "artist_id" varchar(1000),
    "session_id" int,
    "location" varchar(1000),
    "user_agent" varchar(1000)
);
""")

user_table_create = ("""
CREATE TABLE "users" (
    "user_id" int primary key,
    "first_name" varchar(1000),
    "last_name" varchar(1000),
    "gender" varchar(1000),
    "level" varchar(1000)
);
""")

song_table_create = ("""
CREATE TABLE "songs" (
    "song_id" int primary key,
    "title" varchar(1000),
    "year" int,
    "artist_id" varchar(1000),
    "duration" float
);
""")

artist_table_create = ("""
CREATE TABLE "artists" (
    "artist_id" varchar(1000) primary key,
    "name" varchar(1000),
    "location" varchar(1000),
    "latitube" float,
    "longitube" float
);
""")

time_table_create = ("""
CREATE TABLE "times" (
    "start_time" int primary key,
    "hour" int,
    "day" int,
    "week" int,
    "month" int,
    "year" int,
    "weekday" int
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    region 'us-west-2'
    format as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    region 'us-west-2'
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        DATEADD(s, e.ts / 1000, '19700101') AS start_time,
        e.userId as userId,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.sessionId as session_id,
        e.location as location,
        e.useragent user_agent
    FROM staging_events e
    JOIN staging_songs s
    ON (e.artist = s.artist_name AND e.title = s.title AND e.length = s.duration)
    WHERE e.page = 'NextSong'
    ;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT 
        DISTINCT e.userid AS user_id,
        e.firstname AS firstname,
        e.lastname ASlastname,
        e.gender AS gender,
        e.level ASlevel
    FROM staging_events e 
    WHERE e.page = 'NextSong'
    ;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT s.song_id AS song_id,
        s.title AS title,
        s.artist_id AS artist_id,
        s.year AS year,
        s.duraction AS duration
    FROM staging_songs s
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT 
        DISTINCT s.artist_id AS artist_id,
        s.artist_name AS artist_name,
        s.artist_location AS artist_location,
        s.artist_latitude AS artist_latitude,
        s.artist_longitube AS artist_longitube
    FROM staging_songs s
    ;
""")

time_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(DATEADD(s, ts / 1000, '19700101')) AS start_time, 
        EXTRACT(HOUR FROM start_time) as hour,
        EXTRACT(DAY FROM start_time) as day,
        EXTRACT(WEEK FROM start_time) as week,
        EXTRACT(MONTH FROM start_time) as month,
        EXTRACT(YEAR FROM start_time) as year,
        EXTRACT(WEEKDAY FROM start_time) as weekday
    FROM staging_events
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
