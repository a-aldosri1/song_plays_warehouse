import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays_fact"
user_table_drop = "DROP TABLE IF EXISTS users_dim"
song_table_drop = "DROP TABLE IF EXISTS songs_dim"
artist_table_drop = "DROP TABLE IF EXISTS artists_dim"
time_table_drop = "DROP TABLE IF EXISTS time_dim"

# CREATE TABLES

staging_events_table_create= ("""

CREATE TABLE staging_events (
    "artist" nvarchar(200) NULL,
     "auth" varchar(30) NOT NULL,
     "firstName" varchar(40)  NULL,
     "gender" char(1)  NULL,
     "itemInSession" INTEGER NOT NULL,
     "lastName" varchar(40)  NULL,
    "length" double precision  NULL,
    "level" varchar(10) not NULL,
    "location" varchar(250) NULL,
    "method" varchar(10) NULL,
    "page" varchar(25) NULL,
    "registration" BIGINT   NULL,
    "sessionId" bigint not NULL,
    "song" nvarchar(250)  NULL ,
    "status" INTEGER  NULL,
    "ts"  bigint not NULL,
    "userAgent" varchar(300) NULL,
    "userId" bigint  null
)
""")

    # "id" bigint identity(0, 1) NOT NULL,
staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    "artist_id" varchar(25) NOT NULL,
    "artist_latitude" double precision  NULL,
    "artist_longitude" double precision  NULL,
    "artist_location" varchar(200) NULL,
    "artist_name" nvarchar(200) NULL,
    "song_id" varchar(25) NOT NULL,
    "title" nvarchar(250) NOT NULL,
    "duration" double precision NOT NULL,
    "year" int  NOT NULL
   
)


""")






songplay_table_create = ("""
CREATE TABLE song_plays_fact (
    "songplay_id" bigint identity(0, 1) NOT NULL,
    "start_time" char(18) not NULL sortkey,
    "user_id" bigint  NULL,
    "level" varchar(10) NULL,
    "song_id" varchar(25) NULL distkey,
    "artist_id" varchar(25) NULL,
    "session_id" bigint NOT NULL,
    "location" varchar(250) NULL,
    "user_agent" varchar(250) NULL
   
)


""")

user_table_create = ("""

CREATE TABLE users_dim (
    "user_id" bigint not NULL sortkey,
    "first_name" varchar(40) NOT NULL,
    "last_name" varchar(40) NOT NULL,
    "gender" char(1) NOT NULL
   
)



""")

song_table_create = ("""

CREATE TABLE songs_dim (
    "song_id" varchar(25) NOT NULL sortkey distkey,
    "title" nvarchar(250) NOT NULL,
    "artist_id" varchar(25) NOT NULL,
    "year" int  NOT NULL,
    "duration" double precision NOT NULL

   
)



""")

artist_table_create = ("""
CREATE TABLE artists_dim (
    "artist_id" varchar(25) NOT NULL sortkey,
    "name" nvarchar(250) not NULL,
    "location" varchar(200) NULL,
     "latitude" double precision  NULL,
    "longitude" double precision  NULL



)

""")

time_table_create = ("""

CREATE TABLE time_dim (
    "start_time" char(18) not NULL sortkey,
    "hour" SMALLINT not NULL,
    "day" SMALLINT not NULL,
    "week" SMALLINT not NULL,
    "month" SMALLINT not NULL,
    "year" SMALLINT not NULL,
    "weekday" SMALLINT not NULL

)


""")



# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}' 
    credentials 'aws_iam_role={}' 
    Region 'us-west-2'
    JSON '{}';
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from '{}' 
    credentials 'aws_iam_role={}' 
    Region 'us-west-2'
    json 'auto';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES




songplay_table_insert = ("""

insert into song_plays_fact (
start_time, 
user_id, 
level, 
song_id, 
artist_id, 
session_id, 
location, 
user_agent )    
select distinct to_char(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second', 'YYYY-MM-DD HH24:MI')  start_time, 
e.userId as user_id,
e.level,
s.song_id,
s.artist_id,
e.sessionid as session_id,
e.location,
e.userAgent as user_agent
from staging_events as e
left JOIN staging_songs s on lower(e.song) = lower(s.title) and lower(e.artist) = lower(s.artist_name)
where e.page = 'NextSong'



""")

user_table_insert = ("""
insert into users_dim (
user_id,
first_name,
last_name,
gender
)

select distinct 
userId as user_id,
firstname as first_name,
lastname as last_name,
gender
from staging_events as e
where userId is not null 


""")

song_table_insert = ("""
insert into songs_dim (
song_id,
title,
artist_id,
year,
duration
)
select distinct 
song_id,
title,
artist_id,
year,
duration
from staging_songs
where song_id is not null

""")

artist_table_insert = ("""

insert into artists_dim (artist_id,name,location,latitude)

with artists_cte as (
select distinct artist_id,
artist_name as name,
artist_location as location,
artist_latitude as latitude,
artist_longitude as longitude
from staging_songs
),

artists_ordered as (
select artist_id,
name,
location,
latitude,
row_number() over(partition by artist_id order by location, name desc) as row_order
from artists_cte


)

select artist_id,
name,
location,
latitude
from artists_ordered
where row_order = 1


""")

time_table_insert = ("""
insert into time_dim (start_time, hour, day, week, month, year, weekday)

with start_time as (
select 
TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
from staging_events
)


select distinct to_char( start_time,'YYYY-MM-DD HH24:MI') as start_time,
date_part(hour,start_time) as hour,
date_part(day,start_time) as day,
date_part(week,start_time) as week,
date_part(month,start_time) as month,
date_part(year,start_time) as year,
date_part(dow,start_time) as weekday


from start_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
