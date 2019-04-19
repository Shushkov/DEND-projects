# Project 3: Data Modeling with Redshift
This project `POC` for ETL pipeline for Sparkify analysis DB in AWS ecosystem.
### Purpose
Main goal - it's crate `POC` for ETL process based on parse JSON logs for Sparkify users page walk events.
Second, it's training with Amazon Redshift dimension data modeling.

---
That provide few tables:
1. Fact Table

    - songplays - records from log data files associated with songs, artists,time and user tables

2. Dimension Tables

    - users - users in the app
        * user_id, first_name, last_name, gender, level
    - songs - songs from music database
        * song_id, title, artist_id, year, duration
    - artists - artists from music database
        * artist_id, name, location, lattitude, longitude
    - time - timestamps of records in songplays broken down into specific units
        * start_time, hour, day, week, month, year, weekday  
        
3. Staging tables
used for intemidiant store data from json files as-is

    - staging_songs
      * num_song ,artist_id ,artist_latitude ,artist_longitude ,artist_location ,artist_name ,song_id ,title ,duration ,year
    - staging_events
      * artist ,auth ,firstName ,gender ,itemInSession ,lastName ,length ,level ,location ,method
      * page ,registration ,sessionId ,song ,status ,ts ,userAgent ,userId

## Getting Started

First of all do thw following step:

1. run `python3 create_tables.py` in terminal to create sparkifydb DB in your locall PostgreSQL instant
2. run `python3 etl.py` to start etl pipeline proccess

After that you will be able to run quaries to database.
### Use cases
That demo to show some analyze case for db

1. Show timelaps for songs play on Sparkify 

``SELECT (TIMESTAMP 'epoch' + sng.start_time/1000 * INTERVAL '1 second') as start_time, ss.title, art.name FROM songplays sng join songs ss on sng.song_id = ss.id join artists art on art.id = sng.artist_id;``
2. Analyze what browser prever our users

``select count(user_agent) as amount, user_agent from songplays group by user_agent order by amount desc;``
3. Analyze user playlist and what s/he like )

``select tt.*, users.first_name, title from songplays join users on user_id=users.id join time tt on tt.start_time = songplays.start_time join songs ss on songplays.song_id = ss.id order by first_name, start_time;``

4. and more what do you want