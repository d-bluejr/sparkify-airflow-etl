Summary
The purpose of this project is to create and load a star schema for storing Sparkify's app data. The files persisted are song JSON files and log files from the app. Using proper ETL techniques, the schema will store the songs, artists, users, and time as dimension tables and the song play data as the fact table. This goal is accomplished by loading data into Redshift via Apache Airflow

Schema
Name: sparkify
Tables:
    staging_events (staging_event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId) - the log file staging table 
    staging_songs (staging_song_id, num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year) - the song file staging table
    artists (artist_id, name, location, latittude, longitude) - the artist data of the songs in the app 
    songs (song_id, title, artist_id, year, duration) - the song data of the songs in the app
    songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) - the play data for a song played by a user on the app
    time (start_time, hour, day, week, month, year, weekday) - the song timestamp data broken into different units of measurement
    users (user_id, first_name, last_name, gender, level) - the user data for the app

ETL Process Explanation
The files are loaded via the following process:
1. The song and log files are loaded into the staging_songs and staging_events tables respectively. 
2. The staging_songs and staging_events tables are used to populate the artist, songs, songplays, time, and user tables. 
