class SqlQueries:
    
    copy_query = ("""
        COPY {} 
        FROM '{}'
        REGION '{}'
        JSON '{}'
        DATEFORMAT '{}'
        TIMEFORMAT '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """)
    
    
    
    
    
    songplay_table_insert = ("""
        INSERT INTO {}
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
            FROM {}
            WHERE page='{}' AND sessionid IS NOT NULL AND start_time IS NOT NULL ) events
            LEFT JOIN {} songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO {}
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong' AND userid IS NOT NULL 
    """)

    song_table_insert = ("""
        INSERT INTO {}
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL 
    """)

    artist_table_insert = ("""
        INSERT INTO {}
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL 
    """)

    time_table_insert = ("""
        INSERT INTO {}
        SELECT s_events.ts as timestamp,
            TIMESTAMP WITHOUT TIME ZONE 'epoch' + (s_events.ts::bigint::float / 1000) * INTERVAL '1 second' as start_time,
            EXTRACT(hour  FROM TIMESTAMP WITHOUT TIME ZONE 'epoch' + (s_events.ts::bigint::float / 1000)\
            * INTERVAL '1 second') as hour,
            EXTRACT(day   FROM TIMESTAMP WITHOUT TIME ZONE 'epoch' + (s_events.ts::bigint::float / 1000)\
            * INTERVAL '1 second') as day,
            EXTRACT(week  FROM TIMESTAMP WITHOUT TIME ZONE 'epoch' + (s_events.ts::bigint::float / 1000)\
            * INTERVAL '1 second') as week,
            EXTRACT(month FROM TIMESTAMP WITHOUT TIME ZONE 'epoch' + (s_events.ts::bigint::float / 1000)\
            * INTERVAL '1 second') as month,
            EXTRACT(year  FROM TIMESTAMP WITHOUT TIME ZONE 'epoch' + (s_events.ts::bigint::float / 1000)\
            * INTERVAL '1 second') as year,
            EXTRACT(dow   FROM TIMESTAMP WITHOUT TIME ZONE 'epoch' + (s_events.ts::bigint::float / 1000)\
            * INTERVAL '1 second') as weekday
        FROM staging_events s_events
        WHERE s_events.ts IS NOT NULL 
    """)
    
    