# Project Summary
Sparkify, a startup, had a growing database of user information and songs, and needed to move the database onto the cloud.

The song and log dataset is in S3, their datasets is loaded into Redshift.By collecting and transforming those data sets, we can build a database where there are dimension tables that can give us insight into the songs that users are listening to.The goal of the analysis is to ensure that the queries provided by the analysis team and the results of the ETL pipeline produce the expected results.

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
1. users - users in the app
    * user_id, first_name, last_name, gender, level
2. songs - songs in music database
    * song_id, title, artist_id, year, duration
3. artists - artists in music database
    * artist_id, name, location, latitude, longitude
4. time - timestamps of records in songplays broken down into specific units
    * start_time, hour, day, week, month, year, weekday

## How To run the script

1. you run the terminal, and run the following command.

```python
$ python create_tables.py
```

2. Run When you run `etl.py`, etl processing is performed.

```python
$ python etl.py
```

## Analysis
The following is an example of SQL for analysis.
Execute the following SQL to find the top 20 songs played by women in November 2018.

```sql
select TOP 20 
  a.name as artist_name,
  s.title as song_title,
  count(*) as views
from songplays sp
JOIN users u ON sp.user_id = u.user_id
JOIN time t ON sp.start_time = t.start_time
JOIN songs s ON sp.song_id = s.song_id
JOIN artists a ON a.artist_id = sp.artist_id
WHERE
  u.gender = 'F' AND
  t.year = 2018 AND t.month = 11
GROUP BY a.name, s.title
ORDER BY views desc
```