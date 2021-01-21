#drop tables 
songplay_table_drop="DROP TABLE IF EXISTS songplays ;"
user_table_drop = "DROP TABLE IF EXISTS users; "
song_table_drop = "DROP TABLE IF EXISTS songs; "
artist_table_drop = "DROP TABLE IF EXISTS artists; "
time_table_drop = "DROP TABLE IF EXISTS time; "

#create tables
song_table_create="""
CREATE TABLE IF NOT EXISTS songs(
							song_id varchar PRIMARY KEY,
							title varchar ,
							artist_id varchar NOT NULL,
							year int, 
							duration float
							)"""

artist_table_create="""
CREATE TABLE IF NOT EXISTS artists(
							artist_id varchar PRIMARY KEY,
							name varchar, 
							location varchar NOT NULL,
							latitude float,
							longitude float)"""
# insert data
song_table_insert="""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING"""

artist_table_insert="""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING """

drop_table_queries=[songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries=[song_table_create, artist_table_create]
