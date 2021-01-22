import psycopg2
import os
import glob
import pandas as pd
from sql_queries import * 


def process_song_data(cur, filepath):
	df = pd.read_json(filepath, lines=True)
	song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0]
	cur.execute(song_table_insert, song_data.tolist())
	artist_data = df[["artist_id", "artist_name", "artist_location", \
										"artist_latitude", "artist_longitude"]].values[0]
	cur.execute(artist_table_insert, artist_data.tolist())

def process_log_data(cur, filepath):
	df = pd.read_json(filepath, lines=True)
	df = df[df['page'] == 'NextSong']
	t=pd.to_datetime(df['ts'],unit='ms')
	time_data = [t,\
          t.dt.hour,\
          t.dt.day,\
          t.dt.week,\
          t.dt.month,\
          t.dt.year,\
          t.dt.dayofweek]
	column_labels = ['ts', 'hour', 'day', 'week', 'month', 'year', 'dayofweek']
	time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
	for i, row in time_df.iterrows():
		cur.execute(time_table_insert, row.tolist())

	user_data = df[["userId", "firstName", "lastName", "gender", "level"]]
	for i, row in user_data.iterrows():
		cur.execute(user_table_insert, row.tolist())

	for i, row in df.iterrows():
		results=cur.execute(song_select, (row.song, row.artist, row.length))
		if results :
			songId, artistId = results
		else:
			songId, artistId = None, None

		songplay_data = (pd.to_datetime(row.ts, unit='ms'),\
						row.userId,\
						songId,\
						artistId,\
						row.level,\
						row.sessionId,\
						row.location,\
						row.userAgent)
		cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    all_files=[]
    for root, dirs, files in os.walk(filepath):
        files=glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    num_files=len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
    	func(cur, datafile)
    	conn.commit()
    	print("{}/{} files processed.".format(i, num_files))


def main():
	conn=psycopg2.connect("host=127.0.0.1 user=postgres password=admin dbname=sparkifydb")
	cur=conn.cursor()

	process_data(cur, conn, filepath="data/song_data", func=process_song_data )
	process_data(cur, conn, filepath="data/log_data", func=process_log_data )


	conn.close()

if __name__=="__main__":
	main()





