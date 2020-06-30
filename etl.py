import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
     - READS the song file from the filepath and creates a dataframe
     - Creates a list of song_data from the dataframe 
     - Use song_data to INSERT in song_table_insert
     - Creates a list of artist_data from the dataframe 
     - Use artist_data to INSERT in artist_table_insert  
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    song_data = song_data[0,:].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = artist_data[0,:].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
     - READS the log file from the filepath and creates a dataframe
     - Filter by 'NextSong' action
     - Convert timestamp to datetime
     - Create a list time_data from the datetime dataframe
     - Create dataframe time_df from time_data
     - Use time_data dataframe to INSERT in time_table_insert
     - Creates a list of user_data from the dataframe 
     - Use user_data to INSERT in user_table_insert
     - get song_id and artist_id from song and artist tables
     - Creates a list of songplay_data from the dataframe 
     - Use songplay_data to INSERT in songplay_table_insert     
    """

    # open log file
    df = pd.read_json(filepath,  lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.day_name]
    column_labels =  ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame({'start_time': df['ts'], 'hour': t.dt.hour, 'day': t.dt.day, \
                        'week': t.dt.week, 'month': t.dt.month, 'year': t.dt.year, 'weekday': t.dt.day_name()})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, int(row.userId), row.level, \
                     songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
     
     - Get all json files from the filepath and append in all_files
     - iterate fuctions process_song_file and process_log_file through all the files over all_files and print them
     
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
#        if i%10==0:
#            print(datafile)


def main():
    """
     
     - connect to sparkify database
     - calling process_data functions to process all the files in song_data and log_data and generate the lists for INSERT tables
     
     """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    '''
    cur.execute("ALTER TABLE users ADD CONSTRAINT user1 FOREIGN KEY (user_id) REFERENCES songplays (user_id);")
    cur.execute("ALTER TABLE songs ADD CONSTRAINT songs1 FOREIGN KEY (song_id) REFERENCES songplays (song_id);")
    cur.execute("ALTER TABLE artists ADD CONSTRAINT artists1 FOREIGN KEY (artist_id) REFERENCES songplays (artist_id);")
    cur.execute("ALTER TABLE time ADD CONSTRAINT time1 FOREIGN KEY (start_time) REFERENCES songplays (start_time);")
    '''
    
    conn.close()


if __name__ == "__main__":
    main()
