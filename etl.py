import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    ''' 
    - Open and process a JSON file from filepath, storing data in a pandas data frame.
    - Insert song data into the song table.
    - Insert artist data into the artist table.
    '''
    # Open the song file
    data = pd.read_json(filepath, typ='series')  # Open as a series and then convert to a DataFrame
    df = pd.DataFrame([data])

    # Insert song record
    song_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = list(song_df.values[0])
    cur.execute(song_table_insert, song_data)

    # Insert artist record
    artist_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = list(artist_df.values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    - Read a JSON log file from filepath into a pandas data frame.
    - Filter data to include only 'Next Song' actions.
    - Extract time information and insert it into the time table.
    - Extract user information and insert it into the user table.
    - Find song_id and artist_id for songs in the log data set.
    - Extract songplay information and insert it into the songplay table.
    '''
    # Open the log file
    df = pd.read_json(filepath, lines=True)

    # Filter by 'NextSong' action
    df = df[df['page'] == 'NextSong']

    # Convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # Extract datetime units
    timestamp = list(t.dt.time.values)
    hour = list(t.dt.hour.values)
    day = list(t.dt.day.values)
    week = list(t.dt.isocalendar().week.values)
    month = list(t.dt.month.values)
    year = list(t.dt.year.values)
    weekday = list(t.dt.day_name().values)

    # Insert time data records
    time_data = [timestamp, hour, day, week, month, year, weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    time_dict = {label: data for label, data in zip(column_labels, time_data)}
    time_df = pd.DataFrame(time_dict)

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # Insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # Insert songplay records
    for _, row in df.iterrows():
        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results[0], results[1]
        else:
            songid, artistid = None, None

        # Insert songplay record
        songplay_data = (t[_], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    - Aggregate all JSON files from a directory into a list.
    - Iterate over the list of files and apply a processing function to those files.
    - Report progress as it runs.
    '''
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # Get the total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    - Connect to the sparkifydb.
    - Apply song processing to the song dataset and insert data into the songs and artists tables.
    - Apply log processing to the log dataset and insert data into time, users, and songplays tables.
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb")
    cur = conn.cursor()

    process_data(cur, conn, filepath='../data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='../data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
