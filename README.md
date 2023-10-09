# Music-Analysis-Postgres

## Main Aim

The primary objectives of this project are as follows:

1. Perform data modeling using PostgreSQL.
2. Create an ETL (Extract, Transform, Load) pipeline using Python within a Jupyter Notebook.
3. Define fact and dimension tables to establish a star schema data model that enhances query performance for song play analysis.
4. Implement an ETL pipeline to transfer data from JSON files into PostgreSQL tables using Python and SQL.


## Overview

Sparkify, a burgeoning startup, is keen to gain insights from the data they've gathered regarding songs and user activities on their new music streaming platform. The analytics team is particularly interested in understanding user song preferences. Presently, they lack a convenient means to query their data, which is stored in JSON logs for user activity and JSON metadata for the songs available on the platform.

## Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```bash
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```json
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": 35.1495,
    "artist_longitude": -90.0490,
    "artist_location": "Memphis, Tennessee, USA",
    "artist_name": "Johnny Cash",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Ring of Fire",
    "duration": 166.32036,
    "year": 1963
}
```

## Log Dataset
The second dataset consists of log files in JSON format that have been generated using an event simulator. These log files mimic user activity on a music streaming application and are based on specific configurations. The files are organized chronologically by year and month.


And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

```json
{
    "artist": "Beyoncé",
    "auth": "Logged In",
    "firstName": "Alice",
    "gender": "F",
    "itemInSession": 7,
    "lastName": "Johnson",
    "length": 212.74567,
    "level": "paid",
    "location": "New York, NY",
    "method": "PUT",
    "page": "NextSong",
    "registration": 1548079511234.0,
    "sessionId": 101,
    "song": "Crazy in Love",
    "status": 200,
    "ts": 1548367823796,
    "userAgent": "\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36\"",
    "userId": "123"
}

```

## Description of Files

* `data folder` - contains the datasets

* `queries.py` - Houses SQL queries responsible for creating tables, inserting data into tables, dropping tables, and executing various other database queries.

* `table-creation.py` - Python script that creates a postgres database containing empty tables

* `etl.py` - Python script that extracts data from JSON files, transforms it to the appropriate data type or format, and loads it into a SQL table

* `test.ipynb` - Jupyter Notebook containing sample queries


## How to Run

1. Run `table-creation.py` to create database and tables
2. Run `etl.py` to load data into appropriate tables
3. Run cells in `test.ipynb` to test that data was loaded correctly
